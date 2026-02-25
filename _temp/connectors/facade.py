import asyncio
import json
import uuid
from enum import Enum
from abc import ABC, abstractmethod
from typing import ClassVar, List, Dict, Union, Set, Optional

from asgiref.sync import sync_to_async

from core.defs import CurrencyPair
from core.utils import utc_now_float
from core.repository import Repository
from core.connectors import (
    BaseExchangeConnector, BookTicker, BookDepth, Ticker, CandleStick, WithdrawInfo
)
from connectors.defs import PERPETUAL_TOKENS

from .defs import (
    build_tickers_key, build_book_ticker_key, build_book_depth_key,
    build_candles_key
)
from .meta import EXCHANGE_CONNECTOR_CLASSES


class ExchangeFacade:

    CACHE_DEPTH_TTL_SEC = 5
    CACHE_CANDLES_TTL_SEC = 60

    class EventType(Enum):
        ON_TICKER = 'ON_TICKER'

    class Observer(ABC):

        @abstractmethod
        async def on_ticker(self, event: BookTicker):
            pass

    def __init__(self, code: str, repo: Repository):
        self.__code = code
        self.__repo = repo
        self.__subscriptions: Dict[ExchangeFacade.Observer, asyncio.Task] = {}

    @property
    def code(self) -> str:
        return self.__code

    async def get_all_symbols(self, section: str = 'spot') -> List[str]:
        tickers: List[Ticker] = await self.__load_tickers()
        if section == 'spot':
            tickers = list(
                filter(lambda tk: tk.is_spot_enabled, tickers)
            )
        elif section == 'margin':
            tickers = list(
                filter(lambda tk: tk.is_margin_enabled, tickers)
            )
        else:
            raise RuntimeError(f'Unexpected section: {section}')
        return [tk.symbol for tk in tickers]

    async def read_pairs(self, quote_filter: str = None, base_filter: str = None, section: str = 'spot') -> List[CurrencyPair]:
        """Получить все торговые пары по бирже

        :param quote_filter: фильтр по коду инструмента
        :param base_filter: фильтр по коду базового
        :param section: Секция рынка
        :return: список пар
        """

        # У коннекторов есть метод явного запроса CurrencyPair но этот тип
        # данных сложно обновлять потому что он отсутствует в subscriber-ах
        # и быстро "устаревает" поэтому пересчитаем из Book-Ticker в CurrencyPair
        notnull_book_tickers: List[BookTicker] = await self.read_book_tickers(section=section)
        pairs = []
        for tk in notnull_book_tickers:
            base, quota = tk.symbol.split('/')
            if quote_filter and quote_filter != quota:
                continue
            if base_filter and base_filter != base:
                continue
            p = CurrencyPair(
                base=base,
                quota=quota,
                ratio=(tk.ask_price + tk.bid_price) / 2,  # вообще должна быть ценой последней сделки
                utc=None  # не реализован пересчет из внутреннего таймера биржи в URC
            )
            pairs.append(p)
        return pairs

    async def read_book_tickers(self, symbol: Union[str, List[str]] = 'all', section: str = 'spot') -> List[BookTicker]:
        """Получить все заявки (тикер + заявки в стакане)

        :param symbol: фильтр по символам тикера
        :param section: Секция рынка
        :return: Список текущих заявок
        """

        tickers = await self.__load_tickers(symbol)
        repo_keys = [build_book_ticker_key(self.code, tk.symbol, section) for tk in tickers]
        js_book_tickers = await self.__repo.get(key=repo_keys)
        # отсортируем непустые
        book_tickers: List[BookTicker] = [
            BookTicker.from_json(json.dumps(val)) for val in
            js_book_tickers.values() if val is not None
        ]
        return book_tickers

    async def read_book_depths(
        self, symbol: Union[str, List[str]],
        section: str = 'spot', limit: int = 100
    ) -> List[BookDepth]:
        tickers = await self.__load_tickers(symbol)
        repo_keys = [build_book_depth_key(self.code, tk.symbol, section) for tk in tickers]
        js_book_depths = await self.__repo.get(key=repo_keys)
        # отсортируем непустые
        cached_book_depths: List[BookDepth] = [
            BookDepth.from_json(json.dumps(val)) for val in
            js_book_depths.values() if val is not None
        ]

        enum_symbols = symbol if isinstance(symbol, list) else [symbol]
        depth_map = {sym: None for sym in enum_symbols}
        now = utc_now_float()
        for cached_depth in cached_book_depths:
            if cached_depth.utc and (now - cached_depth.utc) < self.CACHE_DEPTH_TTL_SEC:
                if min(len(cached_depth.bids), len(cached_depth.asks)) >= limit:
                    depth_map[cached_depth.symbol] = cached_depth

        conn = self.__allocate_connector()
        for sym in [s for s, d in depth_map.items() if d is None]:
            depth = await sync_to_async(conn.get_depth)(sym, limit=limit)
            if depth:
                repo_key = build_book_depth_key(self.code, sym, section)
                await self.__repo.set(
                    repo_key, value=depth.as_json()
                )
                depth_map[sym] = depth
        return [d for d in depth_map.values() if d is not None]

    async def read_candles(self, symbol: str, section: str = 'spot') -> Optional[List[CandleStick]]:
        repo_key = build_candles_key(self.code, symbol, section)
        js = await self.__repo.get(key=repo_key)
        if not js:
            conn = self.__allocate_connector()
            candles = await sync_to_async(conn.get_klines)(symbol)
            if candles:
                if any(c.usd_volume is None for c in candles):
                    base, _ = symbol.split('/')
                    pairs = await self.read_pairs(base_filter=base)
                    ratio = None
                    for p in pairs:
                        if p.quota in PERPETUAL_TOKENS:
                            ratio = p.ratio
                            break
                    if ratio is not None:
                        for c in candles:
                            c.usd_volume = c.coin_volume * ratio
                await self.__repo.set(
                    repo_key,
                    value={'candles': [i.as_json() for i in candles]},
                    ttl=self.CACHE_CANDLES_TTL_SEC
                )
                return candles
            else:
                return None
        else:
            candles_js = js.get('candles')
            if not candles_js:
                return None
            return [CandleStick.from_json_ext(i) for i in candles_js]

    async def read_withdraw_info(self) -> Optional[Dict[str, List[WithdrawInfo]]]:
        conn = self.__allocate_connector()
        return await sync_to_async(conn.get_withdraw_info)()

    async def attach(
        self, observer: Observer, symbol: Union[str, List[str]] = 'all',
        events: Set[EventType] = None, section: str = 'spot'
    ) -> str:
        if symbol == 'all':
            filtering_symbols = 'all'
        else:
            tickers = await self.__load_tickers(symbol)
            filtering_symbols = [tk.symbol for tk in tickers]
        task = asyncio.create_task(
            coro=self.__notify_task(
                observer,
                repo=self.__repo,
                symbols=filtering_symbols,
                events=events or {self.EventType.ON_TICKER},
                section=section
            )
        )
        task.set_name(uuid.uuid4().hex)
        self.__subscriptions[observer] = task
        return task.get_name()

    def detach(self, observer: Union[Observer, str]):
        if isinstance(observer, str):
            _ = [ob for ob, tsk in self.__subscriptions.items() if tsk.get_name() == observer]
            observer = _[0] if _ else None
            if not observer:
                raise RuntimeError(f'Unknown subscription with observer id: {observer}')
        if observer in self.__subscriptions:
            task = self.__subscriptions[observer]
            task.cancel()
            del self.__subscriptions[observer]
        else:
            raise RuntimeError('Observer is not registered in subscriptions')

    async def __load_tickers(self, symbol: Union[str, List[str]] = 'all') -> List[Ticker]:
        values = await self.__repo.get(build_tickers_key(self.code))
        if not values:
            return []
        tickers_raw = values.get('tickers')
        tickers: List[Ticker] = [Ticker.from_json(json.dumps(tk)) for tk in tickers_raw]
        if symbol != 'all':
            if isinstance(symbol, str):
                symbol = [symbol]
            tickers = list(filter(lambda i: i.symbol in symbol, tickers))
        return tickers

    async def __notify_task(
            self, observer: Observer, repo: Repository,
            symbols: Union[str, List[str]], events: Set[EventType],
            section: str = 'spot'
    ):

        sub_tokens = []
        if symbols == 'all':
            pattern = '*'
        else:
            pattern = None

        if self.EventType.ON_TICKER in events:

            async def __on_ticker(msg: Dict):
                nonlocal observer
                event = BookTicker.from_json(json.dumps(msg))
                await observer.on_ticker(event)

            if pattern:
                tickers_pattern = build_book_ticker_key(
                    exchange_code=self.code,
                    symbol=pattern,
                    section=section
                )
                token = await repo.subscribe(tickers_pattern, cb=__on_ticker)
            else:
                repo_keys = [
                    build_book_ticker_key(
                        exchange_code=self.code, symbol=sym,
                        section=section) for sym in symbols
                ]
                token = await repo.subscribe(repo_keys, cb=__on_ticker)
            sub_tokens.append(token)

        try:
            while True:
                # Will be killed only from supervisor
                await asyncio.sleep(10000)
        finally:
            for token in sub_tokens:
                repo.unsubscribe(token)

    def __allocate_connector(self) -> BaseExchangeConnector:
        cls = [
            cls for cls in EXCHANGE_CONNECTOR_CLASSES
            if cls.exchange_id() == self.code
        ][0]
        conn = cls()
        return conn


class ExchangesFacade:
    """Паттерн фасада для коннекторов бирж.

    Оптимизирует использование сокет-соединений, кеширование,
    прячет особенности реализации тонкой логики обращения с коннекторами разных биржи
    """

    def __init__(self):
        self.__repo = Repository()
        self.__exchanges_codes = []
        self.__connectors_cache: Dict[str, ExchangeFacade] = {}
        for cls in EXCHANGE_CONNECTOR_CLASSES:
            self._register_connector(cls)

    def exchange_codes(self) -> List[str]:
        """Доступные коннекторы бирж"""
        return self.__exchanges_codes

    def get_exchange(self, exchange_code: str) -> ExchangeFacade:
        if exchange_code not in self.__connectors_cache:
            if exchange_code not in self.__exchanges_codes:
                raise RuntimeError(f'Unknown exchange_code: {exchange_code}')
            self.__connectors_cache[exchange_code] = ExchangeFacade(
                code=exchange_code,
                repo=self.__repo
            )
        return self.__connectors_cache.get(exchange_code)

    def _register_connector(self, connector_class: ClassVar[BaseExchangeConnector]):
        if issubclass(connector_class, BaseExchangeConnector):
            eid = connector_class.exchange_id()
            if eid not in self.__exchanges_codes:
                self.__exchanges_codes.append(connector_class.exchange_id())
        else:
            raise RuntimeError('Unexpected connector_class value!')

