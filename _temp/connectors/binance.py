import json
import logging
import queue
import threading
from typing import List, Optional, Dict, Union, Tuple, Type

from bs4 import BeautifulSoup
from django.conf import settings
from binance.spot import Spot
from binance.error import ClientError
from binance.websocket.spot.websocket_client import SpotWebsocketClient

import core.throttler
from core.exceptions import *
from core.defs import CurrencyPair
from core.http import AnonymousWebBrowser
from core.utils import utc_now_float
from core.connectors import BaseExchangeConnector, Ticker, BookTicker, \
    BidAsk, BookDepth, CandleStick
from core.p2p import P2PService, FiatInfo, TokenInfo, PaymentMethod, \
    TradeSide, P2PAdvert, P2PAdvertInfo, P2PAdvertiser

from .defs import *
from .utils import redis_get, redis_set, build_tickers_dict


class LocalCache:

    def __init__(self, throttle_timeout: float):
        self.throttler = core.throttler.Throttler(throttle_timeout)
        self.app_data = None


class BinanceP2PService(P2PService):

    _cache = LocalCache(throttle_timeout=60*60)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Map Fiat to info
        self._filter_cond: Dict[str, Dict] = {}
        self._config: Dict[str, Dict] = {}

    async def _create_session(self, socks: Optional[Tuple[str, int]]) -> AnonymousWebBrowser:
        return AnonymousWebBrowser(
            base_url='https://p2p.binance.com',
            socks=socks
        )

    async def load_fiat_list(self) -> List[FiatInfo]:
        browser: AnonymousWebBrowser
        app_data = await self._get_app_data()
        fiat_parent = app_data['pageData']['redux']['reactQuery']['hydrate']
        key = [k for k in fiat_parent.keys() if 'fiat' in k]
        key = key[0] if key else None
        if key:
            resp = fiat_parent[key]
            if resp['success']:
                fiat_list = []
                for item in resp['data']:
                    fiat_list.append(
                        FiatInfo(
                            code=item['currencyCode'],
                            country_code=item.get('countryCode'),
                            symbol=item.get('currencySymbol')
                        )
                    )
                return [fiat for fiat in fiat_list if fiat.code != 'ALL']
            else:
                raise HTMLParserError(resp['message'])
        else:
            raise HTMLParserError('Unexpected HTML/JSON structure')

    async def load_tokens(self, fiat: str) -> List[TokenInfo]:
        cfg = await self._get_config(fiat)
        areas = cfg['areas']
        p2p_area = None
        for a in areas:
            if a['area'] == 'P2P':
                p2p_area = a
                break
        if not p2p_area:
            raise BrowserOperationError('Unexpected filters collection struct')
        tokens = []
        for side in p2p_area['tradeSides']:
            assets = [asset['asset'] for asset in side.get('assets')]
            tokens.extend(assets)
        tokens = list(set(tokens))
        return [TokenInfo(code=code) for code in tokens]

    async def load_payment_methods(self, fiat: str) -> List[PaymentMethod]:
        filters = await self._get_filters_cond(fiat)
        methods = []
        for meth in filters['tradeMethods']:
            methods.append(
                PaymentMethod(
                    code=meth['identifier'],
                    label=meth['tradeMethodName'],
                    extra=meth
                )
            )
        return methods

    async def load_adv_page(
        self, side: TradeSide, fiat: str,
        token: str, page: int, size: int = None
    ) -> List[P2PAdvert]:
        async with self.session(mono=False) as browser:
            resp = await browser.post(
                '/bapi/c2c/v2/friendly/c2c/adv/search',
                js={
                    "fiat": fiat,
                    "page": page,
                    "rows": size or 10,

                    # объявления мейкеров работают наоборот
                    "tradeType": "SELL" if side == TradeSide.BUY else "BUY",

                    "asset": token,
                    "countries": [],
                    "proMerchantAds": False,
                    "shieldMerchantAds": False,
                    "publisherType": None,
                    "payTypes": []
                }
            )
            if not resp['success']:
                raise BrowserOperationError(resp['message'])
            advs = []
            for item in resp['data']:
                _ = item['adv']
                info = P2PAdvertInfo(
                    uid=_['advNo'],
                    price=float(_['price']),
                    quantity=float(_['tradableQuantity']),
                    min_max_qty=(
                        float(_['minSingleTransQuantity']),
                        float(_['maxSingleTransQuantity'])
                    ),
                    pay_methods=[
                        m['identifier']
                        for m in _['tradeMethods']
                    ],
                    extra=_
                )
                _ = item['advertiser']
                rating_ = _.get('positiveRate')
                if rating_ is not None:
                    rating_ *= 100

                if 'merchant' in _.get('userIdentity', '').lower():
                    verified_merchant = True
                else:
                    verified_merchant = False
                advertiser = P2PAdvertiser(
                    uid=_['userNo'],
                    nick=_['nickName'],
                    rating=rating_,
                    finish_rate=_['monthFinishRate'] * 100,
                    trades_month=_['monthOrderCount'],
                    trades_total=_['orderCount'],
                    verified_merchant=verified_merchant,
                    extra=_
                )
                advs.append(
                    P2PAdvert(
                        info=info,
                        advertiser=advertiser,
                        utc=utc_now_float()
                    )
                )
            return advs

    async def _get_app_data(self) -> dict:
        if self._cache.throttler.may_pass('app_data'):
            self._cache.app_data = None
        if not self._cache.app_data:
            async with self.session() as browser:
                # Load app data
                html = await browser.get('/')
                soup = BeautifulSoup(html)
                script = soup.find('script', {'id': '__APP_DATA'})
                try:
                    self._cache.app_data = json.loads(script.string)
                except json.JSONDecodeError as e:
                    raise SerializationError(e.msg)
        return self._cache.app_data

    async def _get_filters_cond(self, fiat: str) -> dict:
        if not self._filter_cond.get(fiat):
            async with self.session(mono=True) as browser:
                # Load filters cond
                resp = await browser.post(
                    '/bapi/c2c/v2/public/c2c/adv/filter-conditions',
                    js={'fiat': fiat}
                )
                if not resp['success']:
                    raise BrowserOperationError(resp['message'])
                self._filter_cond[fiat] = resp['data']
        return self._filter_cond[fiat]

    async def _get_config(self, fiat: str) -> dict:
        if not self._config.get(fiat):
            async with self.session(mono=True) as browser:
                # Load config
                resp = await browser.post(
                    '/bapi/c2c/v2/friendly/c2c/portal/config',
                    js={'fiat': fiat}
                )
                if not resp['success']:
                    raise BrowserOperationError(resp['message'])
                self._config[fiat] = resp['data']
        return self._config[fiat]


class BinanceConnector(BaseExchangeConnector):

    def __init__(self, is_testing: bool = False):
        super().__init__(is_testing)
        self.__client = Spot()
        self.__cached_tickers: Optional[List[Ticker]] = None
        self.__cached_tickers_dict: Dict[str, Ticker] = {}
        self.__ws: Optional[SpotWebsocketClient] = None
        self.__event_queue = queue.Queue()
        self.__read_event = threading.Event()
        self.__cb_thread: Optional[threading.Thread] = None
        self.__cb_terminated = True
        self.__cb: Optional[BaseExchangeConnector.Callback] = None
        self.__redis_cache_key = f'connector/{self.exchange_id()}/cached'

    @classmethod
    def exchange_id(cls) -> str:
        return 'binance'

    def start(self, cb: BaseExchangeConnector.Callback, symbols: List[str] = None, depth: bool = True):
        if not self.__cached_tickers_dict:
            self.get_all_tickers()
        self.__ws = SpotWebsocketClient()
        self.__ws.start()
        symbols = list(self.__cached_tickers_dict.keys()) if not symbols else symbols
        filtering_symbols = [sym.replace('/', '').lower() for sym in symbols]
        self.__cb = cb
        if self.__cb_thread is None:
            self.__cb_thread = threading.Thread(target=self.__cb_routine)
            self.__cb_thread.daemon = True
            self.__cb_terminated = False
            self.__cb_thread.start()

        def __callback__(msg):
            # Twisted thread context
            self.__event_queue.put(msg)
            self.__read_event.set()

        self.__ws.book_ticker(
            id=id(self), callback=__callback__, symbol=filtering_symbols
        )

        if depth:
            self.__ws.partial_book_depth(
                symbol=filtering_symbols,
                id=id(self),
                level=DEF_BOOKS_DEPTH_LEVEL,
                speed=DEF_TICKERS_SPEED_MS,
                callback=__callback__
            )

    def stop(self):
        self.__ws.stop()
        self.__ws = None
        self.__cb_terminated = True
        self.__read_event.set()
        self.__cb_thread = True

    def get_all_tickers(self) -> List[Ticker]:
        if self.__cached_tickers:
            return self.__cached_tickers
        cached_tickers = redis_get(self.__redis_cache_key)
        if cached_tickers:
            self.__cached_tickers = [Ticker.from_json(json.dumps(tk)) for tk in cached_tickers]
            self.__cached_tickers_dict = build_tickers_dict(self.__cached_tickers)
            return self.__cached_tickers

        tickers = []
        infos = self.__client.exchange_info()
        for symbol in infos['symbols']:
            if symbol['status'] == 'TRADING' and 'MARKET' in symbol['orderTypes']:
                if symbol['isSpotTradingAllowed'] and symbol['isMarginTradingAllowed']:
                    tickers.append(
                        Ticker(
                            symbol=CurrencyPair.build_code(base=symbol['baseAsset'], quota=symbol['quoteAsset']),
                            base=symbol['baseAsset'],
                            quote=symbol['quoteAsset'],
                            is_spot_enabled=symbol['isSpotTradingAllowed'],
                            is_margin_enabled=symbol['isMarginTradingAllowed'],
                            exchange_symbol=symbol['symbol']
                        )
                    )
        self.__cached_tickers = tickers
        self.__cached_tickers_dict = build_tickers_dict(tickers)
        redis_set(
            self.__redis_cache_key,
            [tk.as_json() for tk in tickers],
            ttl=settings.EXCHANGE_CONNECTOR_CACHE_TIMEOUT
        )
        return tickers

    def get_price(self, pair_code: str) -> Optional[CurrencyPair]:
        if not self.__cached_tickers_dict:
            self.get_all_tickers()
        symbol = pair_code.replace('/', '')
        ticker: Ticker = self.__cached_tickers_dict.get(symbol)
        if not ticker:
            return None
        try:
            price = self.__client.ticker_price(symbol=symbol)
        except ClientError:
            return None
        if ticker:
            return CurrencyPair(
                base=ticker.base,
                quota=ticker.quote,
                ratio=float(price['price']),
                utc=utc_now_float()
            )
        else:
            return None

    def get_pairs(self, symbols: List[str] = None) -> List[CurrencyPair]:
        if symbols is None:
            if not self.__cached_tickers_dict:
                self.get_all_tickers()
            filtering_symbols = list(self.__cached_tickers_dict.keys())
        else:
            filtering_symbols = [sym for sym in symbols]
        filtering_symbols = [sym.replace('/', '') for sym in filtering_symbols]

        fetched_prices = []
        page_sz = 300
        for i in range(0, len(filtering_symbols), page_sz):
            chunk = filtering_symbols[i:i + page_sz]
            prices = self.__client.ticker_price(symbols=chunk)
            fetched_prices.extend(prices)

        pairs = []
        for price in fetched_prices:
            ticker: Ticker = self.__cached_tickers_dict.get(price['symbol'])
            if ticker:
                pairs.append(
                    CurrencyPair(
                        base=ticker.base,
                        quota=ticker.quote,
                        ratio=float(price['price']),
                        utc=utc_now_float()
                    )
                )
        return pairs

    def get_depth(self, symbol: str, limit: int = 100) -> Optional[BookDepth]:
        ex_sym = self.__get_exchange_symbol(symbol)
        if ex_sym:
            data = self.__client.depth(symbol=ex_sym, limit=limit)
            return self.__deserialize_depth(symbol, ex_sym, data)
        else:
            return None

    def get_klines(self, symbol: str) -> Optional[List[CandleStick]]:
        ex_sym = self.__get_exchange_symbol(symbol)
        if ex_sym:
            items = self.__client.klines(
                symbol=ex_sym, interval='1m',
                limit=KLINE_WINDOW_SECS // 60
            )
            klines = [
                self.__deserialize_candle_stick(symbol, i) for i in items
            ]
            return sorted(klines, key=lambda i: -i.utc_open_time)
        else:
            return None

    @classmethod
    def p2p(cls) -> Optional[Type[P2PService]]:
        return BinanceP2PService

    def __get_exchange_symbol(self, symbol: str) -> Optional[str]:
        if not self.__cached_tickers_dict:
            self.get_all_tickers()
        ticker = self.__cached_tickers_dict.get(symbol)
        if ticker:
            return ticker.exchange_symbol
        else:
            return None

    def __cb_routine(self):
        while not self.__cb_terminated:
            self.__read_event.wait()
            if self.__cb_terminated:
                return
            while not self.__event_queue.empty():
                raw_msg = self.__event_queue.get()
                msg = self.__raw_msg_to_event(raw_msg)
                if self.__cb:
                    if isinstance(msg, BookTicker):
                        if self._throttler.may_pass(msg.symbol, tag='book'):
                            self.__cb.handle(book=msg)
                    elif isinstance(msg, BookDepth):
                        if self._throttler.may_pass(msg.symbol, tag='depth'):
                            self.__cb.handle(depth=msg)
                else:
                    return

    def __raw_msg_to_event(self, msg: Dict) -> Union[None, BookTicker, BookDepth]:
        stream = msg.get('stream')
        if not stream:
            return None
        stream_parts = stream.split('@')
        symbol = stream_parts[0]
        ticker: Ticker = self.__cached_tickers_dict.get(symbol.upper())
        data = msg.get('data')
        if not ticker or not data:
            return None
        if stream.endswith('@bookTicker'):
            book = BookTicker(
                symbol=ticker.symbol,
                bid_price=float(data['b']),
                bid_qty=float(data['B']),
                ask_price=float(data['a']),
                ask_qty=float(data['A']),
                last_update_id=data['u'],
                utc=utc_now_float()
            )
            return book
        elif '@depth' in stream:
            return self.__deserialize_depth(
                ticker.symbol, ticker.exchange_symbol, data
            )

    @classmethod
    def __deserialize_depth(
        cls, symbol: str, exchange_symbol: str, data: Dict
    ) -> BookDepth:
        bids = []
        asks = []
        for item in data['bids']:
            price, qty = item
            bids.append(BidAsk(price=float(price), quantity=float(qty)))
        for item in data['asks']:
            price, qty = item
            asks.append(BidAsk(price=float(price), quantity=float(qty)))
        depth = BookDepth(
            symbol=symbol,
            exchange_symbol=exchange_symbol,
            bids=bids,
            asks=asks,
            last_update_id=data['lastUpdateId'],
            utc=utc_now_float()
        )
        return depth

    @classmethod
    def __deserialize_candle_stick(cls, symbol: str, data: List) -> CandleStick:
        base, quote = symbol.split('/')
        quote_volume = float(data[7])
        if quote in PERPETUAL_TOKENS:
            usd_volume = quote_volume
        else:
            usd_volume = None
        return CandleStick(
            utc_open_time=float(data[0]/1000),
            open_price=float(data[1]),
            high_price=float(data[2]),
            low_price=float(data[3]),
            close_price=float(data[4]),
            coin_volume=float(data[5]),
            usd_volume=usd_volume
        )
