import json
import queue
import threading
from typing import List, Optional, Dict, Any

import okx.MarketData as MarketData
from django.conf import settings

from core.exceptions import ExchangeResponseError
from core.connectors import (
    BaseExchangeConnector, Ticker, BookTicker, BidAsk, BookDepth, CandleStick
)
from core.utils import utc_now_float
from core.defs import CurrencyPair

from .scheduling import WebSocketAssist
from .utils import build_tickers_dict, redis_get, redis_set
from .defs import PERPETUAL_TOKENS, KLINE_WINDOW_SECS


class OkxConnector(BaseExchangeConnector):

    # Docs: https://www.okx.com/docs-v5/en/
    WS_URL_PROD = 'wss://ws.okx.com:8443/ws/v5/public'
    WS_URL_TEST = 'wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999'

    class WsListener(WebSocketAssist.Listener):

        def __init__(self, q: queue.Queue):
            self.__queue: queue.Queue = q

        def on(self, data: Optional[Any], closed: bool):
            self.__queue.put_nowait([data, closed])

    def __init__(self, is_testing: bool = False):
        super().__init__(is_testing)
        self.__cached_tickers: Optional[List[Ticker]] = None
        self.__cached_tickers_dict: Dict[str, Ticker] = {}
        self.__ws: Optional[WebSocketAssist] = None
        self.__events_th: Optional[threading.Thread] = None
        self.__events_queue: Optional[queue.Queue] = None
        self.__started = False
        self.__redis_cache_key = f'connector/{self.exchange_id()}/cached'

    @classmethod
    def exchange_id(cls) -> str:
        return 'okx'

    def start(self, cb: BaseExchangeConnector.Callback, symbols: List[str] = None, depth: bool = True):
        if self.__started:
            # Пока не поддерживается одновременная работа по нескольким listen таскам
            raise RuntimeError('You already activate listen task. Stop old ones at first!')
        self.__ws = WebSocketAssist(url=self.WS_URL_TEST if self._is_testing else self.WS_URL_PROD)
        ok = self.__ws.connect()
        if not ok:
            raise RuntimeError('Error while connect to OKX websocket Endpoint!')
        if not self.__cached_tickers_dict:
            self.get_all_tickers()
        symbols = list(self.__cached_tickers_dict.keys()) if not symbols else symbols
        filtering_symbols = [sym.replace('/', '-').upper() for sym in symbols if sym in self.__cached_tickers_dict]
        self.__events_queue = queue.Queue()
        listener = self.WsListener(self.__events_queue)
        self.__events_th = threading.Thread(
            target=self.__events_fetch_routine, args=(cb, self.__events_queue)
        )
        self.__events_th.daemon = True
        self.__events_th.start()
        self.__ws.subscribe(listener)
        cmd1 = {
            "op": "subscribe",
            "args": [
                {"channel": "tickers", "instId": sym}
                for sym in filtering_symbols
            ]
        }
        cmd2 = {
            "op": "subscribe",
            "args": [
                {"channel": "books5", "instId": sym}
                for sym in filtering_symbols
            ]
        }
        self.__ws.send(cmd1)
        if depth:
            self.__ws.send(cmd2)
        self.__started = True

    def stop(self):
        if self.__started:
            self.__started = False
            self.__events_queue.put_nowait([None, True])
            self.__events_th.join(timeout=3.0)
            self.__events_queue = None
            self.__events_th = None
            pass

    def get_all_tickers(self) -> List[Ticker]:
        if self.__cached_tickers:
            return self.__cached_tickers
        cached_tickers = redis_get(self.__redis_cache_key)
        if cached_tickers:
            self.__cached_tickers = [Ticker.from_json(json.dumps(tk)) for tk in cached_tickers]
            self.__cached_tickers_dict = build_tickers_dict(self.__cached_tickers)
            return self.__cached_tickers

        market_data = self.__fetch_market_data()
        resp = market_data.get_tickers(instType="SPOT")
        if resp['code'] == '0':
            items = resp['data']
            tickers = []
            for item in items:
                base, quota = item['instId'].split('-')
                tickers.append(
                    Ticker(
                        symbol=CurrencyPair.build_code(base=base, quota=quota),
                        base=base,
                        quote=quota,
                        is_spot_enabled=item['instType'] == 'SPOT',
                        is_margin_enabled=False,  # TODO
                        exchange_symbol=item['instId']
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
        else:
            raise ExchangeResponseError(resp['msg'])

    def get_price(self, pair_code: str) -> Optional[CurrencyPair]:
        inst_id = pair_code.replace('/', '-')
        market_data = self.__fetch_market_data()
        resp = market_data.get_ticker(inst_id)
        if resp['code'] == '0':
            inst = resp['data'][0]
            base, quota = inst['instId'].split('-')
            return CurrencyPair(
                base=base,
                quota=quota,
                ratio=float(inst['last']),
                utc=utc_now_float()
            )
        else:
            return None

    def get_pairs(self, symbols: List[str] = None) -> List[CurrencyPair]:
        market_data = self.__fetch_market_data()
        resp = market_data.get_tickers(instType="SPOT")
        if resp['code'] == '0':
            pairs = []
            items = resp['data']
            for item in items:
                base, quota = item['instId'].split('-')
                pairs.append(
                    CurrencyPair(
                        base=base,
                        quota=quota,
                        ratio=float(item['bidPx']),
                        utc=utc_now_float()
                    )
                )
            return pairs
        else:
            raise ExchangeResponseError(resp['msg'])

    def get_depth(self, symbol: str, limit: int = 100) -> Optional[BookDepth]:
        ex_sym = self.__get_exchange_symbol(symbol)
        market_data = self.__fetch_market_data()
        resp = market_data.get_orderbook(instId=ex_sym, sz=str(limit))
        if resp['code'] == '0':
            items = resp['data']
            data = items[0]
            data['instId'] = ex_sym
            depth = self.__raw_data_to_depth_event(data)
            return depth
        else:
            raise ExchangeResponseError(resp['msg'])

    def get_klines(self, symbol: str) -> Optional[List[CandleStick]]:
        ex_sym = self.__get_exchange_symbol(symbol)
        market_data = self.__fetch_market_data()
        resp = market_data.get_candlesticks(
            instId=ex_sym, bar='1m',
            limit=KLINE_WINDOW_SECS // 60
        )
        if resp['code'] == '0':
            items = resp['data']
            klines = [
                self.__deserialize_candle_stick(symbol, i) for i in items
            ]
            return klines
        else:
            raise ExchangeResponseError(resp['msg'])

    def __get_exchange_symbol(self, symbol: str) -> Optional[str]:
        if not self.__cached_tickers_dict:
            self.get_all_tickers()
        ticker = self.__cached_tickers_dict.get(symbol)
        if ticker:
            return ticker.exchange_symbol
        else:
            return None

    def __events_fetch_routine(self, cb: BaseExchangeConnector.Callback, q: queue.Queue):
        while True:
            item = q.get()
            data, closed = item
            if closed:
                return
            channel = data.get('arg', {}).get('channel', None)
            if channel == 'tickers' and 'data' in data:
                data_items = data['data']
                for data_item in data_items:
                    event = self.__raw_data_to_book_event(data_item)
                    if event and self._throttler.may_pass(event.symbol, tag='book'):
                        cb.handle(book=event)
            elif channel.startswith('book') and 'data' in data:
                data_items = data['data']
                for data_item in data_items:
                    event = self.__raw_data_to_depth_event(data_item)
                    if event and self._throttler.may_pass(event.symbol, tag='depth'):
                        cb.handle(depth=event)

    def __fetch_market_data(self):
        return MarketData.MarketAPI(flag="1" if self._is_testing else "0")

    def __raw_data_to_book_event(self, data: Dict) -> Optional[BookTicker]:
        if data:
            symbol = data.get('instId')
            ticker: Ticker = self.__cached_tickers_dict.get(symbol.upper())
            book = BookTicker(
                symbol=ticker.symbol,
                bid_price=float(data['bidPx']),
                bid_qty=float(data['bidSz']),
                ask_price=float(data['askPx']),
                ask_qty=float(data['askSz']),
                last_update_id=int(data['ts']),
                utc=utc_now_float()
            )
            return book
        else:
            return None

    def __raw_data_to_depth_event(self, data: Dict) -> Optional[BookDepth]:
        if data:
            symbol = data.get('instId')
            ticker: Ticker = self.__cached_tickers_dict.get(symbol.upper())
            bids = []
            asks = []
            for item in data['bids']:
                price, qty, _, _ = item
                bids.append(BidAsk(price=float(price), quantity=float(qty)))
            for item in data['asks']:
                price, qty, _, _ = item
                asks.append(BidAsk(price=float(price), quantity=float(qty)))
            depth = BookDepth(
                symbol=ticker.symbol,
                exchange_symbol=ticker.exchange_symbol,
                bids=bids,
                asks=asks,
                last_update_id=int(data['ts']),
                utc=utc_now_float()
            )
            return depth
        else:
            return None

    @classmethod
    def __deserialize_candle_stick(
        cls, symbol: str, data: List
    ) -> CandleStick:
        base, quote = symbol.split('/')
        quote_volume = float(data[6])
        if quote in PERPETUAL_TOKENS:
            usd_volume = quote_volume
        else:
            usd_volume = None
        return CandleStick(
            utc_open_time=float(data[0]) / 1000,
            open_price=float(data[1]),
            high_price=float(data[2]),
            low_price=float(data[3]),
            close_price=float(data[4]),
            coin_volume=float(data[5]),
            usd_volume=usd_volume
        )
