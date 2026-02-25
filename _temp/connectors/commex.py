import json
import threading
import time
from typing import List, Optional

import requests
import websocket

from core.defs import CurrencyPair
from core.utils import utc_now_float
from core.connectors import BaseExchangeConnector, Ticker, BookTicker, BidAsk, BookDepth, CandleStick

from .utils import CustomExchangeConnector

COMMEX_API_HOST = "https://api.commex.com"
COMMEX_API_PREFIX = "/api/v1"
COMMEX_API_HEADERS = {'Content-Type': 'application/json'}
COMMEX_WS_URL = "wss://stream.commex.com/streams"


class CommexConnector(CustomExchangeConnector):

    def __init__(self, is_testing: bool = False):
        super().__init__(is_testing)
        self.__req_counter = 1

    @classmethod
    def exchange_id(cls) -> str:
        return 'commex'

    @classmethod
    def ws_url(cls) -> str:
        return COMMEX_WS_URL

    def start(self, cb: BaseExchangeConnector.Callback, symbols: List[str] = None, depth: bool = True):
        super().start(cb, symbols)

        stream_names = [f"{ex_sym.lower()}@bookTicker" for ex_sym in self._filtering_symbols]
        if depth:
            stream_names += [f"{ex_sym.lower()}@depth10" for ex_sym in self._filtering_symbols]
        self._ws.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": stream_names,
            "id": self.__req_counter
        }))
        self.__req_counter += 1

    def _fetch_tickers_from_server(self) -> List[Ticker]:
        url = '/ticker/price'
        spot_pairs_js_list = requests.request('GET', COMMEX_API_HOST + COMMEX_API_PREFIX + url,
                                              headers=COMMEX_API_HEADERS).json()

        res = []
        for d in spot_pairs_js_list:
            t = self.__from_commex_api_ticker(d)
            if t:
                res += [t]
        return res

    def get_pairs(self, symbols: List[str] = None) -> List[CurrencyPair]:
        if symbols is None:
            if not self._cached_tickers_dict:
                self.get_all_tickers()
            filtering_symbols = set({ticker.exchange_symbol for ticker in self._cached_tickers})
        else:
            filtering_symbols = set([sym.replace('/', '') for sym in symbols])

        url = '/ticker/price'
        tickers_list_js = requests.request('GET', COMMEX_API_HOST + COMMEX_API_PREFIX + url, headers=COMMEX_API_HEADERS).json()
        if type(tickers_list_js) != list:
            tickers_list_js = [tickers_list_js]
        pairs = []
        for js_ticker in tickers_list_js:
            if js_ticker["symbol"] in filtering_symbols:
                base, quite = self.__from_exchange_symbol(js_ticker["symbol"]).split('/')
                if not base:
                    continue
                pairs.append(
                    CurrencyPair(
                        base=base,
                        quota=quite,
                        ratio=float(js_ticker["price"]),
                        utc=utc_now_float()
                    )
                )
        return pairs

    def get_price(self, pair_code: str) -> Optional[CurrencyPair]:
        exchange_symbol = pair_code.replace('/', '').upper()
        query_param = f"symbol={exchange_symbol}"
        url = '/ticker/price'
        response = requests.request('GET', COMMEX_API_HOST + COMMEX_API_PREFIX + url + "?" + query_param,
                                           headers=COMMEX_API_HEADERS)
        if response.status_code != 200:
            return None

        ticker_js = response.json()
        base, quite = pair_code.split('/')
        return CurrencyPair(
            base=base,
            quota=quite,
            ratio=float(ticker_js["price"]),
            utc=utc_now_float()
            )

    def get_depth(self, symbol: str, limit: int = 100) -> Optional[BookDepth]:
        exchange_symbol = symbol.replace('/', '').upper()
        query_param = f"symbol={exchange_symbol}&limit={limit}"
        url = '/depth'
        order_book_js = requests.request('GET', COMMEX_API_HOST + COMMEX_API_PREFIX + url + "?" + query_param,
                                     headers=COMMEX_API_HEADERS).json()
        return BookDepth(
            symbol=symbol,
            bids=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in order_book_js["bids"]],
            asks=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in order_book_js["asks"]],
            exchange_symbol=exchange_symbol,
            last_update_id=order_book_js["lastUpdateId"],
            utc=utc_now_float()
        )

    def get_klines(self, symbol: str) -> Optional[List[CandleStick]]:
        exchange_symbol = symbol.replace('/', '').upper()
        query_param = f"symbol={exchange_symbol}&interval=1m&limit=60"
        url = '/klines'
        klines_array_js = requests.request('GET', COMMEX_API_HOST + COMMEX_API_PREFIX + url + "?" + query_param,
                                         headers=COMMEX_API_HEADERS).json()
        result = []
        for kline in klines_array_js:
            result += [
                CandleStick(
                    utc_open_time=float(kline[0])/1000,
                    open_price=float(kline[1]),
                    high_price=float(kline[2]),
                    low_price=float(kline[3]),
                    close_price=float(kline[4]),
                    coin_volume=float(kline[5])
                )
            ]

        return result[::-1]

    @staticmethod
    def __from_exchange_symbol(exchange_symbol: str) -> Optional[str]:
        exchange_symbol = exchange_symbol.upper()
        quotes = ["USDT", "BTC", "ETH", "USDC", "TUSD", "HT"]
        for q in quotes:
            if exchange_symbol.endswith(q):
                return f"{exchange_symbol[0:-len(q)]}/{q}"
        return None

    @classmethod
    def __from_commex_api_ticker(cls, d) -> Optional[Ticker]:
        exchange_symbol = d["symbol"]
        b_q = cls.__from_exchange_symbol(exchange_symbol)
        if b_q is None:
            return None
        base, quote = b_q.split('/')

        return Ticker(
            symbol=CurrencyPair.build_code(base=base.upper(), quota=quote.upper()),
            base=base.upper(),
            quote=quote.upper(),
            is_spot_enabled=True,
            is_margin_enabled=False,
            exchange_symbol=exchange_symbol
        )

    def _handle_ws_message(self, _, msg):
        if msg == "ping":
            self.__handle_ping()
        msg = json.loads(msg)["data"]
        if msg['e'] == "bookTicker":
            self.__handle_book_ticker(msg)
        elif msg['e'] == "depthUpdate":
            self.__handle_order_book(msg)

    def __handle_book_ticker(self, msg):
        ex_symbol = msg["s"].upper()
        if not self._throttler.may_pass(ex_symbol, tag='book'):
            return

        symbol = self.__from_exchange_symbol(ex_symbol)

        book_event = BookTicker(
            symbol=symbol,
            bid_price=float(msg["b"]),
            bid_qty=float(msg["B"]),
            ask_price=float(msg["a"]),
            ask_qty=float(msg["A"]),
            utc=float(msg["T"]) / 1000,
            last_update_id=msg["u"]
        )
        self._cb.handle(book=book_event)

    def __handle_order_book(self, msg):
        ex_symbol = msg["s"].upper()
        if not self._throttler.may_pass(ex_symbol, tag='depth'):
            return

        symbol = self.__from_exchange_symbol(ex_symbol)

        book_depth = BookDepth(
            symbol=symbol,
            bids=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in msg["b"]],
            asks=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in msg["a"]],
            exchange_symbol=ex_symbol,
            utc=float(msg["T"]) / 1000,
            last_update_id=msg['u']
        )
        self._cb.handle(depth=book_depth)

    def __handle_ping(self):
        self._ws.send("pong")
