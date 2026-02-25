import json
import threading
from typing import List, Optional

import requests

from core.defs import CurrencyPair
from core.utils import utc_now_float
from core.connectors import BaseExchangeConnector, Ticker, BookTicker, BidAsk, BookDepth, CandleStick

from .utils import CustomExchangeConnector

BITGET_API_HOST = "https://api.bitget.com"
BITGET_API_PREFIX = "/api/v2"
BITGET_API_HEADERS = {'Content-Type': 'application/json'}
BITGET_WS_URL = "wss://ws.bitget.com/v2/ws/public"


class BitgetConnector(CustomExchangeConnector):

    def __init__(self, is_testing: bool = False):
        super().__init__(is_testing)
        self.__ping_timer = None

    @classmethod
    def exchange_id(cls) -> str:
        return 'bitget'

    @classmethod
    def ws_url(cls) -> str:
        return BITGET_WS_URL

    def start(self, cb: BaseExchangeConnector.Callback, symbols: List[str] = None, depth: bool = True):
        super().start(cb, symbols)

        self._ws.send(json.dumps({
            "op": "subscribe",
            "args": [
                {
                    "instType": "SPOT",
                    "channel": "ticker",
                    "instId": ex_sym
                }
                for ex_sym in self._filtering_symbols
            ]
        }))

        if depth:
            self._ws.send(json.dumps({
                "op": "subscribe",
                "args": [
                    {
                        "instType": "SPOT",
                        "channel": "books5",
                        "instId": ex_sym
                    }
                    for ex_sym in self._filtering_symbols
                ]
            }))

        self.__send_ping()

    def stop(self):
        self.__ping_timer.cancel()
        super().stop()

    def _fetch_tickers_from_server(self) -> List[Ticker]:
        url = '/spot/public/symbols'
        spot_pairs_js_list = requests.request('GET', BITGET_API_HOST + BITGET_API_PREFIX + url,
                                              headers=BITGET_API_HEADERS).json()["data"]

        res = []
        for d in spot_pairs_js_list:
            t = self.__from_bitget_api_ticker(d)
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

        url = '/spot/market/tickers'
        tickers_list_js = requests.request('GET', BITGET_API_HOST + BITGET_API_PREFIX + url, headers=BITGET_API_HEADERS).json()["data"]
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
                        ratio=float(js_ticker["lastPr"]),
                        utc=utc_now_float()
                    )
                )
        return pairs

    def get_price(self, pair_code: str) -> Optional[CurrencyPair]:
        exchange_symbol = pair_code.replace('/', '').upper()
        query_param = f"symbol={exchange_symbol}"
        url = '/spot/market/tickers'
        response = requests.request('GET', BITGET_API_HOST + BITGET_API_PREFIX + url + "?" + query_param,
                                           headers=BITGET_API_HEADERS)
        if response.status_code != 200:
            return None

        data = response.json()["data"]
        if not data:
            return None
        ticker_js = data[0]
        base, quite = pair_code.split('/')
        return CurrencyPair(
            base=base,
            quota=quite,
            ratio=float(ticker_js["lastPr"]),
            utc=utc_now_float()
            )

    def get_depth(self, symbol: str, limit: int = 100) -> Optional[BookDepth]:
        exchange_symbol = symbol.replace('/', '').upper()
        query_param = f"symbol={exchange_symbol}&limit={limit}"
        url = '/spot/market/orderbook'
        order_book_js = requests.request('GET', BITGET_API_HOST + BITGET_API_PREFIX + url + "?" + query_param,
                                     headers=BITGET_API_HEADERS).json()["data"]
        return BookDepth(
            symbol=symbol,
            bids=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in order_book_js["bids"]],
            asks=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in order_book_js["asks"]],
            exchange_symbol=exchange_symbol,
            last_update_id=f'{exchange_symbol}{order_book_js["ts"]}',
            utc=float(order_book_js["ts"])/1000
        )

    def get_klines(self, symbol: str) -> Optional[List[CandleStick]]:
        exchange_symbol = symbol.replace('/', '')
        query_param = f"symbol={exchange_symbol}&granularity=1min&limit=60"
        url = '/spot/market/candles'
        klines_array_js = requests.request('GET', BITGET_API_HOST + BITGET_API_PREFIX + url + "?" + query_param,
                                         headers=BITGET_API_HEADERS).json()["data"]
        result = []
        for kline in klines_array_js:
            result += [
                CandleStick(
                    utc_open_time=float(kline[0])/1000,
                    open_price=float(kline[1]),
                    high_price=float(kline[2]),
                    low_price=float(kline[3]),
                    close_price=float(kline[4]),
                    coin_volume=float(kline[5]),
                    usd_volume=float(kline[7])
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
    def __from_bitget_api_ticker(cls, d) -> Optional[Ticker]:
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
        msg = json.loads(msg)
        channel = msg["arg"]["channel"]
        if channel == "ticker":
            self.__handle_book_ticker(msg)
        elif channel.startswith("books"):
            self.__handle_order_book(msg)

    def __handle_book_ticker(self, msg):
        if 'data' not in msg:
            return
        ex_symbol = msg["arg"]["instId"]
        if not self._throttler.may_pass(ex_symbol, tag='book'):
            return

        symbol = self.__from_exchange_symbol(ex_symbol)

        d = msg['data'][0]
        book_event = BookTicker(
            symbol=symbol,
            bid_price=float(d["bidPr"]),
            bid_qty=float(d["bidSz"]),
            ask_price=float(d["askPr"]),
            ask_qty=float(d["askSz"]),
            utc=float(d["ts"]) / 1000,
            last_update_id=f'{ex_symbol}{d["ts"]}'
        )
        self._cb.handle(book=book_event)

    def __handle_order_book(self, msg):
        if 'data' not in msg:
            return
        ex_symbol = msg["arg"]["instId"]
        if not self._throttler.may_pass(ex_symbol, tag='depth'):
            return

        symbol = self.__from_exchange_symbol(ex_symbol)
        d = msg['data'][0]

        book_depth = BookDepth(
            symbol=symbol,
            bids=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in d["bids"]],
            asks=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in d["asks"]],
            exchange_symbol=ex_symbol,
            utc=float(d["ts"]) / 1000,
            last_update_id=f'{ex_symbol}{d["ts"]}'
        )
        self._cb.handle(depth=book_depth)

    def __send_ping(self):
        if self._ws:
            self._ws.send("ping")
        self.__ping_timer = threading.Timer(30, self.__send_ping)
        self.__ping_timer.start()
