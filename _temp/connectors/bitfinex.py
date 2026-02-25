import json
import threading
from dataclasses import dataclass
from typing import List, Optional, Dict

import requests

from core.defs import CurrencyPair
from core.utils import utc_now_float
from core.connectors import BaseExchangeConnector, Ticker, BookTicker, BidAsk, BookDepth, CandleStick
import collections.abc

from sortedcontainers import SortedDict

from .utils import CustomExchangeConnector

BITFINEX_API_HOST = "https://api-pub.bitfinex.com"
BITFINEX_API_PREFIX = "/v2"
BITFINEX_API_HEADERS = {'Content-Type': 'application/json'}
BITFINEX_WS_URL = "wss://api-pub.bitfinex.com/ws/2"


class Depth:
    def __init__(self):
        self.bids: SortedDict[float, float] = SortedDict()  # price -> amount
        self.asks: SortedDict[float, float] = SortedDict()  # # price -> amount


class BitfinexConnector(CustomExchangeConnector):

    def __init__(self, is_testing: bool = False):
        super().__init__(is_testing)
        self.__ticker_ids: Dict[int, str] = {} # id -> exchange symbol
        self.__book_ids: Dict[int, str] = {}  # id -> exchange symbol
        self.__book_snapshot: Dict[str, Depth] = {}  # exchange symbol -> Depth

    @classmethod
    def exchange_id(cls) -> str:
        return 'bitfinex'

    @classmethod
    def ws_url(cls) -> str:
        return BITFINEX_WS_URL

    def start(self, cb: BaseExchangeConnector.Callback, symbols: List[str] = None, depth: bool = True):
        super().start(cb, symbols)

        self.__ticker_ids.clear()
        self.__book_ids.clear()
        self.__book_snapshot.clear()

        for ex_sym in self._filtering_symbols:
            self._ws.send(json.dumps({
                "event": "subscribe",
                "channel": "ticker",
                "symbol": ex_sym
            }))

            if depth:
                self._ws.send(json.dumps({
                    "event": "subscribe",
                    "channel": "book",
                    "symbol": ex_sym
                }))

    def _fetch_tickers_from_server(self) -> List[Ticker]:
        url = '/conf/pub:list:pair:exchange'
        spot_pairs_js_list = requests.request('GET', BITFINEX_API_HOST + BITFINEX_API_PREFIX + url,
                                              headers=BITFINEX_API_HEADERS).json()[0]

        res = []
        for d in spot_pairs_js_list:
            t = self.__from_bitfinex_api_ticker(d)
            if t:
                res += [t]
        return res

    def get_pairs(self, symbols: List[str] = None) -> List[CurrencyPair]:
        if symbols:
            if not self._cached_tickers_dict:
                self.get_all_tickers()
            filtering_ex_symbols = []
            for symbol in symbols:
                ex_symbol = self.__get_ex_symbol(symbol)
                if ex_symbol:
                    filtering_ex_symbols += [ex_symbol]
            query_param = f"symbols={','.join(filtering_ex_symbols)}"

        else:
            query_param = f"symbols=ALL"
        url = '/tickers'
        tickers_list_js = requests.request('GET', BITFINEX_API_HOST + BITFINEX_API_PREFIX + url + "?" + query_param,
                                           headers=BITFINEX_API_HEADERS).json()
        pairs = []
        for js_ticker in tickers_list_js:
            b_q = self.__from_exchange_symbol(js_ticker[0])
            if not b_q:
                continue
            base, quite = b_q.split('/')
            pairs.append(
                CurrencyPair(
                    base=base,
                    quota=quite,
                    ratio=float(js_ticker[7]),
                    utc=utc_now_float()
                )
            )
        return pairs

    def get_price(self, pair_code: str) -> Optional[CurrencyPair]:
        base, quote = pair_code.split('/')
        ex_symbol = self.__get_ex_symbol(pair_code)
        if not ex_symbol:
            return None
        query_param = f"symbols={ex_symbol}"

        url = '/tickers'
        js_ticker = requests.request('GET', BITFINEX_API_HOST + BITFINEX_API_PREFIX + url + "?" + query_param,
                                           headers=BITFINEX_API_HEADERS).json()[0]
        return CurrencyPair(
            base=base,
            quota=quote,
            ratio=float(js_ticker[7]),
            utc=utc_now_float()
        )

    def __get_ex_symbol(self, pair_code):
        base, quote = pair_code.split('/')
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        for cached_ticker in self._cached_tickers:
            if cached_ticker.base == base and cached_ticker.quote == quote:
                return cached_ticker.exchange_symbol
        return None

    def get_depth(self, symbol: str, limit: int = 100) -> Optional[BookDepth]:
        exchange_symbol = self.__get_ex_symbol(symbol)
        query_param = f"len=25"
        url = f'/book/{exchange_symbol}/P0'
        order_book_js = requests.request('GET', BITFINEX_API_HOST + BITFINEX_API_PREFIX + url + "?" + query_param,
                                     headers=BITFINEX_API_HEADERS).json()
        bids, asks = [], []
        for row in order_book_js:
            if float(row[2]) > 0:
                bids.append(row)
            else:
                asks.append(row)
        return BookDepth(
            symbol=symbol,
            bids=[BidAsk(price=float(row[0]), quantity=float(row[2])) for row in bids],
            asks=[BidAsk(price=float(row[0]), quantity=-float(row[2])) for row in asks],
            exchange_symbol=exchange_symbol,
            last_update_id=f'{exchange_symbol}{utc_now_float()}',
            utc=utc_now_float()
        )

    def get_klines(self, symbol: str) -> Optional[List[CandleStick]]:
        exchange_symbol = self.__get_ex_symbol(symbol)
        url = f'/candles/trade:1m:{exchange_symbol}/hist'
        query_param = f"limit=60"
        klines_array_js = requests.request('GET', BITFINEX_API_HOST + BITFINEX_API_PREFIX + url + "?" + query_param,
                                         headers=BITFINEX_API_HEADERS).json()
        result = []
        for kline in klines_array_js:
            result += [
                CandleStick(
                    utc_open_time=float(kline[0])/1000,
                    open_price=float(kline[1]),
                    close_price=float(kline[2]),
                    high_price=float(kline[3]),
                    low_price=float(kline[4]),
                    coin_volume=float(kline[5])
                )
            ]

        return result

    @staticmethod
    def __from_exchange_symbol(exchange_symbol: str) -> Optional[str]:
        if exchange_symbol[0] != 't':
            return None
        exchange_symbol = exchange_symbol[1:]
        if ':' in exchange_symbol:
            return exchange_symbol.replace(':', '/', 1)

        quotes = ["USDT", "BTC", "ETH", "USDC", "TUSD", "HT", "USD", "EUR"]
        for q in quotes:
            if exchange_symbol.endswith(q):
                return f"{exchange_symbol[0:-len(q)]}/{q}"
        return None

    @classmethod
    def __from_bitfinex_api_ticker(cls, d) -> Optional[Ticker]:
        exchange_symbol = f't{d}'
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
        if msg[0] == '{':
            msg = json.loads(msg)
            self.__handle_subscribed(msg)
        elif msg[0] == '[':
            msg = json.loads(msg)
            if msg[0] in self.__ticker_ids:
                self.__handle_book_ticker(msg)
            elif msg[0] in self.__book_ids:
                self.__handle_order_book(msg)

    def __handle_subscribed(self, msg: dict):
        if msg["channel"] == "ticker":
            self.__ticker_ids[int(msg["chanId"])] = msg["symbol"]
        elif msg["channel"] == "book":
            self.__book_ids[int(msg["chanId"])] = msg["symbol"]

    def __handle_book_ticker(self, msg):
        ex_symbol = self.__ticker_ids[msg[0]]
        if not (len(msg) > 1 and isinstance(msg[1], collections.abc.Sequence) and len(msg[1]) >= 4):
            return
        if not self._throttler.may_pass(ex_symbol, tag='book'):
            return

        symbol = self.__from_exchange_symbol(ex_symbol)
        d = msg[1]
        book_event = BookTicker(
            symbol=symbol,
            bid_price=float(d[0]),
            bid_qty=float(d[1]),
            ask_price=float(d[2]),
            ask_qty=float(d[3]),
            utc=utc_now_float(),
            last_update_id=f'{ex_symbol}{utc_now_float()}'
        )
        self._cb.handle(book=book_event)

    def __handle_order_book(self, msg):
        ex_symbol = self.__book_ids[msg[0]]
        cur_depth = self.__book_snapshot.get(ex_symbol)
        if not cur_depth:
            cur_depth = Depth()
            self.__book_snapshot[ex_symbol] = cur_depth
            d = msg[1]
        else:
            d = [msg[1]]

        symbol = self.__from_exchange_symbol(ex_symbol)

        for new_book_row in d:
            price, new_count, new_amount = float(new_book_row[0]), float(new_book_row[1]), float(new_book_row[2])
            if new_count > 0:
                if new_amount > 0:
                    cur_depth.bids[price] = new_amount
                    if len(cur_depth.bids) > 20:
                        cur_depth.bids.popitem(0)
                else:
                    cur_depth.asks[price] = -new_amount
                    if len(cur_depth.asks) > 20:
                        cur_depth.asks.popitem(-1)
            elif new_count == 0:
                if new_amount == 1:
                    if price in cur_depth.bids:
                        del cur_depth.bids[price]
                elif new_amount == -1:
                    if price in cur_depth.asks:
                        del cur_depth.asks[price]

        if not self._throttler.may_pass(ex_symbol, tag='depth'):
            return

        book_depth = BookDepth(
            symbol=symbol,
            bids=[BidAsk(price=item[0], quantity=item[1]) for item in cur_depth.bids.items()],
            asks=[BidAsk(price=item[0], quantity=item[1]) for item in cur_depth.asks.items()],
            exchange_symbol=ex_symbol,
            utc=utc_now_float(),
            last_update_id=f'{ex_symbol}{utc_now_float()}'
        )
        self._cb.handle(depth=book_depth)
