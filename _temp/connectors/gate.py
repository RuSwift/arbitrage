import json
import time
from typing import List, Optional, Dict

import requests

from core.defs import CurrencyPair
from core.utils import utc_now_float
from core.connectors import BaseExchangeConnector, Ticker, BookTicker, BidAsk, BookDepth, CandleStick, WithdrawInfo

from .utils import CustomExchangeConnector

GATE_API_HOST = "https://api.gateio.ws"
GATE_API_PREFIX = "/api/v4"
GATE_API_HEADERS = {'Accept': 'application/json', 'Content-Type': 'application/json'}
GATE_WS_URL = "wss://api.gateio.ws/ws/v4/"


class GateConnector(CustomExchangeConnector):

    def __init__(self, is_testing: bool = False):
        super().__init__(is_testing)

    @classmethod
    def exchange_id(cls) -> str:
        return 'gate'

    @classmethod
    def ws_url(cls) -> str:
        return GATE_WS_URL

    def start(self, cb: BaseExchangeConnector.Callback, symbols: List[str] = None, depth: bool = True):
        super().start(cb, symbols)
        self._ws.send(json.dumps({
            "time": int(time.time()),
            "channel": "spot.book_ticker",
            "event": "subscribe",
            "payload": self._filtering_symbols
        }))

        if depth:
            self._ws.send(json.dumps({
                "time": int(time.time()),
                "channel": "spot.order_book",
                "event": "subscribe",
                "payload": [[sym, "10", "1000ms"] for sym in self._filtering_symbols]
            }))

    def _fetch_tickers_from_server(self) -> List[Ticker]:
        url = '/spot/currency_pairs'
        spot_pairs_js_list = requests.request('GET', GATE_API_HOST + GATE_API_PREFIX + url,
                                              headers=GATE_API_HEADERS).json()

        return [self.__from_gate_api_ticker(d) for d in spot_pairs_js_list]

    def get_pairs(self, symbols: List[str] = None) -> List[CurrencyPair]:
        if symbols is None:
            if not self._cached_tickers_dict:
                self.get_all_tickers()
            filtering_symbols = set({ticker.exchange_symbol for ticker in self._cached_tickers})
        else:
            filtering_symbols = set([sym.replace('/', '_') for sym in symbols])

        url = '/spot/tickers'
        tickers_list_js = requests.request('GET', GATE_API_HOST + GATE_API_PREFIX + url, headers=GATE_API_HEADERS).json()
        pairs = []
        for js_ticker in tickers_list_js:
            if js_ticker["currency_pair"] in filtering_symbols:
                base, quite = js_ticker["currency_pair"].split('_')
                pairs.append(
                    CurrencyPair(
                        base=base,
                        quota=quite,
                        ratio=float(js_ticker["last"]),
                        utc=utc_now_float()
                    )
                )
        return pairs

    def get_price(self, pair_code: str) -> Optional[CurrencyPair]:
        exchange_symbol = pair_code.replace('/', '_')
        query_param = f"currency_pair={exchange_symbol}"
        url = '/spot/tickers'
        response = requests.request('GET', GATE_API_HOST + GATE_API_PREFIX + url + "?" + query_param,
                                           headers=GATE_API_HEADERS)
        if response.status_code != 200:
            return None

        ticker_js = response.json()[0]
        base, quite = ticker_js["currency_pair"].split('_')
        return CurrencyPair(
            base=base,
            quota=quite,
            ratio=float(ticker_js["last"]),
            utc=utc_now_float()
            )

    def get_depth(self, symbol: str, limit: int = 100) -> Optional[BookDepth]:
        exchange_symbol = symbol.replace('/', '_')
        query_param = f"currency_pair={exchange_symbol}&limit={limit}&with_id=true"
        url = '/spot/order_book'
        order_book_js = requests.request('GET', GATE_API_HOST + GATE_API_PREFIX + url + "?" + query_param,
                                     headers=GATE_API_HEADERS).json()
        return BookDepth(
            symbol=symbol,
            bids=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in order_book_js["bids"]],
            asks=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in order_book_js["asks"]],
            exchange_symbol=exchange_symbol,
            last_update_id=order_book_js["id"],
            utc=float(order_book_js["update"])/1000
        )

    def get_klines(self, symbol: str) -> Optional[List[CandleStick]]:
        exchange_symbol = symbol.replace('/', '_')
        query_param = f"currency_pair={exchange_symbol}&interval=1m&limit=60"
        url = '/spot/candlesticks'
        klines_array_js = requests.request('GET', GATE_API_HOST + GATE_API_PREFIX + url + "?" + query_param,
                                         headers=GATE_API_HEADERS).json()
        result = []
        for kline in klines_array_js:
            if len(kline) >= 7 and kline[7]:
                result += [
                    CandleStick(
                        utc_open_time=float(kline[0]),
                        open_price=float(kline[5]),
                        high_price=float(kline[3]),
                        low_price=float(kline[4]),
                        close_price=float(kline[2]),
                        coin_volume=float(kline[6])
                    )
                ]

        return result[::-1]

    def get_withdraw_info(self) -> Optional[Dict[str, List[WithdrawInfo]]]:
        url = "/spot/currencies"
        r = requests.request('GET', GATE_API_HOST + GATE_API_PREFIX + url,
                             headers=GATE_API_HEADERS)

        js_arr = r.json()
        res = {}
        for coin_js in js_arr:
            coin = coin_js['currency'].upper()
            if '_' in coin:
                idx = coin.index('_')
                coin = coin[0:idx]
            w_info = WithdrawInfo(
                ex_code=self.exchange_id(),
                coin=coin,
                network_names=[coin_js['chain']],
                withdraw_enabled=not coin_js['withdraw_disabled'],
                deposit_enabled=not coin_js['deposit_disabled']
            )
            if coin not in res:
                res[coin] = [w_info]
            else:
                res[coin].append(w_info)

        return res

    @staticmethod
    def __from_gate_api_ticker(d) -> Ticker:
        base, quite = d['base'], d['quote']
        return Ticker(
            symbol=CurrencyPair.build_code(base=base.upper(), quota=quite.upper()),
            base=base.upper(),
            quote=quite.upper(),
            is_spot_enabled=True,
            is_margin_enabled=False,
            exchange_symbol=d['id']
        )

    def _handle_ws_message(self, _, msg):
        msg = json.loads(msg)
        if "channel" in msg:
            if msg["channel"] == "spot.book_ticker":
                self.__handle_book_ticker(msg)
            elif msg["channel"] == "spot.order_book":
                self.__handle_order_book(msg)

    def __handle_book_ticker(self, msg):
        result = msg["result"]
        symbol = result["s"].replace('_', '/')
        if self._throttler.may_pass(symbol, tag='book'):
            book_event = BookTicker(
                symbol=symbol,
                bid_price=float(result["b"]),
                bid_qty=float(result["B"]),
                ask_price=float(result["a"]),
                ask_qty=float(result["A"]),
                utc=float(msg["time_ms"])/1000,
                last_update_id=int(result["u"])
            )
            self._cb.handle(book=book_event)

    def __handle_order_book(self, msg):
        result = msg["result"]
        symbol = result["s"].replace('_', '/')
        if self._throttler.may_pass(symbol, tag='depth'):
            book_depth = BookDepth(
                symbol=symbol,
                bids=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in result["bids"]],
                asks=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in result["asks"]],
                exchange_symbol=result["s"],
                last_update_id=result["lastUpdateId"],
                utc=float(msg["time_ms"])/1000
            )
            self._cb.handle(depth=book_depth)
