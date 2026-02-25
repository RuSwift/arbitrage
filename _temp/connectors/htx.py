import json
from typing import List, Optional, Dict
import gzip
import requests
from datetime import datetime, timezone, timedelta

from core.defs import CurrencyPair
from core.connectors import BaseExchangeConnector, Ticker, BookTicker, BidAsk, BookDepth, CandleStick, WithdrawInfo

from .utils import CustomExchangeConnector

HTX_API_HOST = "https://api.huobi.pro"
HTX_API_PREFIX = ""
HTX_API_HEADERS = {'Content-Type': 'application/json'}
HTX_WS_URL = "wss://api.huobi.pro/ws/"


class HtxConnector(CustomExchangeConnector):

    def __init__(self, is_testing: bool = False):
        super().__init__(is_testing)

    @classmethod
    def exchange_id(cls) -> str:
        return 'htx'

    @classmethod
    def ws_url(cls) -> str:
        return HTX_WS_URL

    def start(self, cb: BaseExchangeConnector.Callback, symbols: List[str] = None, depth: bool = True):
        super().start(cb, symbols)
        for ex_sym in self._filtering_symbols:
            self._ws.send(json.dumps({
                "sub": f"market.{ex_sym.lower()}.bbo",
                "id": f"id_bbo_{ex_sym}"
            }))

            if depth:
                self._ws.send(json.dumps({
                    "sub": f"market.{ex_sym.lower()}.depth.step0",
                    "id": f"id_depth_{ex_sym}"
                }))

    def _fetch_tickers_from_server(self) -> List[Ticker]:
        url = '/market/tickers'
        spot_pairs_js_list = requests.request('GET', HTX_API_HOST + HTX_API_PREFIX + url,
                                              headers=HTX_API_HEADERS).json()["data"]

        res = []
        for d in spot_pairs_js_list:
            t = self.__from_htx_api_ticker(d)
            if t:
                res += [t]
        return res

    def get_pairs(self, symbols: List[str] = None) -> List[CurrencyPair]:
        if symbols is None:
            if not self._cached_tickers_dict:
                self.get_all_tickers()
            filtering_symbols = set({ticker.exchange_symbol for ticker in self._cached_tickers})
        else:
            filtering_symbols = set([sym.replace('/', '_') for sym in symbols])

        url = '/market/tickers'
        tickers_js = requests.request('GET', HTX_API_HOST + HTX_API_PREFIX + url, headers=HTX_API_HEADERS).json()
        pairs = []
        for js_ticker in tickers_js["data"]:
            if js_ticker["symbol"] in filtering_symbols:
                base, quite = self.__from_exchange_symbol(js_ticker["symbol"]).split('/')
                pairs.append(
                    CurrencyPair(
                        base=base,
                        quota=quite,
                        ratio=(float(js_ticker["bid"])+float(js_ticker["ask"]))/2,
                        utc=float(tickers_js["ts"])/1000
                    )
                )
        return pairs

    def get_price(self, pair_code: str) -> Optional[CurrencyPair]:
        exchange_symbol = pair_code.replace('/', '').lower()
        query_param = f"symbol={exchange_symbol}"
        url = '/market/trade'
        response = requests.request('GET', HTX_API_HOST + HTX_API_PREFIX + url + "?" + query_param,
                                           headers=HTX_API_HEADERS)
        if response.status_code != 200:
            return None

        ticker_js = response.json()
        if ticker_js["status"] != "ok":
            return None

        base, quite = pair_code.split('/')
        return CurrencyPair(
            base=base,
            quota=quite,
            ratio=float(ticker_js["tick"]["data"][0]["price"]),
            utc=self.__to_utc_timestamp(float(ticker_js["ts"])/1000)
            )

    def get_depth(self, symbol: str, limit: int = 100) -> Optional[BookDepth]:
        exchange_symbol = symbol.replace('/', '').lower()
        query_param = f"symbol={exchange_symbol}&depth={10}&type=step0"
        url = '/market/depth'
        order_book_js = requests.request('GET', HTX_API_HOST + HTX_API_PREFIX + url + "?" + query_param,
                                     headers=HTX_API_HEADERS).json()["tick"]
        return BookDepth(
            symbol=symbol,
            bids=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in order_book_js["bids"]],
            asks=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in order_book_js["asks"]],
            exchange_symbol=exchange_symbol,
            last_update_id=int(order_book_js["ts"]),
            utc=self.__to_utc_timestamp(float(order_book_js["ts"])/1000)
        )

    def get_klines(self, symbol: str) -> Optional[List[CandleStick]]:
        exchange_symbol = symbol.replace('/', '').lower()
        query_param = f"symbol={exchange_symbol}&period=1min&size=60"
        url = '/market/history/kline'
        klines_array_js = requests.request('GET', HTX_API_HOST + HTX_API_PREFIX + url + "?" + query_param,
                                         headers=HTX_API_HEADERS).json()["data"]
        result = []
        for kline in klines_array_js:
            result += [
                CandleStick(
                    utc_open_time=int(kline["id"]),
                    open_price=float(kline["open"]),
                    high_price=float(kline["high"]),
                    low_price=float(kline["low"]),
                    close_price=float(kline["close"]),
                    coin_volume=float(kline["amount"])
                )
            ]

        return result

    def get_withdraw_info(self) -> Optional[Dict[str, List[WithdrawInfo]]]:
        url = "/reference/currencies"
        r = requests.request('GET', HTX_API_HOST + '/v2' + url,
                             headers=HTX_API_HEADERS)

        js_arr = r.json()['data']
        res = {}
        for coin_js in js_arr:
            coin = coin_js['currency'].upper()
            for chain_js in coin_js['chains']:
                network_names = [chain_js['chain'], chain_js['displayName']]
                w_info = WithdrawInfo(
                    ex_code=self.exchange_id(),
                    coin=coin,
                    network_names=network_names,
                    withdraw_enabled=chain_js['withdrawStatus'] == 'allowed',
                    deposit_enabled=chain_js['depositStatus'] == 'allowed',
                    fixed_withdraw_fee=float(chain_js['transactFeeWithdraw']) if 'transactFeeWithdraw' in chain_js else None,
                    withdraw_min=float(chain_js['minWithdrawAmt']),
                    withdraw_max=float(chain_js['maxWithdrawAmt'])
                )
                if coin not in res:
                    res[coin] = [w_info]
                else:
                    res[coin].append(w_info)

        return res

    @staticmethod
    def __from_exchange_symbol(exchange_symbol: str) -> Optional[str]:
        exchange_symbol = exchange_symbol.upper()
        quotes = ["USDT", "BTC", "ETH", "USDC", "TUSD", "HT"]
        for q in quotes:
            if exchange_symbol.endswith(q):
                return f"{exchange_symbol[0:-len(q)]}/{q}"
        return None

    @classmethod
    def __from_htx_api_ticker(cls, d) -> Optional[Ticker]:
        b_q = cls.__from_exchange_symbol(d["symbol"])
        if not b_q:
            return None
        base, quite = b_q.split('/')
        return Ticker(
            symbol=CurrencyPair.build_code(base=base.upper(), quota=quite.upper()),
            base=base.upper(),
            quote=quite.upper(),
            is_spot_enabled=True,
            is_margin_enabled=False,
            exchange_symbol=d["symbol"]
        )

    def _handle_ws_message(self, _, msg):
        msg = gzip.decompress(msg)
        msg = json.loads(msg)
        if "ch" in msg:
            if msg["ch"].startswith("market.") and msg["ch"].endswith(".bbo"):
                self.__handle_book_ticker(msg)
            elif msg["ch"].startswith("market.") and msg["ch"].endswith(".depth.step0"):
                self.__handle_order_book(msg)
        elif "ping" in msg:
            self.__handle_ping(msg)

    def __handle_book_ticker(self, msg):
        symbol = self.__from_exchange_symbol(msg["tick"]["symbol"])
        if self._throttler.may_pass(symbol, tag='book'):
            book_event = BookTicker(
                symbol=symbol,
                bid_price=float(msg["tick"]["bid"]),
                bid_qty=float(msg["tick"]["bidSize"]),
                ask_price=float(msg["tick"]["ask"]),
                ask_qty=float(msg["tick"]["askSize"]),
                utc=float(msg["tick"]["quoteTime"])/1000,
                last_update_id=int(msg["tick"]["seqId"])
            )
            self._cb.handle(book=book_event)

    def __handle_order_book(self, msg):
        symbol = self.__from_exchange_symbol(msg["ch"].split('.')[1])
        if self._throttler.may_pass(symbol, tag='depth'):
            book_depth = BookDepth(
                symbol=symbol,
                bids=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in msg["tick"]["bids"]],
                asks=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in msg["tick"]["asks"]],
                exchange_symbol=msg["ch"].split('.')[1],
                last_update_id=int(msg["tick"]["ts"]),
                utc=float(msg["tick"]["ts"])/1000
            )
            self._cb.handle(depth=book_depth)

    def __handle_ping(self, msg):
        ping_id = msg["ping"]
        self._ws.send(json.dumps({
            "pong": ping_id
        }))

    @staticmethod
    def __to_utc_timestamp(timestamp: float) -> float:
        singapore_timestamp = timestamp
        singapore_time = datetime.utcfromtimestamp(singapore_timestamp)

        singapore_timezone = timezone(timedelta(hours=8))
        singapore_time = singapore_time.replace(tzinfo=singapore_timezone)

        utc_time = singapore_time.astimezone(timezone.utc)
        utc_timestamp = utc_time.timestamp()
        return utc_timestamp
