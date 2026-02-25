import hashlib
import hmac
import json
import re
from datetime import datetime, timezone
from typing import List, Optional, Dict

from settings.base import cfg as app_cfg

import requests

from core.defs import CurrencyPair
from core.utils import utc_now_float
from core.connectors import BaseExchangeConnector, Ticker, BookTicker, BidAsk, BookDepth, CandleStick, WithdrawInfo

from .utils import CustomExchangeConnector

MEXC_API_HOST = "https://api.mexc.com"
MEXC_API_PREFIX = "/api/v3"
MEXC_API_HEADERS = {'Content-Type': 'application/json'}
MEXC_WS_URL = "wss://wbs.mexc.com/ws"


class MexcConnector(CustomExchangeConnector):

    def __init__(self, is_testing: bool = False):
        super().__init__(is_testing)

    @classmethod
    def exchange_id(cls) -> str:
        return 'mexc'

    @classmethod
    def ws_url(cls) -> str:
        return MEXC_WS_URL

    def start(self, cb: BaseExchangeConnector.Callback, symbols: List[str] = None, depth: bool = True):
        super().start(cb, symbols)
        for ex_sym in self._filtering_symbols:
            self._ws.send(json.dumps({
                "method": "SUBSCRIPTION",
                "params": [
                    f"spot@public.bookTicker.v3.api@{ex_sym}"
                ]
            }))
            if depth:
                self._ws.send(json.dumps({
                    "method": "SUBSCRIPTION",
                    "params": [
                        f"spot@public.limit.depth.v3.api@{ex_sym}@10"
                    ]
                }))

    def _fetch_tickers_from_server(self) -> List[Ticker]:
        url = '/ticker/bookTicker'
        spot_pairs_js_list = requests.request('GET', MEXC_API_HOST + MEXC_API_PREFIX + url,
                                              headers=MEXC_API_HEADERS).json()

        res = []
        for d in spot_pairs_js_list:
            t = self.__from_mexc_api_ticker(d)
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
        tickers_list_js = requests.request('GET', MEXC_API_HOST + MEXC_API_PREFIX + url, headers=MEXC_API_HEADERS).json()
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
        exchange_symbol = pair_code.replace('/', '')
        query_param = f"symbol={exchange_symbol}"
        url = '/ticker/price'
        response = requests.request('GET', MEXC_API_HOST + MEXC_API_PREFIX + url + "?" + query_param,
                                           headers=MEXC_API_HEADERS)
        if response.status_code != 200:
            return None

        ticker_js = response.json()
        b_q = self.__from_exchange_symbol(ticker_js["symbol"])
        if not b_q:
            return None
        base, quite = b_q.split('/')
        return CurrencyPair(
            base=base,
            quota=quite,
            ratio=float(ticker_js["price"]),
            utc=utc_now_float()
            )

    def get_depth(self, symbol: str, limit: int = 100) -> Optional[BookDepth]:
        exchange_symbol = symbol.replace('/', '')
        query_param = f"symbol={exchange_symbol}&limit={limit}"
        url = '/depth'
        order_book_js = requests.request('GET', MEXC_API_HOST + MEXC_API_PREFIX + url + "?" + query_param,
                                     headers=MEXC_API_HEADERS).json()
        return BookDepth(
            symbol=symbol,
            bids=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in order_book_js["bids"]],
            asks=[BidAsk(price=float(row[0]), quantity=float(row[1])) for row in order_book_js["asks"]],
            exchange_symbol=exchange_symbol,
            last_update_id=order_book_js["lastUpdateId"],
            utc=utc_now_float()
        )

    def get_klines(self, symbol: str) -> Optional[List[CandleStick]]:
        exchange_symbol = symbol.replace('/', '')
        query_param = f"symbol={exchange_symbol}&interval=1m&limit=60"
        url = '/klines'
        klines_array_js = requests.request('GET', MEXC_API_HOST + MEXC_API_PREFIX + url + "?" + query_param,
                                         headers=MEXC_API_HEADERS).json()
        result = []
        for kline in klines_array_js:
            result += [
                CandleStick(
                    utc_open_time=float(kline[0]),
                    open_price=float(kline[1]),
                    high_price=float(kline[2]),
                    low_price=float(kline[3]),
                    close_price=float(kline[4]),
                    coin_volume=float(kline[5])
                )
            ]

        return result[::-1]

    def get_withdraw_info(self) -> Optional[Dict[str, List[WithdrawInfo]]]:
        secret = app_cfg.cex.secrets[self.exchange_id()]
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json',
                   'X-MEXC-APIKEY': secret.api_key}

        ts = round(datetime.now(timezone.utc).timestamp() * 1000)
        query_param_no_sign = f'timestamp={ts}'
        signature = hmac.new(
            bytes(secret.secret_key, 'latin-1'),
            msg=bytes(query_param_no_sign, 'latin-1'),
            digestmod=hashlib.sha256
        ).hexdigest().lower()

        query_param_sign = query_param_no_sign + f'&signature={signature}'
        r = requests.request('GET', MEXC_API_HOST + MEXC_API_PREFIX + "/capital/config/getall" + '?' + query_param_sign,
                             headers=headers)

        js = r.json()
        res = {}
        for coin_js in js:
            coin = coin_js['coin'].upper()
            for nl_js in coin_js['networkList']:
                matches = re.match(r'^(.*?)\s*\((.*?)\)\s*$', nl_js['network'])
                if matches:
                    network_names = [matches.group(1), matches.group(2)]
                else:
                    network_names = [nl_js['network']]
                w_info = WithdrawInfo(
                    ex_code=self.exchange_id(),
                    coin=coin,
                    network_names=network_names,
                    withdraw_enabled=nl_js['withdrawEnable'],
                    deposit_enabled=nl_js['depositEnable'],
                    fixed_withdraw_fee=nl_js['withdrawFee'],
                    withdraw_min=nl_js['withdrawMin'],
                    withdraw_max=nl_js['withdrawMax']
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
    def __from_mexc_api_ticker(cls, d) -> Optional[Ticker]:
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
        if "c" in msg:
            if msg["c"].startswith("spot@public.bookTicker.v3.api@"):
                self.__handle_book_ticker(msg)
            elif msg["c"].startswith("spot@public.limit.depth.v3.api@"):
                self.__handle_order_book(msg)

    def __handle_book_ticker(self, msg):
        b_q = self.__from_exchange_symbol(msg["s"])
        if not b_q:
            return
        symbol = b_q
        if self._throttler.may_pass(symbol, tag='book'):
            book_event = BookTicker(
                symbol=symbol,
                bid_price=float(msg["d"]["b"]),
                bid_qty=float(msg["d"]["B"]),
                ask_price=float(msg["d"]["a"]),
                ask_qty=float(msg["d"]["A"]),
                utc=float(msg["t"])/1000,
                last_update_id=msg["t"]
            )
            self._cb.handle(book=book_event)

    def __handle_order_book(self, msg):
        b_q = self.__from_exchange_symbol(msg["s"])
        if not b_q:
            return
        symbol = b_q
        if self._throttler.may_pass(symbol, tag='depth'):
            book_depth = BookDepth(
                symbol=symbol,
                bids=[BidAsk(price=float(row["p"]), quantity=float(row["v"])) for row in msg["d"]["bids"]],
                asks=[BidAsk(price=float(row["p"]), quantity=float(row["v"])) for row in msg["d"]["asks"]],
                exchange_symbol=msg["s"],
                last_update_id=msg["d"]["r"],
                utc=float(msg["t"])/1000
            )
            self._cb.handle(depth=book_depth)
