import asyncio
import json
import threading
from typing import List, Optional, Dict

import requests
from django.conf import settings
from settings.base import cfg as app_cfg

from kucoin.client import Market
from kucoin.client import WsToken
from kucoin.ws_client import KucoinWsClient

from core.utils import utc_now_float
import core.throttler
from core.defs import CurrencyPair
from core.connectors import (
    BaseExchangeConnector, Ticker, BookTicker, BidAsk, BookDepth, CandleStick, WithdrawInfo
)

from .utils import redis_get, redis_set, build_tickers_dict
from .defs import PERPETUAL_TOKENS, KLINE_WINDOW_SECS


KUCOIN_API_HOST = "https://api.kucoin.com"
KUCOIN_API_PREFIX = "/api/v3"
KUCOIN_API_HEADERS = {'Content-Type': 'application/json'}

class LocalCache:

    def __init__(self, throttle_timeout: float):
        self.throttler = core.throttler.Throttler(throttle_timeout)


class KuCoinConnector(BaseExchangeConnector):

    _cache = LocalCache(throttle_timeout=10)

    def __init__(self, is_testing: bool = False):
        super().__init__(is_testing)
        self.__cached_tickers: Optional[List[Ticker]] = None
        self.__cached_tickers_dict: Dict[str, Ticker] = {}
        self.__client = Market()
        if self.exchange_id() in app_cfg.cex.secrets:
            secret = app_cfg.cex.secrets[self.exchange_id()]
            self.__private_client = Market(
                key=secret.api_key,
                secret=secret.secret_key,
                passphrase=secret.pass_phrase
            )
        else:
            self.__private_client = None
        self.__public_ws: Optional[KucoinWsClient] = None
        self.__loop: Optional[asyncio.BaseEventLoop] = None
        self.__thread = None
        self.__cb: Optional[BaseExchangeConnector.Callback] = None
        self.__redis_cache_key = f'connector/{self.exchange_id()}/cached'
        self._request_depth = False

    @classmethod
    def exchange_id(cls) -> str:
        return 'kucoin'

    def start(self, cb: BaseExchangeConnector.Callback, symbols: List[str] = None, depth: bool = True):
        self._request_depth = depth
        if self.__public_ws is not None:
            raise RuntimeError('Websocket Stream API already active. Close resources at first!')
        self.__loop = asyncio.new_event_loop()
        self.__thread = threading.Thread(target=self.__thread_target, args=(self.__loop,), daemon=True)
        self.__thread.start()
        self.__public_ws = asyncio.run_coroutine_threadsafe(KucoinWsClient.create(None, WsToken(), self.__handle_ws_message, private=False), self.__loop).result()
        if not self.__cached_tickers_dict:
            self.get_all_tickers()
        if symbols is None:
            filtering_symbols = [sym.exchange_symbol for sym in self.__cached_tickers]
        else:
            filtering_symbols = [sym.exchange_symbol for sym in self.__cached_tickers if sym.symbol in symbols]
        self.__cb = cb
        for sym in filtering_symbols:
            if self._request_depth:
                asyncio.run_coroutine_threadsafe(self.__public_ws.subscribe(f"/spotMarket/level2Depth50:{sym}"), self.__loop).result()
            else:
                asyncio.run_coroutine_threadsafe(self.__public_ws.subscribe(f"/spotMarket/level2Depth5:{sym}"),
                                                 self.__loop).result()

    def stop(self):
        self.__loop.stop()
        self.__public_ws = None
        self.__thread = None

    def get_all_tickers(self) -> List[Ticker]:
        if self.__cached_tickers:
            return self.__cached_tickers
        cached_tickers = redis_get(self.__redis_cache_key)
        if cached_tickers:
            self.__cached_tickers = [Ticker.from_json(json.dumps(tk)) for tk in cached_tickers]
            self.__cached_tickers_dict = build_tickers_dict(self.__cached_tickers)
            return self.__cached_tickers

        tickers_list_js = self.__client.get_all_tickers()["ticker"]
        tickers = [self.__from_kucoin_api_ticker(d) for d in tickers_list_js]

        self.__cached_tickers = tickers
        self.__cached_tickers_dict = build_tickers_dict(tickers)
        redis_set(
            self.__redis_cache_key,
            [tk.as_json() for tk in tickers],
            ttl=settings.EXCHANGE_CONNECTOR_CACHE_TIMEOUT
        )
        return tickers

    def get_price(self, pair_code: str) -> Optional[CurrencyPair]:
        exchange_symbol = pair_code.replace('/', '-')
        resp = self.__client.get_ticker(exchange_symbol)
        if 'price' not in resp:
            return None
        base, quote = pair_code.split('/')
        return CurrencyPair(
            base=base,
            quota=quote,
            ratio=float(resp["price"]),
            utc=float(resp["time"]*1E-3)
        )

    def get_pairs(self, symbols: List[str] = None) -> List[CurrencyPair]:
        if symbols is None:
            if not self.__cached_tickers_dict:
                self.get_all_tickers()
            filtering_symbols = set({ticker.exchange_symbol for ticker in self.__cached_tickers})
        else:
            filtering_symbols = set([sym.replace('/', '-') for sym in symbols])
        all_tickers = self.__client.get_all_tickers()
        tickers_list_js = all_tickers["ticker"]
        pairs = []
        for js_ticker in tickers_list_js:
            if js_ticker["symbol"] in filtering_symbols:
                base, quite = js_ticker["symbol"].split('-')
                pairs.append(
                    CurrencyPair(
                        base=base,
                        quota=quite,
                        ratio=float(js_ticker["last"]),
                        utc=float(all_tickers["time"]*1E-3)
                    )
                )
        return pairs

    def get_depth(self, symbol: str, limit: int = 100) -> Optional[BookDepth]:
        if not self.__private_client:
            raise RuntimeError('Configure private API')
        ex_sym = self.__get_exchange_symbol(symbol)
        if ex_sym:
            data = self.__private_client.get_aggregated_orderv3(symbol=ex_sym)
            depth = self.__deserialize_depth(
                symbol=symbol, ex_symbol=ex_sym, data=data
            )
            depth.asks = depth.asks[:limit]
            depth.bids = depth.bids[:limit]
            return depth
        else:
            return None

    def get_klines(self, symbol: str) -> Optional[List[CandleStick]]:
        ex_sym = self.__get_exchange_symbol(symbol)
        items = self.__client.get_kline(
            symbol=ex_sym, kline_type='1min',
            startAt=round(utc_now_float() - KLINE_WINDOW_SECS)
        )
        klines = [
            self.__deserialize_candle_stick(symbol, i) for i in items
        ]
        return klines

    def __get_exchange_symbol(self, symbol: str) -> Optional[str]:
        if not self.__cached_tickers_dict:
            self.get_all_tickers()
        ticker = self.__cached_tickers_dict.get(symbol)
        if ticker:
            return ticker.exchange_symbol
        else:
            return None

    def get_withdraw_info(self) -> Optional[Dict[str, List[WithdrawInfo]]]:
        url = "/currencies"
        r = requests.request('GET', KUCOIN_API_HOST + KUCOIN_API_PREFIX + url,
                             headers=KUCOIN_API_HEADERS)

        js_arr = r.json()['data']
        res = {}
        for coin_js in js_arr:
            coin = coin_js['currency'].upper()
            if not coin_js['chains']:
                continue
            for chain_js in coin_js['chains']:
                network_names = [chain_js['chainName']]
                w_info = WithdrawInfo(
                    ex_code=self.exchange_id(),
                    coin=coin,
                    network_names=network_names,
                    withdraw_enabled=chain_js['isWithdrawEnabled'],
                    deposit_enabled=chain_js['isDepositEnabled'],
                    withdraw_min=float(chain_js['withdrawalMinSize']),
                )
                if coin not in res:
                    res[coin] = [w_info]
                else:
                    res[coin].append(w_info)

        return res

    @staticmethod
    def __from_kucoin_api_ticker(d) -> Ticker:
        base, quite = d["symbol"].split('-')
        return Ticker(
            symbol=CurrencyPair.build_code(base=base.upper(), quota=quite.upper()),
            base=base.upper(),
            quote=quite.upper(),
            is_spot_enabled=True,
            is_margin_enabled=False,
            exchange_symbol=d['symbol']
        )

    async def __handle_ws_message(self, msg):
        _, ex_symbol = msg['topic'].split(':')
        symbol = ex_symbol.replace('-', '/')
        data = msg["data"]

        if self._throttler.may_pass(symbol, tag='book'):
            book_event = BookTicker(
                symbol=symbol,
                bid_price=float(data["bids"][0][0]),
                bid_qty=float(data["bids"][0][1]),
                ask_price=float(data["asks"][0][0]),
                ask_qty=float(data["asks"][0][1]),
                utc=float(data["timestamp"]*1E-3),
                last_update_id=int(data["timestamp"])
            )
        else:
            book_event = None

        if self._request_depth and self._throttler.may_pass(symbol, tag='depth'):
            depth_event = self.__deserialize_depth(
                symbol=symbol, ex_symbol=ex_symbol, data=data
            )
        else:
            depth_event = None

        if book_event or depth_event:
            self.__cb.handle(book=book_event, depth=depth_event)

    @classmethod
    def __deserialize_depth(cls, symbol: str, ex_symbol: str, data: Dict) -> BookDepth:
        timestamp = data.get('timestamp') or data.get('time')
        return BookDepth(
            symbol=symbol,
            bids=[BidAsk(price=float(a[0]), quantity=float(a[1])) for a in
                  data["bids"]],
            asks=[BidAsk(price=float(a[0]), quantity=float(a[1])) for a in
                  data["asks"]],
            exchange_symbol=ex_symbol,
            utc=float(timestamp * 1E-3),
            last_update_id=int(timestamp)
        )

    @staticmethod
    def __thread_target(loop: asyncio.AbstractEventLoop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

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
            utc_open_time=float(data[0]),
            open_price=float(data[1]),
            close_price=float(data[2]),
            high_price=float(data[3]),
            low_price=float(data[4]),
            coin_volume=float(data[5]),
            usd_volume=usd_volume
        )
