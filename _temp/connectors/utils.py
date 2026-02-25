import json
import threading
from abc import abstractmethod
from typing import Any, Union, Tuple, Iterable, List, Optional, Dict
from urllib.parse import urlparse
import websocket
import time

import redis
from django.conf import settings

from core.connectors import Ticker, BaseExchangeConnector, WithdrawInfo


def redis_get(key: str) -> Any:
    parsed = urlparse(settings.REDIS_DSN)
    kwargs = {'host': parsed.hostname, 'port': parsed.port or 6379}
    if parsed.username or parsed.password:
        kwargs['username'] = parsed.username
        kwargs['password'] = parsed.password
    r = redis.Redis(**kwargs)
    s = r.get(key)
    return json.loads(s) if s else None


def redis_set(key: str, value: Union[list, dict], ttl: int = None):
    parsed = urlparse(settings.REDIS_DSN)
    kwargs = {'host': parsed.hostname, 'port': parsed.port or 6379}
    if parsed.username or parsed.password:
        kwargs['username'] = parsed.username
        kwargs['password'] = parsed.password
    r = redis.Redis(**kwargs)
    r.set(key, json.dumps(value), ex=ttl)


def redis_get_tuple(key: str, sz: int) -> Tuple:
    if sz <= 0:
        return tuple()
    parsed = urlparse(settings.REDIS_DSN)
    r = redis.Redis(host=parsed.hostname, port=parsed.port or 6379)
    s = r.get(key)
    js = json.loads(s) if s else None
    if isinstance(js, Iterable):
        values = tuple(i for i in js)[:sz]
    else:
        values = (js,)
    return values + tuple([None] * (sz-len(values)))


def build_tickers_dict(tickers: List[Ticker]):
    return {**{t.symbol: t for t in tickers}, **{t.exchange_symbol: t for t in tickers}}

def get_possible_chains(coin: str, wd_from: Dict[str, List[WithdrawInfo]], wd_to: Dict[str, List[WithdrawInfo]]) -> List[Union[WithdrawInfo, WithdrawInfo]]:
    coin = coin.upper()
    infos_from = wd_from.get(coin)
    infos_to = wd_to.get(coin)
    if infos_from is None or infos_to is None:
        return []

    res = []
    for info_from in infos_from:
        if not info_from.withdraw_enabled:
            continue
        network_names_from = {name.upper() for name in info_from.network_names}
        for info_to in infos_to:
            if not info_to.deposit_enabled:
                continue
            network_names_to = {name.upper() for name in info_to.network_names}
            if len(network_names_from.intersection(network_names_to)) > 0:
                res += [(info_from, info_to)]

    return res


class CustomExchangeConnector(BaseExchangeConnector):

    def __init__(self, is_testing: bool = False):
        super().__init__(is_testing)
        self._cached_tickers: Optional[List[Ticker]] = None
        self._cached_tickers_dict: Dict[str, Ticker] = {}
        self._cb: Optional[BaseExchangeConnector.Callback] = None
        self.__redis_cache_key = f'connector/{self.exchange_id()}/cached'
        self._ws = None
        self.__ws_thread = None
        self._filtering_symbols = []

    @classmethod
    @abstractmethod
    def ws_url(cls) -> str:
        raise NotImplemented

    def start(self, cb: BaseExchangeConnector.Callback, symbols: List[str] = None, depth: bool = True):
        if self._ws is not None:
            raise RuntimeError('Websocket Stream API already active. Close resources at first!')

        self._cb = cb
        self._load_tickers(symbols)
        self._connect_to_ws()

    def _load_tickers(self, symbols):
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        if symbols is None:
            self._filtering_symbols = [sym.exchange_symbol for sym in self._cached_tickers]
        else:
            self._filtering_symbols = [sym.exchange_symbol for sym in self._cached_tickers if sym.symbol in symbols]

    @abstractmethod
    def _connect_to_ws(self):
        #websocket.enableTrace(True)
        self._ws = websocket.WebSocketApp(self.ws_url(), on_message=self._handle_ws_message)
        self.__ws_thread = threading.Thread(target=self._ws.run_forever)
        self.__ws_thread.daemon = True
        self.__ws_thread.start()
        create_sock_timeout = 10
        while not self._ws.sock and create_sock_timeout:
            time.sleep(1)
            create_sock_timeout -= 1
        if not self._ws.sock:
            raise Exception("WS connection failed")
        conn_timeout = 10
        while not self._ws.sock.connected and conn_timeout:
            time.sleep(1)
            conn_timeout -= 1
        if not self._ws.sock.connected:
            raise Exception("WS connection timeout")

    def stop(self):
        self._ws.close()
        self.__ws_thread = None
        self._ws = None

    @abstractmethod
    def _fetch_tickers_from_server(self) -> List[Ticker]:
        raise NotImplemented

    def get_all_tickers(self) -> List[Ticker]:
        if self._cached_tickers:
            return self._cached_tickers
        cached_tickers = redis_get(self.__redis_cache_key)
        if cached_tickers:
            self._cached_tickers = [Ticker.from_json(json.dumps(tk)) for tk in cached_tickers]
            self._cached_tickers_dict = build_tickers_dict(self._cached_tickers)
            return self._cached_tickers

        tickers = self._fetch_tickers_from_server()

        self._cached_tickers = tickers
        self._cached_tickers_dict = build_tickers_dict(tickers)
        redis_set(
            self.__redis_cache_key,
            [tk.as_json() for tk in tickers],
            ttl=settings.EXCHANGE_CONNECTOR_CACHE_TIMEOUT
        )
        return tickers

    @abstractmethod
    def _handle_ws_message(self, _, msg):
        raise NotImplemented
