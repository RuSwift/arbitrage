from abc import ABC, abstractmethod
from hashlib import md5
from dataclasses import dataclass
from typing import List, Any, Optional, Type, Dict

from django.conf import settings

from core.throttler import Throttler
from core.defs import CurrencyPair, JsonSerializable
from .p2p import P2PService


@dataclass
class Ticker(JsonSerializable):
    symbol: str
    base: str
    quote: str
    is_spot_enabled: bool
    is_margin_enabled: bool
    exchange_symbol: str = None


@dataclass
class BookTicker(JsonSerializable):
    symbol: str
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    last_update_id: Any = None
    utc: Optional[float] = None


@dataclass
class BidAsk(JsonSerializable):
    price: float
    quantity: float


@dataclass
class BookDepth(JsonSerializable):
    symbol: str
    bids: List[BidAsk]
    asks: List[BidAsk]
    exchange_symbol: str = None
    last_update_id: Any = None
    utc: float = None


@dataclass
class CandleStick(JsonSerializable):
    utc_open_time: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    coin_volume: float
    usd_volume: Optional[float] = None


class ExCredentials(JsonSerializable):
    api_key: str
    secret_key: str
    pass_phrase: Optional[str] = None

    @property
    def md5(self) -> str:
        s = self.api_key + self.secret_key + (self.pass_phrase or '')
        return md5(s.encode()).hexdigest()


@dataclass
class WithdrawInfo(JsonSerializable):
    ex_code: str
    coin: str
    network_names: List[str]
    withdraw_enabled: bool
    deposit_enabled: bool
    fixed_withdraw_fee: Optional[float] = None
    withdraw_min: Optional[float] = None
    withdraw_max: Optional[float] = None


class BaseExchangeConnector:

    class Callback(ABC):
        @abstractmethod
        def handle(self, book: BookTicker = None, depth: BookDepth = None):
            raise NotImplemented

    def __init__(self, is_testing: bool = False):
        self._is_testing = is_testing
        self._throttler = Throttler(timeout=settings.EXCHANGE_CONNECTOR_THROTTLE_TIMEOUT)

    @abstractmethod
    def start(self, cb: Callback, symbols: List[str] = None, depth: bool = True):
        raise NotImplemented

    @abstractmethod
    def stop(self):
        raise NotImplemented

    @classmethod
    @abstractmethod
    def exchange_id(cls) -> str:
        raise NotImplemented

    @abstractmethod
    def get_all_tickers(self) -> List[Ticker]:
        raise NotImplemented

    @abstractmethod
    def get_price(self, pair_code: str) -> Optional[CurrencyPair]:
        raise NotImplemented

    @abstractmethod
    def get_pairs(self, symbols: List[str] = None) -> List[CurrencyPair]:
        raise NotImplemented

    @abstractmethod
    def get_depth(self, symbol: str, limit: int = 100) -> Optional[BookDepth]:
        raise NotImplemented

    @abstractmethod
    def get_klines(self, symbol: str) -> Optional[List[CandleStick]]:
        raise NotImplemented

    @abstractmethod
    def get_withdraw_info(self) -> Optional[Dict[str, List[WithdrawInfo]]]:
        return None

    @classmethod
    def p2p(cls) -> Optional[Type[P2PService]]:
        return None
