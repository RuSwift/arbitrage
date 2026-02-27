"""Base CEX connectors: spot and perpetual (no P2P)."""

from __future__ import annotations

from abc import ABC, abstractmethod

from app.cex.dto import (
    BookDepth,
    BookTicker,
    CandleStick,
    CurrencyPair,
    FundingRate,
    FundingRatePoint,
    PerpetualTicker,
    Ticker,
    WithdrawInfo,
)
from app.cex.throttler import Throttler
from app.settings import Settings

DEFAULT_THROTTLE_TIMEOUT = 1.0
DEFAULT_DEPTH_LIMIT = 100  # default get_depth(limit=...) when not specified
DEFAULT_KLINE_LIMIT = 60  # default get_klines(limit=...) when not specified
DEFAULT_FUNDING_HISTORY_LIMIT = 100  # default get_funding_rate_history(limit=...) when not specified


class Callback(ABC):
    """Protocol for real-time book/depth updates from connectors."""

    @abstractmethod
    def handle(
        self,
        book: BookTicker | None = None,
        depth: BookDepth | None = None,
    ) -> None:
        ...


class _BaseCEXConnectorMixin:
    """Shared state and Throttler for spot and perpetual connectors."""

    def __init__(
        self,
        is_testing: bool = False,
        throttle_timeout: float = DEFAULT_THROTTLE_TIMEOUT,
    ) -> None:
        self._is_testing = is_testing
        redis_url = Settings().redis.url
        key_prefix = f"arbitrage:throttle:{self.__class__.__name__}"
        self._throttler = Throttler(
            timeout=throttle_timeout, redis_url=redis_url, key_prefix=key_prefix
        )


class BaseCEXSpotConnector(_BaseCEXConnectorMixin, ABC):
    """Base connector for spot market: tickers, prices, orderbook, klines."""

    @classmethod
    @abstractmethod
    def exchange_id(cls) -> str:
        ...

    @abstractmethod
    def start(
        self,
        cb: Callback,
        symbols: list[str] | None = None,
        depth: bool = True,
    ) -> None:
        ...

    @abstractmethod
    def stop(self) -> None:
        ...

    @abstractmethod
    def get_all_tickers(self) -> list[Ticker]:
        ...

    @abstractmethod
    def get_price(self, pair_code: str) -> CurrencyPair | None:
        ...

    @abstractmethod
    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        ...

    @abstractmethod
    def get_depth(self, symbol: str, limit: int = DEFAULT_DEPTH_LIMIT) -> BookDepth | None:
        ...

    @abstractmethod
    def get_klines(self, symbol: str, limit: int | None = None) -> list[CandleStick] | None:
        ...

    def get_withdraw_info(self) -> dict[str, list[WithdrawInfo]] | None:
        return None


class BaseCEXPerpetualConnector(_BaseCEXConnectorMixin, ABC):
    """Base connector for perpetual (linear) futures: perpetual list, prices, orderbook, klines."""

    @classmethod
    @abstractmethod
    def exchange_id(cls) -> str:
        ...

    @abstractmethod
    def start(
        self,
        cb: Callback,
        symbols: list[str] | None = None,
        depth: bool = True,
    ) -> None:
        ...

    @abstractmethod
    def stop(self) -> None:
        ...

    @abstractmethod
    def get_all_perpetuals(self) -> list[PerpetualTicker]:
        ...

    @abstractmethod
    def get_price(self, symbol: str) -> CurrencyPair | None:
        ...

    @abstractmethod
    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        ...

    @abstractmethod
    def get_depth(self, symbol: str, limit: int = DEFAULT_DEPTH_LIMIT) -> BookDepth | None:
        ...

    @abstractmethod
    def get_klines(self, symbol: str, limit: int | None = None) -> list[CandleStick] | None:
        ...

    @abstractmethod
    def get_funding_rate(self, symbol: str) -> FundingRate | None:
        ...

    @abstractmethod
    def get_funding_rate_history(
        self, symbol: str, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        ...
