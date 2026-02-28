"""Base CEX connectors: spot and perpetual (no P2P)."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

import requests

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
from app.cex.rest_rate_limit import get_tracker, request_with_retry
from app.cex.throttler import Throttler
from app.settings import Settings

DEFAULT_THROTTLE_TIMEOUT = 1.0
DEFAULT_DEPTH_LIMIT = 100  # default get_depth(limit=...) when not specified
DEFAULT_KLINE_LIMIT = 60  # default get_klines(limit=...) when not specified
DEFAULT_FUNDING_HISTORY_LIMIT = 100  # default get_funding_rate_history(limit=...) when not specified


class Callback(ABC):
    """Protocol for real-time book/depth/kline updates from connectors.

    Connectors may call handle() with any subset of book, depth, kline;
    unsupported event types are simply not sent.
    """

    @abstractmethod
    def handle(
        self,
        book: BookTicker | None = None,
        depth: BookDepth | None = None,
        kline: CandleStick | None = None,
    ) -> None:
        ...


class _BaseCEXConnectorMixin:
    """Shared state and Throttler for spot and perpetual connectors."""

    def __init__(
        self,
        is_testing: bool = False,
        throttle_timeout: float = DEFAULT_THROTTLE_TIMEOUT,
        log: logging.Logger | None = None,
    ) -> None:
        self._is_testing = is_testing
        redis_url = Settings().redis.url
        key_prefix = f"arbitrage:throttle:{self.__class__.__name__}"
        self._throttler = Throttler(
            timeout=throttle_timeout, redis_url=redis_url, key_prefix=key_prefix
        )
        self.log = log if log is not None else logging.getLogger(self.__class__.__module__)

    def _request_limited(
        self,
        url: str,
        params: dict[str, str],
        timeout: int,
    ) -> requests.Response:
        """Perform GET through rate-limited layer (weight + 429 retry)."""
        tracker = get_tracker()
        return request_with_retry(
            tracker=tracker,
            exchange_id=self.exchange_id(),
            kind=self._connector_kind(),
            url=url,
            params=params,
            timeout=timeout,
        )


class BaseCEXSpotConnector(_BaseCEXConnectorMixin, ABC):
    """Base connector for spot market: tickers, prices, orderbook, klines."""

    @classmethod
    @abstractmethod
    def exchange_id(cls) -> str:
        ...

    def _connector_kind(self) -> str:
        return "spot"

    @abstractmethod
    def start(
        self,
        cb: Callback,
        symbols: list[str] | None = None,
        depth: bool = True,
        klines: bool = True,
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

    def _connector_kind(self) -> str:
        return "perpetual"

    @abstractmethod
    def start(
        self,
        cb: Callback,
        symbols: list[str] | None = None,
        depth: bool = True,
        klines: bool = True,
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
