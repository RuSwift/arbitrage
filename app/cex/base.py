"""Base CEX connectors: spot and perpetual (no P2P)."""

from __future__ import annotations

import logging
import threading
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
DEFAULT_SUBSCRIPTION_BATCH_SEC = 4.0  # batch interval for subscribe/unsubscribe; reconnect connectors use 15.0


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
        log: logging.Logger | None = None,
    ) -> None:
        self._is_testing = is_testing
        redis_url = Settings().redis.url
        key_prefix = f"arbitrage:throttle:{self.__class__.__name__}"
        self._throttler = Throttler(
            timeout=throttle_timeout, redis_url=redis_url, key_prefix=key_prefix
        )
        self.log = log if log is not None else logging.getLogger(self.__class__.__module__)
        # Subscribe/unsubscribe batching: queues, lock, timer (started once per batch, no reset)
        self._pending_sub: set[str] = set()
        self._pending_unsub: set[str] = set()
        self._sub_lock = threading.Lock()
        self._sub_timer: threading.Timer | None = None
        self._sub_timer_active = False
        self._subscription_batch_sec: float = getattr(
            self.__class__, "_subscription_batch_sec", DEFAULT_SUBSCRIPTION_BATCH_SEC
        )

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

    def subscribe(self, tokens: list[str]) -> None:
        """Queue tokens for subscription; applied after batch interval (timer started only if idle)."""
        if not tokens:
            return
        with self._sub_lock:
            self._pending_sub.update(tokens)
            self._pending_unsub -= set(tokens)
            if not self._sub_timer_active:
                self._sub_timer_active = True
                self._sub_timer = threading.Timer(
                    self._subscription_batch_sec,
                    self._flush_subscriptions,
                )
                self._sub_timer.daemon = True
                self._sub_timer.start()

    def unsubscribe(self, tokens: list[str]) -> None:
        """Queue tokens for unsubscription; applied after batch interval (timer started only if idle)."""
        if not tokens:
            return
        with self._sub_lock:
            self._pending_unsub.update(tokens)
            self._pending_sub -= set(tokens)
            if not self._sub_timer_active:
                self._sub_timer_active = True
                self._sub_timer = threading.Timer(
                    self._subscription_batch_sec,
                    self._flush_subscriptions,
                )
                self._sub_timer.daemon = True
                self._sub_timer.start()

    def _flush_subscriptions(self) -> None:
        """Run under timer callback: take snapshot, clear state, call _apply_* and _after_subscription_flush."""
        with self._sub_lock:
            to_sub = list(self._pending_sub)
            to_unsub = list(self._pending_unsub)
            self._pending_sub.clear()
            self._pending_unsub.clear()
            if self._sub_timer is not None:
                self._sub_timer.cancel()
                self._sub_timer = None
            self._sub_timer_active = False
        if to_sub or to_unsub:
            self._apply_unsubscribe(to_unsub)
            self._apply_subscribe(to_sub)
            self._after_subscription_flush()

    def _cancel_subscription_timer(self) -> None:
        """Cancel batch timer and clear pending queues. Call from stop()."""
        with self._sub_lock:
            if self._sub_timer is not None:
                self._sub_timer.cancel()
                self._sub_timer = None
            self._sub_timer_active = False
            self._pending_sub.clear()
            self._pending_unsub.clear()

    def _apply_subscribe(self, tokens: list[str]) -> None:
        """Override in connectors that support dynamic subscribe. Default: no-op."""
        pass

    def _apply_unsubscribe(self, tokens: list[str]) -> None:
        """Override in connectors that support dynamic subscribe. Default: no-op."""
        pass

    def _after_subscription_flush(self) -> None:
        """Override in reconnect-type connectors to run stop/start after batch. Default: no-op."""
        pass


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
