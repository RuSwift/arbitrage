"""Cache facades for spot and perpetual connectors (Redis-backed)."""

from __future__ import annotations

import json
import logging
from typing import Callable, TypeVar

from app.cex.base import (
    BaseCEXPerpetualConnector,
    BaseCEXSpotConnector,
    Callback,
    DEFAULT_DEPTH_LIMIT,
    DEFAULT_FUNDING_HISTORY_LIMIT,
)
from app.cex.dto import (
    BookDepth,
    CandleStick,
    CurrencyPair,
    FundingRate,
    FundingRatePoint,
    PerpetualTicker,
    Ticker,
    WithdrawInfo,
)
from app.settings import Settings

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _cache_key(exchange_id: str, kind: str, method: str, *parts: str) -> str:
    """Redis key: {exchange_id}:{kind}:{method}:{parts}."""
    if not parts:
        return f"{exchange_id}:{kind}:{method}"
    return f"{exchange_id}:{kind}:{method}:" + ":".join(str(p) for p in parts)


class CachedSpotConnector:
    """Facade over a spot connector that caches responses in Redis."""

    def __init__(
        self,
        impl: BaseCEXSpotConnector,
        cache_timeout: int = 1,
    ) -> None:
        self.impl = impl
        self.cache_timeout = cache_timeout
        self._redis_url = Settings().redis.url
        self._client: object = None

    def _client_get(self):
        if self._client is None:
            import redis
            self._client = redis.from_url(self._redis_url)
        return self._client

    def _get(self, key: str) -> str | None:
        if self.cache_timeout <= 0:
            return None
        try:
            raw = self._client_get().get(key)
            return raw.decode("utf-8") if isinstance(raw, bytes) else raw
        except Exception as e:
            logger.warning("Cache get failed for %s: %s", key, e)
            return None

    def _set(self, key: str, value: str, ttl: int) -> None:
        if ttl <= 0:
            return
        try:
            self._client_get().setex(key, ttl, value)
        except Exception as e:
            logger.warning("Cache set failed for %s: %s", key, e)

    def _cached_value(
        self,
        key: str,
        fetch: Callable[[], T | None],
        to_dict: Callable[[T], dict],
        from_dict: Callable[[dict], T],
    ) -> T | None:
        raw = self._get(key)
        if raw is not None:
            if raw == "null":
                return None
            try:
                return from_dict(json.loads(raw))
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        out = fetch()
        if self.cache_timeout > 0:
            self._set(key, "null" if out is None else json.dumps(to_dict(out)), self.cache_timeout)
        return out

    def _cached_list(
        self,
        key: str,
        fetch: Callable[[], list[T]],
        to_dict: Callable[[T], dict],
        from_dict: Callable[[dict], T],
    ) -> list[T]:
        raw = self._get(key)
        if raw is not None:
            try:
                return [from_dict(d) for d in json.loads(raw)]
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        out = fetch()
        if self.cache_timeout > 0:
            self._set(key, json.dumps([to_dict(x) for x in out]), self.cache_timeout)
        return out

    def exchange_id(self) -> str:
        return self.impl.exchange_id()

    def start(
        self,
        cb: Callback,
        symbols: list[str] | None = None,
        depth: bool = True,
    ) -> None:
        self.impl.start(cb, symbols=symbols, depth=depth)

    def stop(self) -> None:
        self.impl.stop()

    def get_all_tickers(self) -> list[Ticker]:
        key = _cache_key(self.impl.exchange_id(), "spot", "ticker")
        return self._cached_list(
            key,
            self.impl.get_all_tickers,
            Ticker.as_dict,
            Ticker.from_dict,
        )

    def get_price(self, pair_code: str) -> CurrencyPair | None:
        sym = pair_code.replace("/", "").upper()
        key = _cache_key(self.impl.exchange_id(), "spot", "price", sym)
        return self._cached_value(
            key,
            lambda: self.impl.get_price(pair_code),
            CurrencyPair.as_dict,
            CurrencyPair.from_dict,
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        parts = "all" if symbols is None else ",".join(sorted(s.replace("/", "").upper() for s in symbols))
        key = _cache_key(self.impl.exchange_id(), "spot", "pairs", parts)
        return self._cached_list(
            key,
            lambda: self.impl.get_pairs(symbols),
            CurrencyPair.as_dict,
            CurrencyPair.from_dict,
        )

    def get_depth(self, symbol: str, limit: int = DEFAULT_DEPTH_LIMIT) -> BookDepth | None:
        sym_norm = symbol.replace("/", "").upper()
        key = _cache_key(self.impl.exchange_id(), "spot", "depth", sym_norm, str(limit))
        return self._cached_value(
            key,
            lambda: self.impl.get_depth(symbol, limit=limit),
            BookDepth.as_dict,
            BookDepth.from_dict,
        )

    def get_klines(
        self, symbol: str, limit: int | None = None
    ) -> list[CandleStick] | None:
        n = limit if limit is not None else getattr(self.impl, "KLINE_SIZE", 60)
        sym_norm = symbol.replace("/", "").upper()
        key = _cache_key(self.impl.exchange_id(), "spot", "klines", sym_norm, str(n))
        return self._cached_value(
            key,
            lambda: self.impl.get_klines(symbol, limit=limit),
            lambda x: [c.as_dict() for c in x] if x is not None else None,
            lambda d: [CandleStick.from_dict(i) for i in d] if d is not None else None,
        )

    def get_withdraw_info(self) -> dict[str, list[WithdrawInfo]] | None:
        return self.impl.get_withdraw_info()


class CachedPerpetualConnector:
    """Facade over a perpetual connector that caches responses in Redis."""

    def __init__(
        self,
        impl: BaseCEXPerpetualConnector,
        cache_timeout: int = 1,
    ) -> None:
        self.impl = impl
        self.cache_timeout = cache_timeout
        self._redis_url = Settings().redis.url
        self._client: object = None

    def _client_get(self):
        if self._client is None:
            import redis
            self._client = redis.from_url(self._redis_url)
        return self._client

    def _get(self, key: str) -> str | None:
        if self.cache_timeout <= 0:
            return None
        try:
            raw = self._client_get().get(key)
            return raw.decode("utf-8") if isinstance(raw, bytes) else raw
        except Exception as e:
            logger.warning("Cache get failed for %s: %s", key, e)
            return None

    def _set(self, key: str, value: str, ttl: int) -> None:
        if ttl <= 0:
            return
        try:
            self._client_get().setex(key, ttl, value)
        except Exception as e:
            logger.warning("Cache set failed for %s: %s", key, e)

    def _cached_value(
        self,
        key: str,
        fetch: Callable[[], T | None],
        to_dict: Callable[[T], dict],
        from_dict: Callable[[dict], T],
    ) -> T | None:
        raw = self._get(key)
        if raw is not None:
            if raw == "null":
                return None
            try:
                return from_dict(json.loads(raw))
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        out = fetch()
        if self.cache_timeout > 0:
            self._set(key, "null" if out is None else json.dumps(to_dict(out)), self.cache_timeout)
        return out

    def _cached_list(
        self,
        key: str,
        fetch: Callable[[], list[T]],
        to_dict: Callable[[T], dict],
        from_dict: Callable[[dict], T],
    ) -> list[T]:
        raw = self._get(key)
        if raw is not None:
            try:
                return [from_dict(d) for d in json.loads(raw)]
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        out = fetch()
        if self.cache_timeout > 0:
            self._set(key, json.dumps([to_dict(x) for x in out]), self.cache_timeout)
        return out

    def exchange_id(self) -> str:
        return self.impl.exchange_id()

    def start(
        self,
        cb: Callback,
        symbols: list[str] | None = None,
        depth: bool = True,
    ) -> None:
        self.impl.start(cb, symbols=symbols, depth=depth)

    def stop(self) -> None:
        self.impl.stop()

    def get_all_perpetuals(self) -> list[PerpetualTicker]:
        key = _cache_key(self.impl.exchange_id(), "perpetual", "ticker")
        return self._cached_list(
            key,
            self.impl.get_all_perpetuals,
            PerpetualTicker.as_dict,
            PerpetualTicker.from_dict,
        )

    def get_price(self, symbol: str) -> CurrencyPair | None:
        sym = symbol.replace("/", "")
        key = _cache_key(self.impl.exchange_id(), "perpetual", "price", sym)
        return self._cached_value(
            key,
            lambda: self.impl.get_price(symbol),
            CurrencyPair.as_dict,
            CurrencyPair.from_dict,
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        parts = "all" if symbols is None else ",".join(sorted(s.replace("/", "") for s in symbols))
        key = _cache_key(self.impl.exchange_id(), "perpetual", "pairs", parts)
        return self._cached_list(
            key,
            lambda: self.impl.get_pairs(symbols),
            CurrencyPair.as_dict,
            CurrencyPair.from_dict,
        )

    def get_depth(
        self, symbol: str, limit: int = DEFAULT_DEPTH_LIMIT
    ) -> BookDepth | None:
        sym_norm = symbol.replace("/", "")
        key = _cache_key(self.impl.exchange_id(), "perpetual", "depth", sym_norm, str(limit))
        return self._cached_value(
            key,
            lambda: self.impl.get_depth(symbol, limit=limit),
            BookDepth.as_dict,
            BookDepth.from_dict,
        )

    def get_klines(
        self, symbol: str, limit: int | None = None
    ) -> list[CandleStick] | None:
        n = limit if limit is not None else getattr(self.impl, "KLINE_SIZE", 60)
        sym_norm = symbol.replace("/", "")
        key = _cache_key(self.impl.exchange_id(), "perpetual", "klines", sym_norm, str(n))
        return self._cached_value(
            key,
            lambda: self.impl.get_klines(symbol, limit=limit),
            lambda x: [c.as_dict() for c in x] if x is not None else None,
            lambda d: [CandleStick.from_dict(i) for i in d] if d is not None else None,
        )

    def get_funding_rate(self, symbol: str) -> FundingRate | None:
        sym_norm = symbol.replace("/", "")
        key = _cache_key(self.impl.exchange_id(), "perpetual", "funding", sym_norm)
        return self._cached_value(
            key,
            lambda: self.impl.get_funding_rate(symbol),
            FundingRate.as_dict,
            FundingRate.from_dict,
        )

    def get_funding_rate_history(
        self, symbol: str, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        n = limit if limit is not None else DEFAULT_FUNDING_HISTORY_LIMIT
        sym_norm = symbol.replace("/", "")
        key = _cache_key(
            self.impl.exchange_id(), "perpetual", "funding_history", sym_norm, str(n)
        )
        return self._cached_value(
            key,
            lambda: self.impl.get_funding_rate_history(symbol, limit=limit),
            lambda x: [p.as_dict() for p in x] if x is not None else None,
            lambda d: [FundingRatePoint.from_dict(i) for i in d] if d is not None else None,
        )
