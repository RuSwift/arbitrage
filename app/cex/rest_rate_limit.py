"""
REST rate limiting: weight tracking per exchange (window) and 429 retry with backoff.

All limit config lives here so it can be updated from official docs (e.g. via LLM).
"""

from __future__ import annotations

import time
from typing import Any

import requests

# -----------------------------------------------------------------------------
# REST limits per exchange (update from official docs when needed)
# Doc URLs (workspace rules): binance, bybit, okx, kucoin, htx, mexc, bitfinex
# -----------------------------------------------------------------------------

# Binance Futures (USD-M): weight per minute, header in response
# https://www.binance.com/en/binance-api
# https://binance-docs.github.io/apidocs/futures/en/#limits
BINANCE_LIMIT = 6000
BINANCE_WEIGHT_HEADER = "X-MBX-USED-WEIGHT-1M"

# OKX: rate limits per endpoint; no single weight header in public docs
# https://www.okx.com/docs-v5/en/
# Use conservative request-per-minute style (weight 1 per request)
OKX_LIMIT = 60 * 20  # 20 req/s * 60 → conservative 1200/min
OKX_WEIGHT_HEADER = None

# HTX (Huobi): https://www.htx.com/en-us/opend/newApiPages/
HTX_LIMIT = 100
HTX_WEIGHT_HEADER = None

# Gate: https://www.gate.io/docs/developers/apiv4/
GATE_LIMIT = 100
GATE_WEIGHT_HEADER = None

# KuCoin: https://www.kucoin.com/docs-new
KUCOIN_LIMIT = 100
KUCOIN_WEIGHT_HEADER = None

# MEXC: https://www.mexc.com/ru-RU/mexc-api
MEXC_LIMIT = 100
MEXC_WEIGHT_HEADER = None

# Bitfinex: https://docs.bitfinex.com/docs/rest-public
BITFINEX_LIMIT = 100
BITFINEX_WEIGHT_HEADER = None

# Key: (exchange_id, kind). kind in ("spot", "perpetual")
# Value: dict with "limit" (int) and "weight_header" (str | None)
def _limits() -> dict[tuple[str, str], dict[str, Any]]:
    return {
        ("binance", "spot"): {"limit": BINANCE_LIMIT, "weight_header": BINANCE_WEIGHT_HEADER},
        ("binance", "perpetual"): {"limit": BINANCE_LIMIT, "weight_header": BINANCE_WEIGHT_HEADER},
        ("okx", "spot"): {"limit": OKX_LIMIT, "weight_header": OKX_WEIGHT_HEADER},
        ("okx", "perpetual"): {"limit": OKX_LIMIT, "weight_header": OKX_WEIGHT_HEADER},
        ("htx", "spot"): {"limit": HTX_LIMIT, "weight_header": HTX_WEIGHT_HEADER},
        ("htx", "perpetual"): {"limit": HTX_LIMIT, "weight_header": HTX_WEIGHT_HEADER},
        ("gate", "spot"): {"limit": GATE_LIMIT, "weight_header": GATE_WEIGHT_HEADER},
        ("gate", "perpetual"): {"limit": GATE_LIMIT, "weight_header": GATE_WEIGHT_HEADER},
        ("kucoin", "spot"): {"limit": KUCOIN_LIMIT, "weight_header": KUCOIN_WEIGHT_HEADER},
        ("kucoin", "perpetual"): {"limit": KUCOIN_LIMIT, "weight_header": KUCOIN_WEIGHT_HEADER},
        ("mexc", "spot"): {"limit": MEXC_LIMIT, "weight_header": MEXC_WEIGHT_HEADER},
        ("mexc", "perpetual"): {"limit": MEXC_LIMIT, "weight_header": MEXC_WEIGHT_HEADER},
        ("bitfinex", "spot"): {"limit": BITFINEX_LIMIT, "weight_header": BITFINEX_WEIGHT_HEADER},
        ("bitfinex", "perpetual"): {"limit": BITFINEX_LIMIT, "weight_header": BITFINEX_WEIGHT_HEADER},
    }


_LIMITS_CACHE: dict[tuple[str, str], dict[str, Any]] | None = None


def get_limit(exchange_id: str, kind: str) -> tuple[int, str | None]:
    """Return (limit, weight_header) for (exchange_id, kind). Default 100, None if unknown."""
    global _LIMITS_CACHE
    if _LIMITS_CACHE is None:
        _LIMITS_CACHE = _limits()
    key = (exchange_id.lower(), kind)
    if key in _LIMITS_CACHE:
        c = _LIMITS_CACHE[key]
        return (c["limit"], c.get("weight_header"))
    return (100, None)


# -----------------------------------------------------------------------------
# WeightTracker
# -----------------------------------------------------------------------------

class WeightTracker:
    """In-memory weight usage per (exchange_id, kind) over a sliding window."""

    def __init__(self, window_sec: float = 60.0) -> None:
        self._window_sec = window_sec
        # key: (exchange_id, kind) -> {"window_start": float, "used_weight": float}
        self._state: dict[tuple[str, str], dict[str, float]] = {}

    def _get_state(self, exchange_id: str, kind: str) -> dict[str, float]:
        key = (exchange_id.lower(), kind)
        if key not in self._state:
            self._state[key] = {"window_start": time.monotonic(), "used_weight": 0.0}
        return self._state[key]

    def _reset_window(self, exchange_id: str, kind: str) -> None:
        key = (exchange_id.lower(), kind)
        self._state[key] = {"window_start": time.monotonic(), "used_weight": 0.0}

    def wait_if_needed(self, exchange_id: str, kind: str, estimated_weight: float = 1.0) -> None:
        limit, _ = get_limit(exchange_id, kind)
        st = self._get_state(exchange_id, kind)
        now = time.monotonic()
        elapsed = now - st["window_start"]
        if elapsed >= self._window_sec:
            self._reset_window(exchange_id, kind)
            return
        if st["used_weight"] + estimated_weight >= limit:
            sleep_sec = max(0.0, self._window_sec - elapsed)
            time.sleep(sleep_sec)
            self._reset_window(exchange_id, kind)

    def add_used(self, exchange_id: str, kind: str, weight: float) -> None:
        st = self._get_state(exchange_id, kind)
        now = time.monotonic()
        if now - st["window_start"] >= self._window_sec:
            self._reset_window(exchange_id, kind)
            st = self._get_state(exchange_id, kind)
        st["used_weight"] += weight


# -----------------------------------------------------------------------------
# Singleton tracker for connectors (set by crawler or first use)
# -----------------------------------------------------------------------------

_tracker: WeightTracker | None = None
_default_window_sec = 60.0


def get_tracker(window_sec: float | None = None) -> WeightTracker:
    """Return the global WeightTracker. If none exists, create one with window_sec or default."""
    global _tracker, _default_window_sec
    if _tracker is None:
        _tracker = WeightTracker(window_sec=(window_sec if window_sec is not None else _default_window_sec))
    elif window_sec is not None and window_sec != _tracker._window_sec:
        _tracker = WeightTracker(window_sec=window_sec)
    return _tracker


def set_default_window_sec(sec: float) -> None:
    """Set default window used when creating tracker (e.g. from crawler CLI)."""
    global _default_window_sec
    _default_window_sec = sec


# -----------------------------------------------------------------------------
# request_with_retry: weight wait, 429 backoff, weight accounting
# -----------------------------------------------------------------------------

DEFAULT_WEIGHT_ESTIMATE = 1
MAX_RETRIES_429 = 2
MAX_DELAY_429 = 120
BACKOFF_MULTIPLIER = 1.5


def request_with_retry(
    tracker: WeightTracker,
    exchange_id: str,
    kind: str,
    url: str,
    params: dict[str, str] | None = None,
    timeout: int = 15,
    max_retries_429: int = MAX_RETRIES_429,
    max_delay_429: int = MAX_DELAY_429,
    default_weight: float = DEFAULT_WEIGHT_ESTIMATE,
) -> requests.Response:
    """Perform GET with weight wait, 429 retry (increasing delay, capped), and weight accounting."""
    params = params or {}
    attempt = 0
    delay_429 = 0.0  # will be set from Retry-After on first 429

    while True:
        tracker.wait_if_needed(exchange_id, kind, estimated_weight=default_weight)
        r = requests.get(url, params=params, timeout=timeout)

        if r.status_code != 429:
            if 200 <= r.status_code < 300:
                limit, header_name = get_limit(exchange_id, kind)
                if header_name and header_name in r.headers:
                    try:
                        w = float(r.headers[header_name])
                    except (ValueError, TypeError):
                        w = default_weight
                else:
                    w = default_weight
                tracker.add_used(exchange_id, kind, w)
            return r

        # 429: compute delay (increase on each retry, cap at max_delay_429)
        attempt += 1
        try:
            retry_after = int(r.headers.get("Retry-After", 60))
        except (ValueError, TypeError):
            retry_after = 60
        retry_after = min(retry_after, max_delay_429)
        if attempt == 1:
            delay_429 = float(retry_after)
        else:
            delay_429 = min(delay_429 * BACKOFF_MULTIPLIER, max_delay_429)
        if attempt > max_retries_429:
            r.raise_for_status()
        time.sleep(delay_429)
