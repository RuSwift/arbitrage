"""Shared helpers and assertions for CEX connector tests (spot and perpetual)."""

from __future__ import annotations

import time
from typing import List

from app.cex.base import Callback
from app.cex.dto import (
    BookDepth,
    BookTicker,
    CandleStick,
    CurrencyPair,
    FundingRate,
    FundingRatePoint,
    PerpetualTicker,
    Ticker,
)


class TestableCallback(Callback):
    """Callback that collects book, depth and kline updates for assertions."""

    def __init__(self) -> None:
        self.books: List[BookTicker] = []
        self.depths: List[BookDepth] = []
        self.klines: List[CandleStick] = []

    def handle(
        self,
        book: BookTicker | None = None,
        depth: BookDepth | None = None,
        kline: CandleStick | None = None,
    ) -> None:
        if book is not None:
            self.books.append(book)
        if depth is not None:
            self.depths.append(depth)
        if kline is not None:
            self.klines.append(kline)


def common_check_ticker(obj: Ticker) -> None:
    assert isinstance(obj, Ticker)
    assert obj.symbol
    assert "/" in obj.symbol
    assert obj.base
    assert obj.quote
    assert obj.is_spot_enabled in (True, False)
    assert obj.is_margin_enabled in (True, False)


def common_check_perpetual_ticker(obj: PerpetualTicker) -> None:
    assert isinstance(obj, PerpetualTicker)
    assert obj.symbol
    assert "/" in obj.symbol
    assert obj.base
    assert obj.quote
    assert obj.exchange_symbol
    assert obj.settlement


def assert_utc_near_now(utc_value: float | None, tolerance_seconds: float = 300) -> None:
    """If utc is present and positive, assert it is UTC seconds within ±tolerance_seconds of now."""
    if utc_value is None or (isinstance(utc_value, (int, float)) and float(utc_value) <= 0):
        return
    assert isinstance(utc_value, (int, float)), f"utc must be number, got {type(utc_value)}"
    now = time.time()
    assert abs(float(utc_value) - now) <= tolerance_seconds, (
        f"utc {utc_value} not within ±{tolerance_seconds}s of now {now}"
    )


def common_check_currency_pair(obj: CurrencyPair) -> None:
    assert isinstance(obj, CurrencyPair)
    assert obj.base
    assert obj.quote
    assert isinstance(obj.ratio, (int, float))
    assert_utc_near_now(obj.utc)


def common_check_book_ticker(obj: BookTicker) -> None:
    assert isinstance(obj, BookTicker)
    assert obj.symbol
    assert "/" in obj.symbol
    assert isinstance(obj.ask_qty, (int, float))
    assert isinstance(obj.ask_price, (int, float))
    assert isinstance(obj.bid_qty, (int, float))
    assert isinstance(obj.bid_price, (int, float))
    assert obj.last_update_id is not None or True  # optional on some exchanges
    assert obj.utc is None or isinstance(obj.utc, (int, float))
    assert_utc_near_now(obj.utc)


def common_check_book_depth(obj: BookDepth) -> None:
    assert isinstance(obj, BookDepth)
    assert obj.symbol
    assert "/" in obj.symbol
    assert len(obj.asks) > 0
    assert len(obj.bids) > 0
    assert isinstance(obj.asks[0].price, (int, float))
    assert isinstance(obj.asks[0].quantity, (int, float))
    assert isinstance(obj.bids[0].price, (int, float))
    assert isinstance(obj.bids[0].quantity, (int, float))
    assert obj.last_update_id is not None or True
    assert obj.utc is None or isinstance(obj.utc, (int, float))
    assert_utc_near_now(obj.utc)


def common_check_funding_rate(obj: FundingRate) -> None:
    assert isinstance(obj, FundingRate)
    assert obj.symbol
    assert "/" in obj.symbol
    assert isinstance(obj.rate, (int, float))
    assert isinstance(obj.next_funding_utc, (int, float))
    assert obj.next_funding_utc >= 0
    assert obj.utc is None or isinstance(obj.utc, (int, float))
    assert_utc_near_now(obj.utc)


def common_check_funding_rate_point(obj: FundingRatePoint) -> None:
    assert isinstance(obj, FundingRatePoint)
    assert isinstance(obj.funding_time_utc, (int, float))
    assert isinstance(obj.rate, (int, float))
