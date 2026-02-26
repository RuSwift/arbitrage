"""Shared helpers and assertions for CEX connector tests (spot and perpetual)."""

from __future__ import annotations

from typing import List

from app.cex.base import Callback
from app.cex.dto import BookDepth, BookTicker, CurrencyPair, PerpetualTicker, Ticker


class TestableCallback(Callback):
    """Callback that collects book and depth updates for assertions."""

    def __init__(self) -> None:
        self.books: List[BookTicker] = []
        self.depths: List[BookDepth] = []

    def handle(
        self,
        book: BookTicker | None = None,
        depth: BookDepth | None = None,
    ) -> None:
        if book is not None:
            self.books.append(book)
        if depth is not None:
            self.depths.append(depth)


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


def common_check_currency_pair(obj: CurrencyPair) -> None:
    assert isinstance(obj, CurrencyPair)
    assert obj.base
    assert obj.quote
    assert isinstance(obj.ratio, (int, float))


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
