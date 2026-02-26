from typing import List

from core.defs import CurrencyPair
from core.connectors import BaseExchangeConnector, Ticker, BookTicker, BookDepth


class TestableCallback(BaseExchangeConnector.Callback):

    def __init__(self):
        self.books: List[BookTicker] = []
        self.depths: List[BookDepth] = []

    def handle(self, book: BookTicker = None, depth: BookDepth = None):
        if book:
            self.books.append(book)
        if depth:
            self.depths.append(depth)


def common_check_ticker_obj(obj: Ticker):
    assert isinstance(obj, Ticker)
    assert obj.symbol
    assert '/' in obj.symbol
    assert obj.base
    assert obj.quote
    assert obj.is_spot_enabled in [True, False]
    assert obj.is_margin_enabled in [True, False]


def common_check_curr_pair(obj: CurrencyPair):
    assert isinstance(obj, CurrencyPair)
    assert obj.base
    assert obj.quota
    assert obj.ratio


def common_check_book_ticker(obj: BookTicker):
    assert isinstance(obj, BookTicker)
    assert obj.symbol
    assert '/' in obj.symbol
    assert type(obj.ask_qty) is float
    assert type(obj.ask_price) is float
    assert type(obj.bid_qty) is float
    assert type(obj.bid_price) is float
    assert obj.last_update_id is not None
    assert type(obj.utc) is float


def common_check_book_depth(obj: BookDepth):
    assert isinstance(obj, BookDepth)
    assert obj.symbol
    assert '/' in obj.symbol
    assert len(obj.asks) > 0
    assert len(obj.bids) > 0
    assert type(obj.asks[0].price) is float
    assert type(obj.asks[0].quantity) is float
    assert type(obj.bids[0].price) is float
    assert type(obj.bids[0].quantity) is float
    assert obj.last_update_id is not None
    assert type(obj.utc) is float
