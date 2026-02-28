"""Tests for spot CEX connectors (REST: tickers, pairs, price, depth, klines)."""

from time import sleep

import pytest

from app.cex.base import BaseCEXSpotConnector
from app.cex import (
    BinanceSpotConnector,
    BitfinexSpotConnector,
    BybitSpotConnector,
    GateSpotConnector,
    HtxSpotConnector,
    KucoinSpotConnector,
    MexcSpotConnector,
    OkxSpotConnector,
)

from .helpers_connectors import (
    TestableCallback,
    common_check_book_depth,
    common_check_book_ticker,
    common_check_currency_pair,
    common_check_ticker,
)

SPOT_CONNECTORS = [
    BinanceSpotConnector,
    BitfinexSpotConnector,
    BybitSpotConnector,
    GateSpotConnector,
    HtxSpotConnector,
    KucoinSpotConnector,
    MexcSpotConnector,
    OkxSpotConnector,
]


@pytest.fixture(params=SPOT_CONNECTORS, ids=[c.__name__ for c in SPOT_CONNECTORS])
def connector(request, redis_client) -> BaseCEXSpotConnector:
    """Spot connector for current exchange; requires Redis."""
    return request.param()


@pytest.fixture
def valid_pair_code() -> str:
    return "BTC/USDT"


class TestSpotConnector:
    @pytest.mark.timeout(15)
    def test_get_tickers_list(self, connector: BaseCEXSpotConnector) -> None:
        tickers = connector.get_all_tickers()
        assert len(tickers) > 0
        common_check_ticker(tickers[0])

    @pytest.mark.timeout(15)
    def test_get_tickers_caching(self, connector: BaseCEXSpotConnector) -> None:
        tickers = connector.get_all_tickers()
        for _ in range(2):
            other = connector.__class__()
            other_tickers = other.get_all_tickers()
            assert len(tickers) == len(other_tickers)
            by_sym = {t.symbol: t for t in tickers}
            other_by_sym = {t.symbol: t for t in other_tickers}
            assert set(by_sym) == set(other_by_sym)
            for sym in by_sym:
                assert by_sym[sym] == other_by_sym[sym]

    @pytest.mark.timeout(15)
    def test_get_pairs(self, connector: BaseCEXSpotConnector) -> None:
        pairs = connector.get_pairs()
        assert len(pairs) > 0
        common_check_currency_pair(pairs[0])

    @pytest.mark.timeout(15)
    def test_get_price(
        self, connector: BaseCEXSpotConnector, valid_pair_code: str
    ) -> None:
        pair = connector.get_price(valid_pair_code)
        assert pair is not None
        common_check_currency_pair(pair)
        invalid_code = "XXX/BTC"
        assert connector.get_price(invalid_code) is None

    @pytest.mark.timeout(15)
    def test_get_depth(
        self, connector: BaseCEXSpotConnector, valid_pair_code: str
    ) -> None:
        book = connector.get_depth(valid_pair_code)
        assert book is not None
        assert len(book.bids) > 0
        assert len(book.asks) > 0
        common_check_book_depth(book)

    @pytest.mark.timeout(15)
    def test_get_klines(
        self, connector: BaseCEXSpotConnector, valid_pair_code: str
    ) -> None:
        klines = connector.get_klines(valid_pair_code)
        assert klines is not None
        assert isinstance(klines, list)
        assert len(klines) > 0
        # Ascending or descending order depends on exchange
        assert klines[0].utc_open_time != klines[-1].utc_open_time
        assert 1 <= len(klines) <= 200

    @pytest.mark.slow
    @pytest.mark.timeout(15)
    @pytest.mark.filterwarnings(
        "ignore::pytest.PytestUnhandledThreadExceptionWarning"
    )
    def test_book_events(
        self, connector: BaseCEXSpotConnector, valid_pair_code: str
    ) -> None:
        cb = TestableCallback()
        connector.start(cb, symbols=[valid_pair_code, "BTC/INVALID"])
        sleep(5)
        connector.stop()
        sleep(1)  # let websocket threads (e.g. pybit ping) exit before teardown
        assert len(cb.books) > 0 or len(cb.depths) > 0, "expected at least one book or depth update"
        if cb.depths and cb.depths[0].bids and cb.depths[0].asks:
            common_check_book_depth(cb.depths[0])
        if cb.books:
            common_check_book_ticker(cb.books[0])
