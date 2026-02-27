"""Tests for perpetual (USD-M) CEX connectors (REST: perpetuals, pairs, price, depth, klines)."""

from time import sleep

import pytest

from app.cex.base import BaseCEXPerpetualConnector
from app.cex import (
    BinancePerpetualConnector,
    BitfinexPerpetualConnector,
    BybitPerpetualConnector,
    GatePerpetualConnector,
    HtxPerpetualConnector,
    KucoinPerpetualConnector,
    MexcPerpetualConnector,
    OkxPerpetualConnector,
)

from .helpers_connectors import (
    TestableCallback,
    common_check_book_depth,
    common_check_book_ticker,
    common_check_currency_pair,
    common_check_funding_rate,
    common_check_funding_rate_point,
    common_check_perpetual_ticker,
)

PERPETUAL_CONNECTORS = [
    BinancePerpetualConnector,
    BitfinexPerpetualConnector,
    BybitPerpetualConnector,
    GatePerpetualConnector,
    HtxPerpetualConnector,
    KucoinPerpetualConnector,
    MexcPerpetualConnector,
    OkxPerpetualConnector,
]


@pytest.fixture(params=PERPETUAL_CONNECTORS, ids=[c.__name__ for c in PERPETUAL_CONNECTORS])
def connector(request, redis_client) -> BaseCEXPerpetualConnector:
    """Perpetual connector for current exchange; requires Redis (skipped if unavailable)."""
    return request.param()


@pytest.fixture
def valid_pair_code() -> str:
    return "BTC/USDT"


class TestPerpetualConnector:
    @pytest.mark.timeout(15)
    def test_get_all_perpetuals(self, connector: BaseCEXPerpetualConnector) -> None:
        perps = connector.get_all_perpetuals()
        assert len(perps) > 0
        common_check_perpetual_ticker(perps[0])

    @pytest.mark.timeout(25)
    def test_get_pairs(self, connector: BaseCEXPerpetualConnector) -> None:
        pairs = connector.get_pairs()
        assert len(pairs) > 0
        common_check_currency_pair(pairs[0])

    @pytest.mark.timeout(15)
    def test_get_price(
        self, connector: BaseCEXPerpetualConnector, valid_pair_code: str
    ) -> None:
        pair = connector.get_price(valid_pair_code)
        assert pair is not None
        common_check_currency_pair(pair)
        assert connector.get_price("XXX/BTC") is None

    @pytest.mark.timeout(15)
    def test_get_depth(
        self, connector: BaseCEXPerpetualConnector, valid_pair_code: str
    ) -> None:
        book = connector.get_depth(valid_pair_code)
        assert book is not None
        assert len(book.bids) > 0
        assert len(book.asks) > 0
        common_check_book_depth(book)

    @pytest.mark.timeout(15)
    def test_get_klines(
        self, connector: BaseCEXPerpetualConnector, valid_pair_code: str
    ) -> None:
        klines = connector.get_klines(valid_pair_code)
        assert klines is not None
        assert isinstance(klines, list)
        assert len(klines) > 0
        # Ascending or descending order depends on exchange
        assert klines[0].utc_open_time != klines[-1].utc_open_time
        assert 1 <= len(klines) <= 200

    @pytest.mark.timeout(15)
    def test_get_funding_rate(
        self, connector: BaseCEXPerpetualConnector, valid_pair_code: str
    ) -> None:
        fr = connector.get_funding_rate(valid_pair_code)
        assert fr is not None
        common_check_funding_rate(fr)
        assert connector.get_funding_rate("XXX/BTC") is None

    @pytest.mark.timeout(15)
    def test_get_funding_rate_history(
        self, connector: BaseCEXPerpetualConnector, valid_pair_code: str
    ) -> None:
        history = connector.get_funding_rate_history(valid_pair_code, limit=10)
        assert history is not None
        assert isinstance(history, list)
        assert len(history) <= 10
        if history:
            common_check_funding_rate_point(history[0])

    @pytest.mark.slow
    @pytest.mark.timeout(15)
    @pytest.mark.filterwarnings(
        "ignore::pytest.PytestUnhandledThreadExceptionWarning"
    )
    def test_book_events(
        self, connector: BaseCEXPerpetualConnector, valid_pair_code: str
    ) -> None:
        cb = TestableCallback()
        connector.start(cb, symbols=[valid_pair_code, "BTC/INVALID"])
        sleep(5)
        connector.stop()
        sleep(1)  # let websocket threads (e.g. pybit ping) exit before teardown
        assert len(cb.books) > 0
        if cb.depths and cb.depths[0].bids and cb.depths[0].asks:
            common_check_book_depth(cb.depths[0])
        common_check_book_ticker(cb.books[0])
