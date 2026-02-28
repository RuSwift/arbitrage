"""Tests for perpetual (USD-M) CEX connectors (REST: perpetuals, pairs, price, depth, klines)."""

from time import sleep

import pytest

from app.cex.base import BaseCEXPerpetualConnector
from app.cex.dto import FundingRate
from app.cex.ws_klines import WS_KLINE_UNSUPPORTED
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
    """Perpetual connector for current exchange; requires Redis."""
    return request.param()


@pytest.fixture(scope="session")
def perps_cache() -> dict:
    """Cache of get_all_perpetuals() per exchange for the test session."""
    return {}


@pytest.fixture
def valid_pair_code(
    connector: BaseCEXPerpetualConnector, perps_cache: dict
) -> str:
    """First available perpetual symbol for this exchange (cached per session)."""
    key = connector.exchange_id()
    if key not in perps_cache:
        perps_cache[key] = connector.get_all_perpetuals()
    perps = perps_cache[key]
    assert len(perps) > 0
    return perps[0].symbol


# Number of symbols to try in test_get_funding_rate; at least one must have non-zero rate
FUNDING_RATE_CHECK_SYMBOLS = 10


@pytest.fixture
def symbols_for_funding(
    connector: BaseCEXPerpetualConnector, perps_cache: dict
) -> list[str]:
    """First N perpetual symbols for funding rate check (cached per session)."""
    key = connector.exchange_id()
    if key not in perps_cache:
        perps_cache[key] = connector.get_all_perpetuals()
    perps = perps_cache[key]
    assert len(perps) > 0
    return [p.symbol for p in perps[:FUNDING_RATE_CHECK_SYMBOLS]]


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

    @pytest.mark.timeout(30)
    def test_get_funding_rate(
        self,
        connector: BaseCEXPerpetualConnector,
        symbols_for_funding: list[str],
    ) -> None:
        # Try several symbols; at least one must return non-zero funding (e.g. Bitfinex can have 0 for first pair)
        fr_with_rate: FundingRate | None = None
        for sym in symbols_for_funding:
            fr = connector.get_funding_rate(sym)
            if fr is not None and fr.rate is not None and fr.rate != 0.0:
                fr_with_rate = fr
                break
        assert fr_with_rate is not None, (
            f"no non-zero funding rate among first {len(symbols_for_funding)} symbols: {symbols_for_funding}"
        )
        common_check_funding_rate(fr_with_rate)
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
            pt = history[0]
            common_check_funding_rate_point(pt)
            assert pt.rate is not None, "funding rate point rate must not be empty"
            assert pt.rate != 0.0, "funding rate point rate must not be zero"

    @pytest.mark.slow
    @pytest.mark.timeout(45)
    @pytest.mark.filterwarnings(
        "ignore::pytest.PytestUnhandledThreadExceptionWarning"
    )
    def test_book_events(
        self, connector: BaseCEXPerpetualConnector, valid_pair_code: str
    ) -> None:
        cb = TestableCallback()
        connector.start(cb, symbols=[valid_pair_code, "BTC/INVALID"])
        wait_sec = 20 if connector.exchange_id() == "gate" else 5
        sleep(wait_sec)
        connector.stop()
        sleep(1)
        assert len(cb.books) > 0 or len(cb.depths) > 0, "expected at least one book or depth event"
        assert len(cb.books) > 0, "expected at least one book_ticker event"
        assert len(cb.depths) > 0, "expected at least one depth event"
        if cb.depths and cb.depths[0].bids and cb.depths[0].asks:
            common_check_book_depth(cb.depths[0])
        if cb.books:
            common_check_book_ticker(cb.books[0])
        if (connector.exchange_id(), "perpetual") not in WS_KLINE_UNSUPPORTED:
            assert len(cb.klines) > 0, "expected at least one kline event (WS klines supported)"
