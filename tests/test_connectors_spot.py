"""Tests for spot CEX connectors (REST: tickers, pairs, price, depth, klines)."""

from time import sleep

import pytest

from app.cex.base import BaseCEXSpotConnector
from app.cex.dto import BorrowableAsset
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
    @pytest.mark.timeout(35)
    @pytest.mark.filterwarnings(
        "ignore::pytest.PytestUnhandledThreadExceptionWarning"
    )
    def test_book_events(
        self, connector: BaseCEXSpotConnector, valid_pair_code: str
    ) -> None:
        if connector.exchange_id() == "mexc":
            pytest.skip("MEXC spot uses REST polling only, no WebSocket book/depth")
        cb = TestableCallback()
        connector.start(cb, symbols=[valid_pair_code, "BTC/INVALID"])
        sleep(5)
        connector.stop()
        sleep(1)
        assert len(cb.books) > 0 or len(cb.depths) > 0, "expected at least one book or depth event"
        assert len(cb.books) > 0, "expected at least one book_ticker event"
        assert len(cb.depths) > 0, "expected at least one depth event"
        if cb.depths and cb.depths[0].bids and cb.depths[0].asks:
            common_check_book_depth(cb.depths[0])
        if cb.books:
            common_check_book_ticker(cb.books[0])

    def test_get_borrowable_assets_unsupported_returns_none(
        self, connector: BaseCEXSpotConnector
    ) -> None:
        """Exchanges without borrow API return None."""
        if connector.exchange_id() in ("binance", "gate"):
            pytest.skip(f"{connector.exchange_id()} implements get_borrowable_assets")
        result = connector.get_borrowable_assets()
        assert result is None

    @pytest.mark.timeout(15)
    def test_get_borrowable_assets_binance_returns_list(self) -> None:
        """Binance spot returns list of BorrowableAsset from SAPI margin allAssets (may require API key)."""
        conn = BinanceSpotConnector()
        result = conn.get_borrowable_assets()
        if result is None:
            pytest.skip(
                "Binance SAPI margin allAssets returned error (endpoint may require API key)"
            )
        assert isinstance(result, list)
        assert len(result) > 0
        for item in result:
            assert isinstance(item, BorrowableAsset)
            assert isinstance(item.asset, str)
            assert len(item.asset) > 0
            assert isinstance(item.is_borrowable, bool)
        # At least one borrowable (e.g. USDT)
        borrowable = [a for a in result if a.is_borrowable]
        assert len(borrowable) > 0

    @pytest.mark.timeout(15)
    def test_get_borrowable_assets_gate_returns_list(self) -> None:
        """Gate spot returns list of BorrowableAsset from UniLoan API (public)."""
        conn = GateSpotConnector()
        result = conn.get_borrowable_assets()
        assert result is not None
        assert isinstance(result, list)
        assert len(result) > 0
        for item in result:
            assert isinstance(item, BorrowableAsset)
            assert isinstance(item.asset, str)
            assert len(item.asset) > 0
            assert isinstance(item.is_borrowable, bool)
        borrowable = [a for a in result if a.is_borrowable]
        assert len(borrowable) > 0
        # Gate API provides pool size and rate for many assets
        with_available = [a for a in result if a.available_to_borrow is not None]
        assert len(with_available) > 0

    def test_get_borrowable_assets_okx_returns_none(self) -> None:
        """OKX spot does not implement borrow API; returns None."""
        conn = OkxSpotConnector()
        assert conn.get_borrowable_assets() is None

    def test_borrowable_asset_dto_roundtrip(self) -> None:
        """BorrowableAsset as_dict/from_dict preserves required and optional fields."""
        asset = BorrowableAsset(
            asset="USDT",
            is_borrowable=True,
            available_to_borrow=1000.5,
            max_borrow=50000.0,
            hourly_borrow_rate=0.0001,
            min_borrow=10.0,
            min_repay=10.0,
        )
        data = asset.as_dict()
        restored = BorrowableAsset.from_dict(data)
        assert restored.asset == asset.asset
        assert restored.is_borrowable == asset.is_borrowable
        assert restored.available_to_borrow == asset.available_to_borrow
        assert restored.max_borrow == asset.max_borrow
        assert restored.hourly_borrow_rate == asset.hourly_borrow_rate
        assert restored.min_borrow == asset.min_borrow
        assert restored.min_repay == asset.min_repay
