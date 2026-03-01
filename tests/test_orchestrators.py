"""Tests for sync and async orchestrator retrievers (get_price, get_depth, get_klines: Redis then DB, warm Redis)."""

import json

import pytest

from app.cex.dto import BidAsk, BookDepth, CandleStick
from app.cex.orcestrator import (
    AsyncPerpetualOrchestratorImpl,
    AsyncSpotOrchestratorImpl,
    PerpetualOrchestratorImpl,
    PublishStrategy,
    SpotOrchestratorImpl,
    _book_depth_redis_key,
    _candlestick_redis_key,
    _price_redis_key,
)
from app.db.models import BookDepthSnapshot, CandleStickSnapshot, CurrencyPairSnapshot

TEST_EXCHANGE = "test"
TEST_SYMBOL = "BTC/USDT"
# Symbol with no row in DB for "empty" test isolation
TEST_SYMBOL_EMPTY = "EMPTY/USDT"
# Symbol only for get_klines-from-DB test to avoid interference from other tests' CandleStickSnapshot rows
TEST_SYMBOL_KLINES_DB = "BTC/USDT_KLINES_DB"


def _redis_price_key(kind: str, symbol: str = TEST_SYMBOL) -> str:
    return _price_redis_key(TEST_EXCHANGE, kind, symbol)


def _redis_depth_key(kind: str, symbol: str = TEST_SYMBOL) -> str:
    return _book_depth_redis_key(TEST_EXCHANGE, kind, symbol)


def _redis_candlestick_key(kind: str, symbol: str = TEST_SYMBOL) -> str:
    return _candlestick_redis_key(TEST_EXCHANGE, kind, symbol)


def _sample_candle(
    utc_open_time: float = 60.0,
    open_price: float = 100.0,
    high_price: float = 101.0,
    low_price: float = 99.0,
    close_price: float = 100.5,
    coin_volume: float = 1.0,
    usd_volume: float | None = 50000.0,
) -> CandleStick:
    return CandleStick(
        utc_open_time=utc_open_time,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        coin_volume=coin_volume,
        usd_volume=usd_volume,
    )


def _sample_book_depth(symbol: str = TEST_SYMBOL, utc: float = 1000.0) -> BookDepth:
    return BookDepth(
        symbol=symbol,
        bids=[BidAsk(price=50000.0, quantity=0.5), BidAsk(price=49900.0, quantity=1.0)],
        asks=[BidAsk(price=50100.0, quantity=0.3), BidAsk(price=50200.0, quantity=2.0)],
        exchange_symbol="BTCUSDT",
        last_update_id="123",
        utc=utc,
    )


# ---------------------------------------------------------------------------
# Sync Retriever
# ---------------------------------------------------------------------------


class TestSpotOrchestratorImplRetriever:
    """Sync Spot retriever get_price: Redis first, then DB; warm Redis on DB load."""

    def test_get_price_from_redis(self, db_session, redis_client):
        """Если в Redis есть данные — get_price возвращает их."""
        orb = SpotOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
        )
        key = _redis_price_key("spot")
        redis_client.setex(
            key,
            60,
            json.dumps({"base": "BTC", "quote": "USDT", "ratio": 50000.5, "utc": 1000.0}),
        )
        try:
            pair = orb.get_price()
            assert pair is not None
            assert pair.base == "BTC"
            assert pair.quote == "USDT"
            assert pair.ratio == 50000.5
            assert pair.utc == 1000.0
        finally:
            redis_client.delete(key)

    def test_get_price_from_db_warms_redis(self, db_session, redis_client):
        """Если в Redis нет, грузит последнее из БД по макс id и прогревает Redis."""
        row = CurrencyPairSnapshot(
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=TEST_SYMBOL,
            base="BTC",
            quote="USDT",
            ratio=60000.0,
            utc=2000.0,
            align_to_minutes=1,
            aligned_timestamp=2000.0,
        )
        db_session.add(row)
        db_session.commit()
        key = _redis_price_key("spot")
        redis_client.delete(key)
        orb = SpotOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
        )
        pair = orb.get_price()
        assert pair is not None
        assert pair.base == "BTC"
        assert pair.ratio == 60000.0
        assert pair.utc == 2000.0
        raw = redis_client.get(key)
        assert raw is not None
        data = json.loads(raw)
        assert data["base"] == "BTC" and data["ratio"] == 60000.0
        redis_client.delete(key)

    def test_get_price_empty_returns_none(self, db_session, redis_client):
        """Нет в Redis и нет в БД — возвращает None."""
        symbol = TEST_SYMBOL_EMPTY
        key = _redis_price_key("spot", symbol)
        redis_client.delete(key)
        orb = SpotOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=symbol,
        )
        pair = orb.get_price()
        assert pair is None


class TestPerpetualOrchestratorImplRetriever:
    """Sync Perpetual retriever get_price: та же логика."""

    def test_get_price_from_redis(self, db_session, redis_client):
        key = _redis_price_key("perpetual")
        redis_client.setex(
            key,
            60,
            json.dumps({"base": "BTC", "quote": "USDT", "ratio": 50100.0, "utc": 3000.0}),
        )
        try:
            orb = PerpetualOrchestratorImpl(
                db_session=db_session,
                redis=redis_client,
                exchange_id=TEST_EXCHANGE,
                kind="perpetual",
                symbol=TEST_SYMBOL,
                cache_timeout=60,
            )
            pair = orb.get_price()
            assert pair is not None
            assert pair.ratio == 50100.0
            assert pair.utc == 3000.0
        finally:
            redis_client.delete(key)

    def test_get_price_from_db_warms_redis(self, db_session, redis_client):
        row = CurrencyPairSnapshot(
            exchange_id=TEST_EXCHANGE,
            kind="perpetual",
            symbol=TEST_SYMBOL,
            base="BTC",
            quote="USDT",
            ratio=61000.0,
            utc=4000.0,
            align_to_minutes=1,
            aligned_timestamp=4000.0,
        )
        db_session.add(row)
        db_session.commit()
        key = _redis_price_key("perpetual")
        redis_client.delete(key)
        orb = PerpetualOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="perpetual",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
        )
        pair = orb.get_price()
        assert pair is not None
        assert pair.ratio == 61000.0
        raw = redis_client.get(key)
        assert raw is not None
        redis_client.delete(key)


# ---------------------------------------------------------------------------
# Sync Book Depth (get_depth + publish_book_depth)
# ---------------------------------------------------------------------------


class TestSpotOrchestratorImplDepth:
    """Sync Spot get_depth: Redis first, then DB; publish_book_depth пишет в Redis и БД."""

    def test_get_depth_from_redis(self, db_session, redis_client):
        depth = _sample_book_depth(utc=1500.0)
        key = _redis_depth_key("spot")
        redis_client.setex(key, 60, json.dumps(depth.as_dict()))
        try:
            orb = SpotOrchestratorImpl(
                db_session=db_session,
                redis=redis_client,
                exchange_id=TEST_EXCHANGE,
                kind="spot",
                symbol=TEST_SYMBOL,
                cache_timeout=60,
            )
            out = orb.get_depth()
            assert out is not None
            assert out.symbol == TEST_SYMBOL
            assert out.utc == 1500.0
            assert len(out.bids) == 2 and out.bids[0].price == 50000.0
            assert len(out.asks) == 2 and out.asks[0].price == 50100.0
        finally:
            redis_client.delete(key)

    def test_get_depth_from_db_warms_redis(self, db_session, redis_client):
        row = BookDepthSnapshot(
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=TEST_SYMBOL,
            exchange_symbol="BTCUSDT",
            last_update_id="456",
            utc=2500.0,
            bids_asks={
                "bids": [{"price": 51000.0, "quantity": 0.1}],
                "asks": [{"price": 51100.0, "quantity": 0.2}],
            },
            align_to_minutes=1,
            aligned_timestamp=2500.0,
        )
        db_session.add(row)
        db_session.commit()
        key = _redis_depth_key("spot")
        redis_client.delete(key)
        orb = SpotOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
        )
        out = orb.get_depth()
        assert out is not None
        assert out.symbol == TEST_SYMBOL
        assert out.utc == 2500.0
        assert len(out.bids) == 1 and out.bids[0].price == 51000.0
        assert len(out.asks) == 1 and out.asks[0].price == 51100.0
        raw = redis_client.get(key)
        assert raw is not None
        redis_client.delete(key)

    def test_get_depth_empty_returns_none(self, db_session, redis_client):
        symbol = TEST_SYMBOL_EMPTY
        key = _redis_depth_key("spot", symbol)
        redis_client.delete(key)
        orb = SpotOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=symbol,
        )
        assert orb.get_depth() is None

    def test_publish_book_depth_then_get_depth(self, db_session, redis_client):
        orb = SpotOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
        )
        depth = _sample_book_depth(utc=3000.0)
        orb.publish_book_depth(depth)
        out = orb.get_depth()
        assert out is not None
        assert out.symbol == depth.symbol
        assert out.utc == 3000.0
        assert len(out.bids) == len(depth.bids) and len(out.asks) == len(depth.asks)
        key = _redis_depth_key("spot")
        redis_client.delete(key)


class TestPerpetualOrchestratorImplDepth:
    """Sync Perpetual get_depth и publish_book_depth — та же логика."""

    def test_get_depth_from_redis(self, db_session, redis_client):
        depth = _sample_book_depth(utc=3500.0)
        key = _redis_depth_key("perpetual")
        redis_client.setex(key, 60, json.dumps(depth.as_dict()))
        try:
            orb = PerpetualOrchestratorImpl(
                db_session=db_session,
                redis=redis_client,
                exchange_id=TEST_EXCHANGE,
                kind="perpetual",
                symbol=TEST_SYMBOL,
                cache_timeout=60,
            )
            out = orb.get_depth()
            assert out is not None
            assert out.utc == 3500.0
        finally:
            redis_client.delete(key)

    def test_get_depth_from_db_warms_redis(self, db_session, redis_client):
        row = BookDepthSnapshot(
            exchange_id=TEST_EXCHANGE,
            kind="perpetual",
            symbol=TEST_SYMBOL,
            utc=4500.0,
            bids_asks={"bids": [{"price": 52000.0, "quantity": 0.5}], "asks": [{"price": 52100.0, "quantity": 0.5}]},
            align_to_minutes=1,
            aligned_timestamp=4500.0,
        )
        db_session.add(row)
        db_session.commit()
        key = _redis_depth_key("perpetual")
        redis_client.delete(key)
        orb = PerpetualOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="perpetual",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
        )
        out = orb.get_depth()
        assert out is not None
        assert out.utc == 4500.0
        assert redis_client.get(key) is not None
        redis_client.delete(key)

    def test_get_depth_empty_returns_none(self, db_session, redis_client):
        symbol = TEST_SYMBOL_EMPTY
        key = _redis_depth_key("perpetual", symbol)
        redis_client.delete(key)
        orb = PerpetualOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="perpetual",
            symbol=symbol,
        )
        assert orb.get_depth() is None

    def test_publish_book_depth_then_get_depth(self, db_session, redis_client):
        orb = PerpetualOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="perpetual",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
        )
        depth = _sample_book_depth(utc=5000.0)
        orb.publish_book_depth(depth)
        out = orb.get_depth()
        assert out is not None
        assert out.utc == 5000.0
        key = _redis_depth_key("perpetual")
        redis_client.delete(key)


# ---------------------------------------------------------------------------
# Sync Candlestick (get_klines + publish_candlestick)
# ---------------------------------------------------------------------------


class TestSpotOrchestratorImplCandlestick:
    """Sync Spot get_klines и publish_candlestick: Redis список, при нехватке — БД, merge newer wins."""

    def test_get_klines_from_redis(self, db_session, redis_client):
        """Если в Redis есть список свечей — get_klines возвращает их (свежие первые)."""
        c1 = _sample_candle(utc_open_time=120.0, close_price=100.2)
        c2 = _sample_candle(utc_open_time=60.0, close_price=100.0)
        key = _redis_candlestick_key("spot")
        redis_client.setex(key, 60, json.dumps([c1.as_dict(), c2.as_dict()]))
        try:
            orb = SpotOrchestratorImpl(
                db_session=db_session,
                redis=redis_client,
                exchange_id=TEST_EXCHANGE,
                kind="spot",
                symbol=TEST_SYMBOL,
                cache_timeout=60,
                align_to_minutes=1,
            )
            out = orb.get_klines()
            assert out is not None
            assert len(out) == 2
            assert out[0].utc_open_time == 120.0 and out[0].close_price == 100.2
            assert out[1].utc_open_time == 60.0 and out[1].close_price == 100.0
        finally:
            redis_client.delete(key)

    def test_get_klines_empty_returns_none(self, db_session, redis_client):
        """Нет в Redis и нет в БД — возвращает None."""
        key = _redis_candlestick_key("spot")
        redis_client.delete(key)
        orb = SpotOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=TEST_SYMBOL,
            align_to_minutes=1,
        )
        assert orb.get_klines() is None

    def test_get_klines_from_db_when_redis_has_less_than_limit(self, db_session, redis_client):
        """Если в Redis меньше свечей чем limit — дозапрос из БД, merge, сортировка (свежие первые)."""
        symbol = TEST_SYMBOL_KLINES_DB
        row1 = CandleStickSnapshot(
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=symbol,
            span_in_minutes=1,
            utc_open_time=180.0,
            open_price=101.0,
            high_price=102.0,
            low_price=100.0,
            close_price=101.5,
            coin_volume=2.0,
            usd_volume=203000.0,
            utc=180.0,
            align_to_minutes=1,
            aligned_timestamp=180.0,
        )
        row2 = CandleStickSnapshot(
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=symbol,
            span_in_minutes=1,
            utc_open_time=120.0,
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.5,
            coin_volume=1.0,
            usd_volume=100500.0,
            utc=120.0,
            align_to_minutes=1,
            aligned_timestamp=120.0,
        )
        db_session.add(row1)
        db_session.add(row2)
        db_session.commit()
        key = _redis_candlestick_key("spot", symbol)
        redis_client.delete(key)
        orb = SpotOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=symbol,
            cache_timeout=60,
            align_to_minutes=1,
        )
        out = orb.get_klines(limit=5)
        assert out is not None
        assert len(out) == 2
        assert out[0].utc_open_time == 180.0 and out[0].close_price == 101.5
        assert out[1].utc_open_time == 120.0 and out[1].close_price == 100.5
        redis_client.delete(key)

    def test_publish_candlestick_then_get_klines(self, db_session, redis_client):
        """publish_candlestick пишет в Redis и БД; get_klines возвращает список (свежие первые)."""
        orb = SpotOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
            align_to_minutes=1,
        )
        c1 = _sample_candle(utc_open_time=60.0, close_price=100.0)
        c2 = _sample_candle(utc_open_time=120.0, close_price=100.5)
        orb.publish_candlestick([c1, c2])
        out = orb.get_klines()
        assert out is not None
        assert len(out) == 2
        assert out[0].utc_open_time == 120.0 and out[0].close_price == 100.5
        assert out[1].utc_open_time == 60.0 and out[1].close_price == 100.0
        key = _redis_candlestick_key("spot")
        raw = redis_client.get(key)
        assert raw is not None
        data = json.loads(raw)
        assert len(data) == 2
        redis_client.delete(key)

    def test_get_klines_respects_limit(self, db_session, redis_client):
        """get_klines(limit=N) возвращает не более N свечей."""
        candles = [
            _sample_candle(utc_open_time=60.0 * i, close_price=100.0 + i * 0.1)
            for i in range(1, 6)
        ]
        key = _redis_candlestick_key("spot")
        redis_client.setex(key, 60, json.dumps([c.as_dict() for c in candles]))
        try:
            orb = SpotOrchestratorImpl(
                db_session=db_session,
                redis=redis_client,
                exchange_id=TEST_EXCHANGE,
                kind="spot",
                symbol=TEST_SYMBOL,
                cache_timeout=60,
                align_to_minutes=1,
            )
            out = orb.get_klines(limit=3)
            assert out is not None
            assert len(out) == 3
            assert out[0].utc_open_time == 300.0
        finally:
            redis_client.delete(key)


class TestPerpetualOrchestratorImplCandlestick:
    """Sync Perpetual get_klines и publish_candlestick — та же логика."""

    def test_get_klines_from_redis(self, db_session, redis_client):
        c = _sample_candle(utc_open_time=240.0, close_price=50100.0)
        key = _redis_candlestick_key("perpetual")
        redis_client.setex(key, 60, json.dumps([c.as_dict()]))
        try:
            orb = PerpetualOrchestratorImpl(
                db_session=db_session,
                redis=redis_client,
                exchange_id=TEST_EXCHANGE,
                kind="perpetual",
                symbol=TEST_SYMBOL,
                cache_timeout=60,
                align_to_minutes=1,
            )
            out = orb.get_klines()
            assert out is not None
            assert len(out) == 1
            assert out[0].utc_open_time == 240.0 and out[0].close_price == 50100.0
        finally:
            redis_client.delete(key)

    def test_publish_candlestick_then_get_klines(self, db_session, redis_client):
        orb = PerpetualOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="perpetual",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
            align_to_minutes=1,
        )
        c = _sample_candle(utc_open_time=300.0, close_price=50200.0)
        orb.publish_candlestick(c)
        out = orb.get_klines()
        assert out is not None
        assert len(out) == 1
        assert out[0].utc_open_time == 300.0 and out[0].close_price == 50200.0
        key = _redis_candlestick_key("perpetual")
        redis_client.delete(key)

    def test_publish_candlestick_merge_strategy(self, db_session, redis_client):
        """MERGE: входящие свечи объединяются с текущими из Redis, более свежая побеждает."""
        orb = PerpetualOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="perpetual",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
            align_to_minutes=1,
        )
        existing = _sample_candle(utc_open_time=60.0, close_price=50000.0)
        orb.publish_candlestick(existing)
        updated = _sample_candle(utc_open_time=60.0, close_price=50050.0)
        orb.publish_candlestick(updated, strategy=PublishStrategy.MERGE)
        out = orb.get_klines()
        assert out is not None
        assert len(out) == 1
        assert out[0].close_price == 50050.0
        key = _redis_candlestick_key("perpetual")
        redis_client.delete(key)

    def test_publish_candlestick_replace_raises(self, db_session, redis_client):
        """PublishStrategy.REPLACE для publish_candlestick поднимает ValueError."""
        orb = PerpetualOrchestratorImpl(
            db_session=db_session,
            redis=redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="perpetual",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
            align_to_minutes=1,
        )
        c = _sample_candle(utc_open_time=60.0, close_price=50000.0)
        with pytest.raises(ValueError, match="REPLACE is not supported for publish_candlestick"):
            orb.publish_candlestick(c, strategy=PublishStrategy.REPLACE)


# ---------------------------------------------------------------------------
# Async Retriever (standalone functions so async fixtures resolve correctly)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_spot_get_price_from_redis(async_db_session, async_redis_client):
    """Async Spot: если в Redis есть данные — get_price возвращает их."""
    key = _redis_price_key("spot")
    await async_redis_client.set(key, json.dumps({
        "base": "BTC", "quote": "USDT", "ratio": 50200.0, "utc": 5000.0
    }), ex=60)
    try:
        orb = AsyncSpotOrchestratorImpl(
            db_session=async_db_session,
            redis=async_redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
        )
        pair = await orb.get_price()
        assert pair is not None
        assert pair.ratio == 50200.0
        assert pair.utc == 5000.0
    finally:
        await async_redis_client.delete(key)


@pytest.mark.asyncio
async def test_async_spot_get_price_from_db_warms_redis(async_db_session, async_redis_client):
    """Async Spot: грузит из БД и прогревает Redis."""
    row = CurrencyPairSnapshot(
        exchange_id=TEST_EXCHANGE,
        kind="spot",
        symbol=TEST_SYMBOL,
        base="BTC",
        quote="USDT",
        ratio=62000.0,
        utc=6000.0,
        align_to_minutes=1,
        aligned_timestamp=6000.0,
    )
    async_db_session.add(row)
    await async_db_session.commit()
    key = _redis_price_key("spot")
    await async_redis_client.delete(key)
    orb = AsyncSpotOrchestratorImpl(
        db_session=async_db_session,
        redis=async_redis_client,
        exchange_id=TEST_EXCHANGE,
        kind="spot",
        symbol=TEST_SYMBOL,
        cache_timeout=60,
    )
    pair = await orb.get_price()
    assert pair is not None
    assert pair.ratio == 62000.0
    raw = await async_redis_client.get(key)
    assert raw is not None
    await async_redis_client.delete(key)


@pytest.mark.asyncio
async def test_async_spot_get_price_empty_returns_none(async_db_session, async_redis_client):
    """Async Spot: нет в Redis и БД — None."""
    symbol = TEST_SYMBOL_EMPTY
    key = _redis_price_key("spot", symbol)
    await async_redis_client.delete(key)
    orb = AsyncSpotOrchestratorImpl(
        db_session=async_db_session,
        redis=async_redis_client,
        exchange_id=TEST_EXCHANGE,
        kind="spot",
        symbol=symbol,
    )
    pair = await orb.get_price()
    assert pair is None


@pytest.mark.asyncio
async def test_async_perpetual_get_price_from_redis(async_db_session, async_redis_client):
    """Async Perpetual: данные из Redis."""
    key = _redis_price_key("perpetual")
    await async_redis_client.set(key, json.dumps({
        "base": "BTC", "quote": "USDT", "ratio": 50300.0, "utc": 7000.0
    }), ex=60)
    try:
        orb = AsyncPerpetualOrchestratorImpl(
            db_session=async_db_session,
            redis=async_redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="perpetual",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
        )
        pair = await orb.get_price()
        assert pair is not None
        assert pair.ratio == 50300.0
    finally:
        await async_redis_client.delete(key)


@pytest.mark.asyncio
async def test_async_perpetual_get_price_from_db_warms_redis(async_db_session, async_redis_client):
    """Async Perpetual: грузит из БД и прогревает Redis."""
    row = CurrencyPairSnapshot(
        exchange_id=TEST_EXCHANGE,
        kind="perpetual",
        symbol=TEST_SYMBOL,
        base="BTC",
        quote="USDT",
        ratio=63000.0,
        utc=8000.0,
        align_to_minutes=1,
        aligned_timestamp=8000.0,
    )
    async_db_session.add(row)
    await async_db_session.commit()
    key = _redis_price_key("perpetual")
    await async_redis_client.delete(key)
    orb = AsyncPerpetualOrchestratorImpl(
        db_session=async_db_session,
        redis=async_redis_client,
        exchange_id=TEST_EXCHANGE,
        kind="perpetual",
        symbol=TEST_SYMBOL,
        cache_timeout=60,
    )
    pair = await orb.get_price()
    assert pair is not None
    assert pair.ratio == 63000.0
    raw = await async_redis_client.get(key)
    assert raw is not None
    await async_redis_client.delete(key)


# ---------------------------------------------------------------------------
# Async Book Depth (get_depth)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_spot_get_depth_from_redis(async_db_session, async_redis_client):
    depth = _sample_book_depth(utc=5500.0)
    key = _redis_depth_key("spot")
    await async_redis_client.set(key, json.dumps(depth.as_dict()), ex=60)
    try:
        orb = AsyncSpotOrchestratorImpl(
            db_session=async_db_session,
            redis=async_redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
        )
        out = await orb.get_depth()
        assert out is not None
        assert out.utc == 5500.0
        assert len(out.bids) == 2
    finally:
        await async_redis_client.delete(key)


@pytest.mark.asyncio
async def test_async_spot_get_depth_from_db_warms_redis(async_db_session, async_redis_client):
    row = BookDepthSnapshot(
        exchange_id=TEST_EXCHANGE,
        kind="spot",
        symbol=TEST_SYMBOL,
        utc=6500.0,
        bids_asks={"bids": [{"price": 53000.0, "quantity": 0.2}], "asks": [{"price": 53100.0, "quantity": 0.3}]},
        align_to_minutes=1,
        aligned_timestamp=6500.0,
    )
    async_db_session.add(row)
    await async_db_session.commit()
    key = _redis_depth_key("spot")
    await async_redis_client.delete(key)
    orb = AsyncSpotOrchestratorImpl(
        db_session=async_db_session,
        redis=async_redis_client,
        exchange_id=TEST_EXCHANGE,
        kind="spot",
        symbol=TEST_SYMBOL,
        cache_timeout=60,
    )
    out = await orb.get_depth()
    assert out is not None
    assert out.utc == 6500.0
    raw = await async_redis_client.get(key)
    assert raw is not None
    await async_redis_client.delete(key)


@pytest.mark.asyncio
async def test_async_spot_get_depth_empty_returns_none(async_db_session, async_redis_client):
    symbol = TEST_SYMBOL_EMPTY
    key = _redis_depth_key("spot", symbol)
    await async_redis_client.delete(key)
    orb = AsyncSpotOrchestratorImpl(
        db_session=async_db_session,
        redis=async_redis_client,
        exchange_id=TEST_EXCHANGE,
        kind="spot",
        symbol=symbol,
    )
    assert await orb.get_depth() is None


@pytest.mark.asyncio
async def test_async_perpetual_get_depth_from_redis(async_db_session, async_redis_client):
    depth = _sample_book_depth(utc=7500.0)
    key = _redis_depth_key("perpetual")
    await async_redis_client.set(key, json.dumps(depth.as_dict()), ex=60)
    try:
        orb = AsyncPerpetualOrchestratorImpl(
            db_session=async_db_session,
            redis=async_redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="perpetual",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
        )
        out = await orb.get_depth()
        assert out is not None
        assert out.utc == 7500.0
    finally:
        await async_redis_client.delete(key)


@pytest.mark.asyncio
async def test_async_perpetual_get_depth_from_db_warms_redis(async_db_session, async_redis_client):
    row = BookDepthSnapshot(
        exchange_id=TEST_EXCHANGE,
        kind="perpetual",
        symbol=TEST_SYMBOL,
        utc=8500.0,
        bids_asks={"bids": [{"price": 54000.0, "quantity": 0.1}], "asks": [{"price": 54100.0, "quantity": 0.1}]},
        align_to_minutes=1,
        aligned_timestamp=8500.0,
    )
    async_db_session.add(row)
    await async_db_session.commit()
    key = _redis_depth_key("perpetual")
    await async_redis_client.delete(key)
    orb = AsyncPerpetualOrchestratorImpl(
        db_session=async_db_session,
        redis=async_redis_client,
        exchange_id=TEST_EXCHANGE,
        kind="perpetual",
        symbol=TEST_SYMBOL,
        cache_timeout=60,
    )
    out = await orb.get_depth()
    assert out is not None
    assert out.utc == 8500.0
    raw = await async_redis_client.get(key)
    assert raw is not None
    await async_redis_client.delete(key)


# ---------------------------------------------------------------------------
# Async Candlestick (get_klines only; publish_candlestick только в sync)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_spot_get_klines_from_redis(async_db_session, async_redis_client):
    """Async Spot: get_klines возвращает данные из Redis (свежие первые)."""
    c = _sample_candle(utc_open_time=180.0, close_price=100.3)
    key = _redis_candlestick_key("spot")
    await async_redis_client.set(key, json.dumps([c.as_dict()]), ex=60)
    try:
        orb = AsyncSpotOrchestratorImpl(
            db_session=async_db_session,
            redis=async_redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="spot",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
            align_to_minutes=1,
        )
        out = await orb.get_klines()
        assert out is not None
        assert len(out) == 1
        assert out[0].utc_open_time == 180.0 and out[0].close_price == 100.3
    finally:
        await async_redis_client.delete(key)


@pytest.mark.asyncio
async def test_async_spot_get_klines_empty_returns_none(async_db_session, async_redis_client):
    key = _redis_candlestick_key("spot")
    await async_redis_client.delete(key)
    orb = AsyncSpotOrchestratorImpl(
        db_session=async_db_session,
        redis=async_redis_client,
        exchange_id=TEST_EXCHANGE,
        kind="spot",
        symbol=TEST_SYMBOL,
        align_to_minutes=1,
    )
    assert await orb.get_klines() is None


@pytest.mark.asyncio
async def test_async_perpetual_get_klines_from_redis(async_db_session, async_redis_client):
    c = _sample_candle(utc_open_time=360.0, close_price=50300.0)
    key = _redis_candlestick_key("perpetual")
    await async_redis_client.set(key, json.dumps([c.as_dict()]), ex=60)
    try:
        orb = AsyncPerpetualOrchestratorImpl(
            db_session=async_db_session,
            redis=async_redis_client,
            exchange_id=TEST_EXCHANGE,
            kind="perpetual",
            symbol=TEST_SYMBOL,
            cache_timeout=60,
            align_to_minutes=1,
        )
        out = await orb.get_klines()
        assert out is not None
        assert len(out) == 1
        assert out[0].close_price == 50300.0
    finally:
        await async_redis_client.delete(key)


@pytest.mark.asyncio
async def test_async_perpetual_get_klines_from_db_when_redis_has_less_than_limit(
    async_db_session, async_redis_client
):
    """Async Perpetual: при нехватке данных в Redis дозапрос из БД, merge, сортировка."""
    row = CandleStickSnapshot(
        exchange_id=TEST_EXCHANGE,
        kind="perpetual",
        symbol=TEST_SYMBOL,
        span_in_minutes=1,
        utc_open_time=420.0,
        open_price=50400.0,
        high_price=50500.0,
        low_price=50300.0,
        close_price=50450.0,
        coin_volume=3.0,
        usd_volume=151350.0,
        utc=420.0,
        align_to_minutes=1,
        aligned_timestamp=420.0,
    )
    async_db_session.add(row)
    await async_db_session.commit()
    key = _redis_candlestick_key("perpetual")
    await async_redis_client.delete(key)
    orb = AsyncPerpetualOrchestratorImpl(
        db_session=async_db_session,
        redis=async_redis_client,
        exchange_id=TEST_EXCHANGE,
        kind="perpetual",
        symbol=TEST_SYMBOL,
        cache_timeout=60,
        align_to_minutes=1,
    )
    out = await orb.get_klines(limit=5)
    assert out is not None
    assert len(out) >= 1
    assert out[0].utc_open_time == 420.0 and out[0].close_price == 50450.0
    await async_redis_client.delete(key)
