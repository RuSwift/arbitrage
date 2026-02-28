"""Tests for sync and async orchestrator retrievers (get_price, get_depth: Redis then DB, warm Redis)."""

import json

import pytest

from app.cex.dto import BidAsk, BookDepth
from app.cex.orcestrator import (
    AsyncPerpetualOrchestratorImpl,
    AsyncSpotOrchestratorImpl,
    PerpetualOrchestratorImpl,
    SpotOrchestratorImpl,
    _book_depth_redis_key,
    _price_redis_key,
)
from app.db.models import BookDepthSnapshot, CurrencyPairSnapshot

TEST_EXCHANGE = "test"
TEST_SYMBOL = "BTC/USDT"
# Symbol with no row in DB for "empty" test isolation
TEST_SYMBOL_EMPTY = "EMPTY/USDT"


def _redis_price_key(kind: str, symbol: str = TEST_SYMBOL) -> str:
    return _price_redis_key(TEST_EXCHANGE, kind, symbol)


def _redis_depth_key(kind: str, symbol: str = TEST_SYMBOL) -> str:
    return _book_depth_redis_key(TEST_EXCHANGE, kind, symbol)


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
