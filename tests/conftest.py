"""
Pytest fixtures for tests. Redis and Postgres connection parameters from .env.
For throttle tests, Redis must be available (e.g. docker compose up -d).
"""
import sys
from pathlib import Path

import pytest
import pytest_asyncio
from dotenv import load_dotenv

# Enable async tests (required for TestAsync*OrchestratorImplRetriever)
pytest_plugins = ("pytest_asyncio",)

# Project root and .env
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
_env_file = _project_root / ".env"
if _env_file.exists():
    load_dotenv(_env_file, override=False)


@pytest.fixture(scope="session")
def redis_url():
    """Redis URL from .env (REDIS_*). Load only RedisSettings so tests run without COINMARKETCAP_API_KEY."""
    from app.settings import RedisSettings
    return RedisSettings().url


@pytest.fixture(scope="session")
def database_url():
    """PostgreSQL URL from .env (DB_*) for future DB tests."""
    from app.settings import DatabaseSettings
    return DatabaseSettings().url


@pytest.fixture
def redis_client(redis_url):
    """Redis client from .env."""
    import redis
    client = redis.from_url(redis_url)
    client.ping()
    yield client
    client.close()


@pytest.fixture(scope="session")
def async_database_url():
    """Async PostgreSQL URL from .env (postgresql+asyncpg)."""
    from app.settings import Settings
    return Settings().database.async_url


@pytest.fixture
def db_session(database_url):
    """Sync DB session for orchestrator tests. Rollback on teardown."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    engine = create_engine(database_url)
    factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    session = factory()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest_asyncio.fixture
async def async_db_session(async_database_url):
    """Async DB session for orchestrator tests. Rollback on teardown."""
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
    engine = create_async_engine(async_database_url)
    factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        autocommit=False,
        autoflush=False,
    )
    async with factory() as session:
        try:
            yield session
        finally:
            await session.rollback()


@pytest_asyncio.fixture
async def async_redis_client(redis_url):
    """Async Redis client from .env."""
    from redis.asyncio import from_url
    client = from_url(redis_url)
    await client.ping()
    try:
        yield client
    finally:
        await client.aclose()


THROTTLE_TEST_PREFIX = "arbitrage:throttle:test"
ORCHESTRATOR_TEST_PREFIX = "arbitrage:orchestrator:price:test:"


@pytest.fixture
def throttler(redis_url, redis_client):
    """
    Throttler using Redis with test key prefix. Keys with this prefix
    are cleared before and after the test.
    """
    from app.cex.throttler import Throttler
    # Clear test keys before
    keys = redis_client.keys(f"{THROTTLE_TEST_PREFIX}:*")
    if keys:
        redis_client.delete(*keys)
    t = Throttler(timeout=1.0, redis_url=redis_url, key_prefix=THROTTLE_TEST_PREFIX)
    yield t
    # Clear test keys after
    keys = redis_client.keys(f"{THROTTLE_TEST_PREFIX}:*")
    if keys:
        redis_client.delete(*keys)
