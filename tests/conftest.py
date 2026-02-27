"""
Pytest fixtures for tests. Redis and Postgres connection parameters from .env.
For throttle tests, Redis must be available (e.g. docker compose up -d).
"""
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

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
    """Redis client from .env. Skips test if Redis is unavailable."""
    try:
        import redis
        client = redis.from_url(redis_url)
        client.ping()
        yield client
        client.close()
    except Exception as e:
        pytest.skip(f"Redis unavailable: {e}")


THROTTLE_TEST_PREFIX = "arbitrage:throttle:test"


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
