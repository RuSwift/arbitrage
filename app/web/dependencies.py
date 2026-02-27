"""Dependencies for web app: DB session and Redis client."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.settings import Settings

_engine: Any = None
_SessionLocal: Any = None


def _get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            Settings().database.url,
            echo=Settings().database.echo,
            pool_size=Settings().database.pool_size,
            max_overflow=Settings().database.max_overflow,
        )
    return _engine


def _get_session_factory():
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=_get_engine(),
            autocommit=False,
            autoflush=False,
        )
    return _SessionLocal


def get_db() -> Generator[Session, None, None]:
    """Yield a DB session for FastAPI dependency injection."""
    SessionLocal = _get_session_factory()
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


_redis_client: Any = None


def get_redis():
    """Return a Redis client (lazy). Uses Settings().redis.url."""
    global _redis_client
    if _redis_client is None:
        import redis
        _redis_client = redis.from_url(Settings().redis.url)
    return _redis_client
