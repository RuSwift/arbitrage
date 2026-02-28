"""SQLAlchemy Base и модели для Alembic autogenerate."""

from sqlalchemy.orm import declarative_base

Base = declarative_base()

from app.db.models import (  # noqa: E402
    BookDepthSnapshot,
    CandleStickSnapshot,
    CrawlerIteration,
    CrawlerJob,
    CurrencyPairSnapshot,
    Token,
)

__all__ = [
    "Base",
    "Token",
    "CrawlerJob",
    "CrawlerIteration",
    "CurrencyPairSnapshot",
    "BookDepthSnapshot",
    "CandleStickSnapshot",
]
