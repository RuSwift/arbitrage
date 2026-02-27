"""SQLAlchemy Base и модели для Alembic autogenerate."""

from sqlalchemy.orm import declarative_base

Base = declarative_base()

from app.db.models import CrawlerIteration, CrawlerJob, Token  # noqa: E402

__all__ = ["Base", "Token", "CrawlerJob", "CrawlerIteration"]
