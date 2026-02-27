"""SQLAlchemy models for crawler: Token, CrawlerJob, CrawlerIteration."""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from sqlalchemy.dialects.postgresql import JSONB

from app.db import Base


class Token(Base):
    """Токены, по которым будет идти обход. Уникальность по паре (symbol, source)."""

    __tablename__ = "token"
    __table_args__ = (UniqueConstraint("symbol", "source", name="uq_token_symbol_source"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    source = Column(String, nullable=False)  # "coinmarketcap" | "manual"
    created_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)


class CrawlerJob(Base):
    """Один проход скрипта обхода по бирже (exchange + connector)."""

    __tablename__ = "crawler_job"
    __table_args__ = (
        Index("ix_crawler_job_exchange_connector", "exchange", "connector"),
        Index("ix_crawler_job_start", "start"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String, nullable=False)  # binance | bybit | okx | ...
    connector = Column(String, nullable=False)  # spot | perpetual
    start = Column(DateTime(timezone=True), nullable=False)
    stop = Column(DateTime(timezone=True), nullable=True)
    error = Column(Text, nullable=True)

    iterations = relationship(
        "CrawlerIteration",
        back_populates="crawler_job",
        cascade="all, delete-orphan",
    )


class CrawlerIteration(Base):
    """Одна итерация по токену в рамках CrawlerJob."""

    __tablename__ = "crawler_iteration"
    __table_args__ = (
        Index("ix_crawler_iteration_crawler_job_id", "crawler_job_id"),
        Index("ix_crawler_iteration_job_token", "crawler_job_id", "token"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    crawler_job_id = Column(
        Integer,
        ForeignKey("crawler_job.id", ondelete="CASCADE"),
        nullable=False,
    )
    token = Column(String, nullable=False)
    start = Column(DateTime(timezone=True), nullable=False)
    stop = Column(DateTime(timezone=True), nullable=True)
    done = Column(Boolean, default=False, nullable=False)
    status = Column(String, nullable=False)  # init | pending | success | error | ignore
    comment = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    last_update = Column(DateTime(timezone=True), nullable=False)

    # JSON columns for crawler result data (DTO serialization from app.cex.dto)
    currency_pair = Column(JSONB, nullable=True)  # CurrencyPair
    book_depth = Column(JSONB, nullable=True)  # BookDepth
    klines = Column(JSONB, nullable=True)  # list[CandleStick]
    funding_rate = Column(JSONB, nullable=True)  # FundingRate (perpetual)
    next_funding_rate = Column(JSONB, nullable=True)  # next funding (perpetual)
    funding_rate_history = Column(JSONB, nullable=True)  # list[FundingRatePoint] (perpetual)

    crawler_job = relationship("CrawlerJob", back_populates="iterations")
