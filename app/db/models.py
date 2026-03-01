"""SQLAlchemy models for crawler: Token, CrawlerJob, CrawlerIteration."""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
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
    # UTC
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
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


# ---------------------------------------------------------------------------
# CEX snapshots (exchange_id + kind on all)
# ---------------------------------------------------------------------------


class CurrencyPairSnapshot(Base):
    """Цена пары (spot или perpetual)."""

    __tablename__ = "currency_pair_snapshot"
    __table_args__ = (
        Index("ix_currency_pair_snapshot_exchange_kind", "exchange_id", "kind"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(String, nullable=False)  # binance | bybit | okx | ...
    kind = Column(String, nullable=False)  # spot | perpetual
    symbol = Column(String, nullable=False)  # e.g. BTC/USDT
    base = Column(String, nullable=False)
    quote = Column(String, nullable=False)
    ratio = Column(Float, nullable=False)
    utc = Column(Float, nullable=True)  # реальное время события, Publisher обновляет из события
    align_to_minutes = Column(Integer, nullable=False)
    aligned_timestamp = Column(Float, nullable=False)  # выровненный utc для поиска/обновления


class BookDepthSnapshot(Base):
    """Снимок стакана (bids/asks в JSONB)."""

    __tablename__ = "book_depth_snapshot"
    __table_args__ = (
        Index("ix_book_depth_snapshot_exchange_kind", "exchange_id", "kind"),
        Index("ix_book_depth_snapshot_symbol", "exchange_id", "kind", "symbol"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(String, nullable=False)
    kind = Column(String, nullable=False)  # spot | perpetual
    symbol = Column(String, nullable=False)
    exchange_symbol = Column(String, nullable=True)
    last_update_id = Column(Text, nullable=True)
    utc = Column(Float, nullable=True)  # реальное время события (Publisher обновляет из события)
    bids_asks = Column(JSONB, nullable=False)  # list of {price, quantity} for bids and asks
    align_to_minutes = Column(Integer, nullable=False)
    aligned_timestamp = Column(Float, nullable=False)  # выровненный utc для поиска/обновления


class CandleStickSnapshot(Base):
    """Свеча (OHLCV)."""

    __tablename__ = "candle_stick_snapshot"
    __table_args__ = (
        Index("ix_candle_stick_snapshot_exchange_kind", "exchange_id", "kind"),
        Index("ix_candle_stick_snapshot_symbol", "exchange_id", "kind", "symbol"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(String, nullable=False)
    kind = Column(String, nullable=False)  # spot | perpetual
    symbol = Column(String, nullable=False)
    span_in_minutes = Column(Integer, nullable=False, server_default=text("1"))
    utc_open_time = Column(Float, nullable=False)
    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    coin_volume = Column(Float, nullable=False)
    usd_volume = Column(Float, nullable=True)
    utc = Column(Float, nullable=True)  # реальное время события (Publisher обновляет из события)
    align_to_minutes = Column(Integer, nullable=False)
    aligned_timestamp = Column(Float, nullable=False)  # выровненный utc для поиска/обновления


# ---------------------------------------------------------------------------
# Service configuration
# ---------------------------------------------------------------------------


class ServiceConfig(Base):
    """Конфигурация сервиса. Одна запись на service_name."""

    __tablename__ = "service_config"
    __table_args__ = (UniqueConstraint("service_name", name="uq_service_config_service_name"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    service_name = Column(String, nullable=False)
    config = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    updated_at = Column(DateTime(timezone=True), nullable=True, onupdate=text("now()"))
