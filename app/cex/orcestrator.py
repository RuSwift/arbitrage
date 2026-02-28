import json
import math
import time
from typing import TYPE_CHECKING, Protocol

from app.cex.dto import (
    BookDepth,
    CandleStick,
    CurrencyPair,
    FundingRate,
    FundingRatePoint,
    WithdrawInfo,
)

if TYPE_CHECKING:
    from redis import Redis
    from redis.asyncio import Redis as AsyncRedis
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session

from sqlalchemy import select

from app.db.models import CurrencyPairSnapshot
from enum import Enum

class PublishStrategy(Enum):
    REPLACE = "REPLACE"
    MERGE = "MERGE"


class SpotPublisher(Protocol):
    """
    Протокол Publisher (структурная типизация).
    Предназначен для публикации данных Ticker, BookDepth, CandleStick, WithdrawInfo.
    """

    def publish_price(self, ticker: CurrencyPair) -> None:
        """Публикует цену."""
        ...

    def publish_book_depth(
        self, book_depth: BookDepth, strategy: PublishStrategy = PublishStrategy.REPLACE
    ) -> None:
        """Публикует объект BookDepth."""
        ...

    def publish_candlestick(
        self, candlestick: CandleStick | list[CandleStick], strategy: PublishStrategy = PublishStrategy.MERGE
    ) -> None:
        """Публикует объект CandleStick."""
        ...

    def publish_withdraw_info(self, withdraw_infos: dict[str, list[WithdrawInfo]]) -> None:
        """Публикует объект WithdrawInfo."""
        ...


class SpotRetriever(Protocol):
    """
    Протокол чтения spot-данных (структурная типизация).
    Позволяет получать цену, стакан, свечи, информацию о выводе.
    """

    def get_price(self) -> CurrencyPair | None:
        """Возвращает цену по коду пары."""
        ...

    def get_depth(self, limit: int = 100) -> BookDepth | None:
        """Возвращает стакан по символу."""
        ...

    def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        """Возвращает свечи по символу."""
        ...

    def get_withdraw_info(self) -> dict[str, list[WithdrawInfo]] | None:
        """Возвращает информацию о выводе."""
        ...


class AsyncSpotRetriever(Protocol):
    """
    Асинхронный протокол чтения spot-данных.
    Те же методы, что SpotRetriever, но async.
    """

    async def get_price(self) -> CurrencyPair | None:
        """Возвращает цену по коду пары."""
        ...

    async def get_depth(self, limit: int = 100) -> BookDepth | None:
        """Возвращает стакан по символу."""
        ...

    async def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        """Возвращает свечи по символу."""
        ...

    async def get_withdraw_info(self) -> dict[str, list[WithdrawInfo]] | None:
        """Возвращает информацию о выводе."""
        ...


class PerpetualPublisher(Protocol):
    """
    Протокол публикации perpetual-данных (структурная типизация).
    Цена, стакан, свечи, FundingRate, история фандинга.
    """

    def publish_price(self, ticker: CurrencyPair) -> None:
        """Публикует цену."""
        ...

    def publish_book_depth(
        self, book_depth: BookDepth, strategy: PublishStrategy = PublishStrategy.REPLACE
    ) -> None:
        """Публикует объект BookDepth."""
        ...

    def publish_candlestick(
        self, candlestick: CandleStick | list[CandleStick], strategy: PublishStrategy = PublishStrategy.MERGE
    ) -> None:
        """Публикует объект CandleStick."""
        ...

    def publish_funding_rate(self, funding_rate: FundingRate) -> None:
        """Публикует текущий funding rate."""
        ...

    def publish_funding_history(
        self, symbol: str, history: list[FundingRatePoint]
    ) -> None:
        """Публикует историю funding rate по символу."""
        ...


class PerpetualRetriever(Protocol):
    """
    Протокол чтения perpetual-данных (структурная типизация).
    Цена, стакан, свечи, funding rate, история фандинга.
    """

    def get_price(self) -> CurrencyPair | None:
        """Возвращает цену по символу."""
        ...

    def get_depth(self, limit: int = 100) -> BookDepth | None:
        """Возвращает стакан по символу."""
        ...

    def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        """Возвращает свечи по символу."""
        ...

    def get_funding_rate(self) -> FundingRate | None:
        """Возвращает текущий funding rate по символу."""
        ...

    def get_funding_rate_history(
        self, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        """Возвращает историю funding rate по символу."""
        ...


class AsyncPerpetualRetriever(Protocol):
    """
    Асинхронный протокол чтения perpetual-данных.
    Те же методы, что PerpetualRetriever, но async.
    """

    async def get_price(self) -> CurrencyPair | None:
        """Возвращает цену по символу."""
        ...

    async def get_depth(self, limit: int = 100) -> BookDepth | None:
        """Возвращает стакан по символу."""
        ...

    async def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        """Возвращает свечи по символу."""
        ...

    async def get_funding_rate(self) -> FundingRate | None:
        """Возвращает текущий funding rate по символу."""
        ...

    async def get_funding_rate_history(
        self, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        """Возвращает историю funding rate по символу."""
        ...


# ---------------------------------------------------------------------------
# Orchestrator implementations (Publisher + Retriever for each kind)
# ---------------------------------------------------------------------------


def _align_utc(utc: float | None, align_to_minutes: int) -> float | None:
    """Выравнивает utc к границе align_to_minutes минут (отбрасывает секунды)."""
    if utc is None:
        return None
    interval_sec = align_to_minutes * 60
    return math.floor(utc / interval_sec) * interval_sec


def _price_redis_key(exchange_id: str, kind: str, symbol: str) -> str:
    return f"arbitrage:orchestrator:price:{exchange_id}:{kind}:{symbol}"


def _parse_price_from_redis(raw: bytes | str | None) -> CurrencyPair | None:
    """Парсит значение из Redis (JSON) в CurrencyPair. Возвращает None если raw пусто или невалидно."""
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        data = json.loads(raw)
        return CurrencyPair(
            base=data["base"],
            quote=data["quote"],
            ratio=float(data["ratio"]),
            utc=data.get("utc"),
        )
    except (KeyError, TypeError, ValueError):
        return None


class SpotOrchestratorImpl:
    """
    Реализация оркестратора для spot: SpotPublisher + SpotRetriever.
    """

    def __init__(
        self,
        db_session: "Session",
        redis: "Redis",
        exchange_id: str,
        kind: str,
        symbol: str,
        cache_timeout: float = 15,
        align_to_minutes: int = 1,  # выравнивание timestamp до N минут
    ) -> None:
        self._db_session = db_session
        self._redis = redis
        self._exchange_id = exchange_id
        self._kind = kind
        self._symbol = symbol
        self._cache_timeout = cache_timeout
        self._align_to_minutes = align_to_minutes
        self._db_last_save_stamp: float | None = None

    # SpotRetriever
    def get_price(self) -> CurrencyPair | None:
        key = _price_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = self._redis.get(key)
        pair = _parse_price_from_redis(raw)
        if pair is not None:
            return pair
        record = (
            self._db_session.query(CurrencyPairSnapshot)
            .filter_by(
                exchange_id=self._exchange_id,
                kind=self._kind,
                symbol=self._symbol,
                align_to_minutes=self._align_to_minutes,
            )
            .order_by(CurrencyPairSnapshot.id.desc())
            .first()
        )
        if record is None:
            return None
        pair = CurrencyPair(
            base=record.base,
            quote=record.quote,
            ratio=record.ratio,
            utc=record.utc,
        )
        value = json.dumps({
            "base": record.base,
            "quote": record.quote,
            "ratio": record.ratio,
            "utc": record.utc,
        })
        self._redis.setex(key, int(self._cache_timeout), value)
        return pair

    def get_depth(self, limit: int = 100) -> BookDepth | None:
        raise NotImplementedError

    def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        raise NotImplementedError

    def get_withdraw_info(self) -> dict[str, list[WithdrawInfo]] | None:
        raise NotImplementedError

    # SpotPublisher
    def publish_price(self, ticker: CurrencyPair) -> None:
        aligned_utc = _align_utc(ticker.utc, self._align_to_minutes)
        if aligned_utc is None:
            aligned_utc = _align_utc(time.time(), self._align_to_minutes) or time.time()
        key = _price_redis_key(self._exchange_id, self._kind, self._symbol)
        value = json.dumps({
            "base": ticker.base,
            "quote": ticker.quote,
            "ratio": ticker.ratio,
            "utc": ticker.utc,
        })
        self._redis.setex(key, int(self._cache_timeout), value)
        now = time.time()
        if self._db_last_save_stamp is None or now >= self._db_last_save_stamp + self._cache_timeout:
            record = self._db_session.query(CurrencyPairSnapshot).filter_by(
                exchange_id=self._exchange_id,
                kind=self._kind,
                symbol=self._symbol,
                align_to_minutes=self._align_to_minutes,
                aligned_timestamp=aligned_utc,
            ).first()
            if record is None:
                record = CurrencyPairSnapshot(
                    exchange_id=self._exchange_id,
                    kind=self._kind,
                    symbol=self._symbol,
                    base=ticker.base,
                    quote=ticker.quote,
                    ratio=ticker.ratio,
                    utc=ticker.utc,
                    align_to_minutes=self._align_to_minutes,
                    aligned_timestamp=aligned_utc,
                )
                self._db_session.add(record)
            else:
                record.base = ticker.base
                record.quote = ticker.quote
                record.ratio = ticker.ratio
                record.utc = ticker.utc
            self._db_session.commit()
            self._db_last_save_stamp = now

    def publish_book_depth(
        self, book_depth: BookDepth, strategy: PublishStrategy = PublishStrategy.REPLACE
    ) -> None:
        raise NotImplementedError

    def publish_candlestick(
        self, candlestick: CandleStick | list[CandleStick], strategy: PublishStrategy = PublishStrategy.MERGE
    ) -> None:
        raise NotImplementedError

    def publish_withdraw_info(
        self, withdraw_infos: dict[str, list[WithdrawInfo]]
    ) -> None:
        raise NotImplementedError


class AsyncSpotOrchestratorImpl:
    """
    Реализация асинхронного retriever для spot: AsyncSpotRetriever.
    """

    def __init__(
        self,
        db_session: "AsyncSession",
        redis: "AsyncRedis",
        exchange_id: str,
        kind: str,
        symbol: str,
        cache_timeout: float = 15,
        align_to_minutes: int = 1,
    ) -> None:
        self._db_session = db_session
        self._redis = redis
        self._exchange_id = exchange_id
        self._kind = kind
        self._symbol = symbol
        self._cache_timeout = cache_timeout
        self._align_to_minutes = align_to_minutes

    async def get_price(self) -> CurrencyPair | None:
        key = _price_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = await self._redis.get(key)
        pair = _parse_price_from_redis(raw)
        if pair is not None:
            return pair
        stmt = (
            select(CurrencyPairSnapshot)
            .where(
                CurrencyPairSnapshot.exchange_id == self._exchange_id,
                CurrencyPairSnapshot.kind == self._kind,
                CurrencyPairSnapshot.symbol == self._symbol,
                CurrencyPairSnapshot.align_to_minutes == self._align_to_minutes,
            )
            .order_by(CurrencyPairSnapshot.id.desc())
            .limit(1)
        )
        result = await self._db_session.execute(stmt)
        record = result.scalar_one_or_none()
        if record is None:
            return None
        pair = CurrencyPair(
            base=record.base,
            quote=record.quote,
            ratio=record.ratio,
            utc=record.utc,
        )
        value = json.dumps({
            "base": record.base,
            "quote": record.quote,
            "ratio": record.ratio,
            "utc": record.utc,
        })
        await self._redis.set(key, value, ex=int(self._cache_timeout))
        return pair

    async def get_depth(self, limit: int = 100) -> BookDepth | None:
        raise NotImplementedError

    async def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        raise NotImplementedError

    async def get_withdraw_info(self) -> dict[str, list[WithdrawInfo]] | None:
        raise NotImplementedError


class PerpetualOrchestratorImpl:
    """
    Реализация оркестратора для perpetual: PerpetualPublisher + PerpetualRetriever.
    """

    def __init__(
        self,
        db_session: "Session",
        redis: "Redis",
        exchange_id: str,
        kind: str,
        symbol: str,
        cache_timeout: float = 15,
        align_to_minutes: int = 1,  # выравнивание timestamp до N минут
    ) -> None:
        self._db_session = db_session
        self._redis = redis
        self._exchange_id = exchange_id
        self._kind = kind
        self._symbol = symbol
        self._cache_timeout = cache_timeout
        self._align_to_minutes = align_to_minutes
        self._db_last_save_stamp: float | None = None

    # PerpetualRetriever
    def get_price(self) -> CurrencyPair | None:
        key = _price_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = self._redis.get(key)
        pair = _parse_price_from_redis(raw)
        if pair is not None:
            return pair
        record = (
            self._db_session.query(CurrencyPairSnapshot)
            .filter_by(
                exchange_id=self._exchange_id,
                kind=self._kind,
                symbol=self._symbol,
                align_to_minutes=self._align_to_minutes,
            )
            .order_by(CurrencyPairSnapshot.id.desc())
            .first()
        )
        if record is None:
            return None
        pair = CurrencyPair(
            base=record.base,
            quote=record.quote,
            ratio=record.ratio,
            utc=record.utc,
        )
        value = json.dumps({
            "base": record.base,
            "quote": record.quote,
            "ratio": record.ratio,
            "utc": record.utc,
        })
        self._redis.setex(key, int(self._cache_timeout), value)
        return pair

    def get_depth(self, limit: int = 100) -> BookDepth | None:
        raise NotImplementedError

    def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        raise NotImplementedError

    def get_funding_rate(self) -> FundingRate | None:
        raise NotImplementedError

    def get_funding_rate_history(
        self, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        raise NotImplementedError

    # PerpetualPublisher
    def publish_price(self, ticker: CurrencyPair) -> None:
        aligned_utc = _align_utc(ticker.utc, self._align_to_minutes)
        if aligned_utc is None:
            aligned_utc = _align_utc(time.time(), self._align_to_minutes) or time.time()
        key = _price_redis_key(self._exchange_id, self._kind, self._symbol)
        value = json.dumps({
            "base": ticker.base,
            "quote": ticker.quote,
            "ratio": ticker.ratio,
            "utc": ticker.utc,
        })
        self._redis.setex(key, int(self._cache_timeout), value)
        now = time.time()
        if self._db_last_save_stamp is None or now >= self._db_last_save_stamp + self._cache_timeout:
            record = self._db_session.query(CurrencyPairSnapshot).filter_by(
                exchange_id=self._exchange_id,
                kind=self._kind,
                symbol=self._symbol,
                align_to_minutes=self._align_to_minutes,
                aligned_timestamp=aligned_utc,
            ).first()
            if record is None:
                record = CurrencyPairSnapshot(
                    exchange_id=self._exchange_id,
                    kind=self._kind,
                    symbol=self._symbol,
                    base=ticker.base,
                    quote=ticker.quote,
                    ratio=ticker.ratio,
                    utc=ticker.utc,
                    align_to_minutes=self._align_to_minutes,
                    aligned_timestamp=aligned_utc,
                )
                self._db_session.add(record)
            else:
                record.base = ticker.base
                record.quote = ticker.quote
                record.ratio = ticker.ratio
                record.utc = ticker.utc
            self._db_session.commit()
            self._db_last_save_stamp = now

    def publish_book_depth(
        self, book_depth: BookDepth, strategy: PublishStrategy = PublishStrategy.REPLACE
    ) -> None:
        raise NotImplementedError

    def publish_candlestick(
        self, candlestick: CandleStick | list[CandleStick], strategy: PublishStrategy = PublishStrategy.MERGE
    ) -> None:
        raise NotImplementedError

    def publish_funding_rate(self, funding_rate: FundingRate) -> None:
        raise NotImplementedError

    def publish_funding_history(
        self, symbol: str, history: list[FundingRatePoint]
    ) -> None:
        raise NotImplementedError


class AsyncPerpetualOrchestratorImpl:
    """
    Реализация асинхронного retriever для perpetual: AsyncPerpetualRetriever.
    """

    def __init__(
        self,
        db_session: "AsyncSession",
        redis: "AsyncRedis",
        exchange_id: str,
        kind: str,
        symbol: str,
        cache_timeout: float = 15,
        align_to_minutes: int = 1,
    ) -> None:
        self._db_session = db_session
        self._redis = redis
        self._exchange_id = exchange_id
        self._kind = kind
        self._symbol = symbol
        self._cache_timeout = cache_timeout
        self._align_to_minutes = align_to_minutes

    async def get_price(self) -> CurrencyPair | None:
        key = _price_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = await self._redis.get(key)
        pair = _parse_price_from_redis(raw)
        if pair is not None:
            return pair
        stmt = (
            select(CurrencyPairSnapshot)
            .where(
                CurrencyPairSnapshot.exchange_id == self._exchange_id,
                CurrencyPairSnapshot.kind == self._kind,
                CurrencyPairSnapshot.symbol == self._symbol,
                CurrencyPairSnapshot.align_to_minutes == self._align_to_minutes,
            )
            .order_by(CurrencyPairSnapshot.id.desc())
            .limit(1)
        )
        result = await self._db_session.execute(stmt)
        record = result.scalar_one_or_none()
        if record is None:
            return None
        pair = CurrencyPair(
            base=record.base,
            quote=record.quote,
            ratio=record.ratio,
            utc=record.utc,
        )
        value = json.dumps({
            "base": record.base,
            "quote": record.quote,
            "ratio": record.ratio,
            "utc": record.utc,
        })
        await self._redis.set(key, value, ex=int(self._cache_timeout))
        return pair

    async def get_depth(self, limit: int = 100) -> BookDepth | None:
        raise NotImplementedError

    async def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        raise NotImplementedError

    async def get_funding_rate(self) -> FundingRate | None:
        raise NotImplementedError

    async def get_funding_rate_history(
        self, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        raise NotImplementedError