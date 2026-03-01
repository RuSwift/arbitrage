import json
import logging
import math
import time
from typing import TYPE_CHECKING, Protocol

from app.cex.dto import (
    BidAsk,
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

from app.db.models import BookDepthSnapshot, CandleStickSnapshot, CurrencyPairSnapshot
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


def _book_depth_redis_key(exchange_id: str, kind: str, symbol: str) -> str:
    return f"arbitrage:orchestrator:depth:{exchange_id}:{kind}:{symbol}"


def _parse_depth_from_redis(raw: bytes | str | None) -> BookDepth | None:
    """Парсит значение из Redis (JSON) в BookDepth. Возвращает None если raw пусто или невалидно."""
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        data = json.loads(raw)
        bids = [
            BidAsk(price=float(x["price"]), quantity=float(x["quantity"]))
            for x in (data.get("bids") or [])
        ]
        asks = [
            BidAsk(price=float(x["price"]), quantity=float(x["quantity"]))
            for x in (data.get("asks") or [])
        ]
        return BookDepth(
            symbol=data["symbol"],
            bids=bids,
            asks=asks,
            exchange_symbol=data.get("exchange_symbol"),
            last_update_id=data.get("last_update_id"),
            utc=data.get("utc"),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _candlestick_redis_key(exchange_id: str, kind: str, symbol: str) -> str:
    return f"arbitrage:orchestrator:candlestick:{exchange_id}:{kind}:{symbol}"


def _parse_candlestick_list_from_redis(raw: bytes | str | None) -> list[CandleStick] | None:
    """Парсит значение из Redis (JSON-массив) в list[CandleStick]. Возвращает None если raw пусто или невалидно."""
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        data = json.loads(raw)
        if not isinstance(data, list):
            return None
        return [CandleStick.from_dict(item) for item in data]
    except (KeyError, TypeError, ValueError):
        return None


def _normalize_and_merge_candlesticks(
    candles: list[CandleStick],
    align_to_minutes: int,
    *,
    exchange_id: str | None = None,
    kind: str | None = None,
    symbol: str | None = None,
) -> list[CandleStick]:
    """
    Убирает пересечения по aligned_utc: для одного aligned_utc оставляет одну свечу (более свежая по utc_open_time).
    При пересечении пишет log.critical. Возвращает список без дубликатов по aligned_utc.
    """
    if not candles:
        return []
    by_aligned: dict[float, list[CandleStick]] = {}
    for c in candles:
        aligned = _align_utc(c.utc_open_time, align_to_minutes)
        if aligned is None:
            continue
        by_aligned.setdefault(aligned, []).append(c)
    result: list[CandleStick] = []
    for aligned_utc, group in by_aligned.items():
        if len(group) > 1:
            logging.critical(
                "candlestick overlap by aligned_utc, merging newer wins",
                extra={"exchange_id": exchange_id, "kind": kind, "symbol": symbol, "aligned_utc": aligned_utc},
            )
        winner = max(group, key=lambda x: x.utc_open_time)
        result.append(winner)
    result.sort(key=lambda c: c.utc_open_time, reverse=True)
    return result


def _merge_candlestick_lists_newer_wins(
    a: list[CandleStick], b: list[CandleStick], align_to_minutes: int
) -> list[CandleStick]:
    """Объединяет два списка свечей: по aligned_utc оставляет более свежую (по utc_open_time). Сортировка: свежие первые."""
    by_aligned: dict[float, CandleStick] = {}
    for c in a + b:
        aligned = _align_utc(c.utc_open_time, align_to_minutes)
        if aligned is None:
            continue
        if aligned not in by_aligned or c.utc_open_time > by_aligned[aligned].utc_open_time:
            by_aligned[aligned] = c
    result = list(by_aligned.values())
    result.sort(key=lambda x: x.utc_open_time, reverse=True)
    return result


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
        self._db_last_depth_save_stamp: float | None = None

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
        key = _book_depth_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = self._redis.get(key)
        depth = _parse_depth_from_redis(raw)
        if depth is not None:
            return depth
        record = (
            self._db_session.query(BookDepthSnapshot)
            .filter_by(
                exchange_id=self._exchange_id,
                kind=self._kind,
                symbol=self._symbol,
                align_to_minutes=self._align_to_minutes,
            )
            .order_by(BookDepthSnapshot.id.desc())
            .first()
        )
        if record is None:
            return None
        bids_asks = record.bids_asks or {"bids": [], "asks": []}
        depth = BookDepth(
            symbol=record.symbol,
            bids=[BidAsk(price=p["price"], quantity=p["quantity"]) for p in (bids_asks.get("bids") or [])],
            asks=[BidAsk(price=p["price"], quantity=p["quantity"]) for p in (bids_asks.get("asks") or [])],
            exchange_symbol=record.exchange_symbol,
            last_update_id=record.last_update_id,
            utc=record.utc,
        )
        value = json.dumps(depth.as_dict())
        self._redis.setex(key, int(self._cache_timeout), value)
        return depth

    def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        key = _candlestick_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = self._redis.get(key)
        from_redis = _parse_candlestick_list_from_redis(raw) or []
        if limit is not None and len(from_redis) < limit:
            need = limit
            db_records = (
                self._db_session.query(CandleStickSnapshot)
                .filter_by(
                    exchange_id=self._exchange_id,
                    kind=self._kind,
                    symbol=self._symbol,
                    align_to_minutes=self._align_to_minutes,
                )
                .order_by(CandleStickSnapshot.aligned_timestamp.desc())
                .limit(need)
                .all()
            )
            from_db = [
                CandleStick(
                    utc_open_time=r.utc_open_time,
                    open_price=r.open_price,
                    high_price=r.high_price,
                    low_price=r.low_price,
                    close_price=r.close_price,
                    coin_volume=r.coin_volume,
                    usd_volume=r.usd_volume,
                )
                for r in db_records
            ]
            merged = _merge_candlestick_lists_newer_wins(
                from_redis, from_db, self._align_to_minutes
            )
        else:
            merged = list(from_redis)
            merged.sort(key=lambda x: x.utc_open_time, reverse=True)
        if limit is not None:
            merged = merged[:limit]
        return merged if merged else None

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
        if strategy == PublishStrategy.MERGE:
            raise NotImplementedError
        utc = book_depth.utc
        if utc is None:
            utc = time.time()
        aligned_utc = _align_utc(utc, self._align_to_minutes)
        if aligned_utc is None:
            aligned_utc = _align_utc(time.time(), self._align_to_minutes) or time.time()
        key = _book_depth_redis_key(self._exchange_id, self._kind, self._symbol)
        value = json.dumps(book_depth.as_dict())
        self._redis.setex(key, int(self._cache_timeout), value)
        now = time.time()
        if self._db_last_depth_save_stamp is None or now >= self._db_last_depth_save_stamp + self._cache_timeout:
            bids_asks = {
                "bids": [b.as_dict() for b in book_depth.bids],
                "asks": [a.as_dict() for a in book_depth.asks],
            }
            record = self._db_session.query(BookDepthSnapshot).filter_by(
                exchange_id=self._exchange_id,
                kind=self._kind,
                symbol=self._symbol,
                align_to_minutes=self._align_to_minutes,
                aligned_timestamp=aligned_utc,
            ).first()
            if record is None:
                record = BookDepthSnapshot(
                    exchange_id=self._exchange_id,
                    kind=self._kind,
                    symbol=self._symbol,
                    exchange_symbol=book_depth.exchange_symbol,
                    last_update_id=str(book_depth.last_update_id) if book_depth.last_update_id is not None else None,
                    utc=book_depth.utc,
                    bids_asks=bids_asks,
                    align_to_minutes=self._align_to_minutes,
                    aligned_timestamp=aligned_utc,
                )
                self._db_session.add(record)
            else:
                record.exchange_symbol = book_depth.exchange_symbol
                record.last_update_id = str(book_depth.last_update_id) if book_depth.last_update_id is not None else None
                record.utc = book_depth.utc
                record.bids_asks = bids_asks
            self._db_session.commit()
            self._db_last_depth_save_stamp = now

    def publish_candlestick(
        self, candlestick: CandleStick | list[CandleStick], strategy: PublishStrategy = PublishStrategy.MERGE
    ) -> None:
        if strategy == PublishStrategy.REPLACE:
            raise ValueError("PublishStrategy.REPLACE is not supported for publish_candlestick")
        incoming: list[CandleStick] = (
            [candlestick] if isinstance(candlestick, CandleStick) else list(candlestick)
        )
        normalized = _normalize_and_merge_candlesticks(
            incoming,
            self._align_to_minutes,
            exchange_id=self._exchange_id,
            kind=self._kind,
            symbol=self._symbol,
        )
        key = _candlestick_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = self._redis.get(key)
        current = _parse_candlestick_list_from_redis(raw) or []
        final = _merge_candlestick_lists_newer_wins(
            normalized, current, self._align_to_minutes
        )
        value = json.dumps([c.as_dict() for c in final])
        self._redis.setex(key, int(self._cache_timeout), value)
        now = time.time()
        cutoff = now - self._align_to_minutes * 60
        for c in final:
            aligned_ts = _align_utc(c.utc_open_time, self._align_to_minutes)
            if aligned_ts is None or aligned_ts < cutoff:
                continue
            record = self._db_session.query(CandleStickSnapshot).filter_by(
                exchange_id=self._exchange_id,
                kind=self._kind,
                symbol=self._symbol,
                align_to_minutes=self._align_to_minutes,
                aligned_timestamp=aligned_ts,
            ).first()
            if record is None:
                record = CandleStickSnapshot(
                    exchange_id=self._exchange_id,
                    kind=self._kind,
                    symbol=self._symbol,
                    span_in_minutes=self._align_to_minutes,
                    utc_open_time=c.utc_open_time,
                    open_price=c.open_price,
                    high_price=c.high_price,
                    low_price=c.low_price,
                    close_price=c.close_price,
                    coin_volume=c.coin_volume,
                    usd_volume=c.usd_volume,
                    utc=c.utc_open_time,
                    align_to_minutes=self._align_to_minutes,
                    aligned_timestamp=aligned_ts,
                )
                self._db_session.add(record)
            else:
                if record.utc is None or c.utc_open_time > record.utc:
                    record.utc_open_time = c.utc_open_time
                    record.open_price = c.open_price
                    record.high_price = c.high_price
                    record.low_price = c.low_price
                    record.close_price = c.close_price
                    record.coin_volume = c.coin_volume
                    record.usd_volume = c.usd_volume
                    record.utc = c.utc_open_time
        self._db_session.commit()

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
        key = _book_depth_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = await self._redis.get(key)
        depth = _parse_depth_from_redis(raw)
        if depth is not None:
            return depth
        stmt = (
            select(BookDepthSnapshot)
            .where(
                BookDepthSnapshot.exchange_id == self._exchange_id,
                BookDepthSnapshot.kind == self._kind,
                BookDepthSnapshot.symbol == self._symbol,
                BookDepthSnapshot.align_to_minutes == self._align_to_minutes,
            )
            .order_by(BookDepthSnapshot.id.desc())
            .limit(1)
        )
        result = await self._db_session.execute(stmt)
        record = result.scalar_one_or_none()
        if record is None:
            return None
        bids_asks = record.bids_asks or {"bids": [], "asks": []}
        depth = BookDepth(
            symbol=record.symbol,
            bids=[BidAsk(price=p["price"], quantity=p["quantity"]) for p in (bids_asks.get("bids") or [])],
            asks=[BidAsk(price=p["price"], quantity=p["quantity"]) for p in (bids_asks.get("asks") or [])],
            exchange_symbol=record.exchange_symbol,
            last_update_id=record.last_update_id,
            utc=record.utc,
        )
        value = json.dumps(depth.as_dict())
        await self._redis.set(key, value, ex=int(self._cache_timeout))
        return depth

    async def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        key = _candlestick_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = await self._redis.get(key)
        from_redis = _parse_candlestick_list_from_redis(raw) or []
        if limit is not None and len(from_redis) < limit:
            need = limit
            stmt = (
                select(CandleStickSnapshot)
                .where(
                    CandleStickSnapshot.exchange_id == self._exchange_id,
                    CandleStickSnapshot.kind == self._kind,
                    CandleStickSnapshot.symbol == self._symbol,
                    CandleStickSnapshot.align_to_minutes == self._align_to_minutes,
                )
                .order_by(CandleStickSnapshot.aligned_timestamp.desc())
                .limit(need)
            )
            result = await self._db_session.execute(stmt)
            db_records = result.scalars().all()
            from_db = [
                CandleStick(
                    utc_open_time=r.utc_open_time,
                    open_price=r.open_price,
                    high_price=r.high_price,
                    low_price=r.low_price,
                    close_price=r.close_price,
                    coin_volume=r.coin_volume,
                    usd_volume=r.usd_volume,
                )
                for r in db_records
            ]
            merged = _merge_candlestick_lists_newer_wins(
                from_redis, from_db, self._align_to_minutes
            )
        else:
            merged = list(from_redis)
            merged.sort(key=lambda x: x.utc_open_time, reverse=True)
        if limit is not None:
            merged = merged[:limit]
        return merged if merged else None

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
        self._db_last_depth_save_stamp: float | None = None

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
        key = _book_depth_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = self._redis.get(key)
        depth = _parse_depth_from_redis(raw)
        if depth is not None:
            return depth
        record = (
            self._db_session.query(BookDepthSnapshot)
            .filter_by(
                exchange_id=self._exchange_id,
                kind=self._kind,
                symbol=self._symbol,
                align_to_minutes=self._align_to_minutes,
            )
            .order_by(BookDepthSnapshot.id.desc())
            .first()
        )
        if record is None:
            return None
        bids_asks = record.bids_asks or {"bids": [], "asks": []}
        depth = BookDepth(
            symbol=record.symbol,
            bids=[BidAsk(price=p["price"], quantity=p["quantity"]) for p in (bids_asks.get("bids") or [])],
            asks=[BidAsk(price=p["price"], quantity=p["quantity"]) for p in (bids_asks.get("asks") or [])],
            exchange_symbol=record.exchange_symbol,
            last_update_id=record.last_update_id,
            utc=record.utc,
        )
        value = json.dumps(depth.as_dict())
        self._redis.setex(key, int(self._cache_timeout), value)
        return depth

    def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        key = _candlestick_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = self._redis.get(key)
        from_redis = _parse_candlestick_list_from_redis(raw) or []
        if limit is not None and len(from_redis) < limit:
            need = limit
            db_records = (
                self._db_session.query(CandleStickSnapshot)
                .filter_by(
                    exchange_id=self._exchange_id,
                    kind=self._kind,
                    symbol=self._symbol,
                    align_to_minutes=self._align_to_minutes,
                )
                .order_by(CandleStickSnapshot.aligned_timestamp.desc())
                .limit(need)
                .all()
            )
            from_db = [
                CandleStick(
                    utc_open_time=r.utc_open_time,
                    open_price=r.open_price,
                    high_price=r.high_price,
                    low_price=r.low_price,
                    close_price=r.close_price,
                    coin_volume=r.coin_volume,
                    usd_volume=r.usd_volume,
                )
                for r in db_records
            ]
            merged = _merge_candlestick_lists_newer_wins(
                from_redis, from_db, self._align_to_minutes
            )
        else:
            merged = list(from_redis)
            merged.sort(key=lambda x: x.utc_open_time, reverse=True)
        if limit is not None:
            merged = merged[:limit]
        return merged if merged else None

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
        if strategy == PublishStrategy.MERGE:
            raise NotImplementedError
        utc = book_depth.utc
        if utc is None:
            utc = time.time()
        aligned_utc = _align_utc(utc, self._align_to_minutes)
        if aligned_utc is None:
            aligned_utc = _align_utc(time.time(), self._align_to_minutes) or time.time()
        key = _book_depth_redis_key(self._exchange_id, self._kind, self._symbol)
        value = json.dumps(book_depth.as_dict())
        self._redis.setex(key, int(self._cache_timeout), value)
        now = time.time()
        if self._db_last_depth_save_stamp is None or now >= self._db_last_depth_save_stamp + self._cache_timeout:
            bids_asks = {
                "bids": [b.as_dict() for b in book_depth.bids],
                "asks": [a.as_dict() for a in book_depth.asks],
            }
            record = self._db_session.query(BookDepthSnapshot).filter_by(
                exchange_id=self._exchange_id,
                kind=self._kind,
                symbol=self._symbol,
                align_to_minutes=self._align_to_minutes,
                aligned_timestamp=aligned_utc,
            ).first()
            if record is None:
                record = BookDepthSnapshot(
                    exchange_id=self._exchange_id,
                    kind=self._kind,
                    symbol=self._symbol,
                    exchange_symbol=book_depth.exchange_symbol,
                    last_update_id=str(book_depth.last_update_id) if book_depth.last_update_id is not None else None,
                    utc=book_depth.utc,
                    bids_asks=bids_asks,
                    align_to_minutes=self._align_to_minutes,
                    aligned_timestamp=aligned_utc,
                )
                self._db_session.add(record)
            else:
                record.exchange_symbol = book_depth.exchange_symbol
                record.last_update_id = str(book_depth.last_update_id) if book_depth.last_update_id is not None else None
                record.utc = book_depth.utc
                record.bids_asks = bids_asks
            self._db_session.commit()
            self._db_last_depth_save_stamp = now

    def publish_candlestick(
        self, candlestick: CandleStick | list[CandleStick], strategy: PublishStrategy = PublishStrategy.MERGE
    ) -> None:
        if strategy == PublishStrategy.REPLACE:
            raise ValueError("PublishStrategy.REPLACE is not supported for publish_candlestick")
        incoming: list[CandleStick] = (
            [candlestick] if isinstance(candlestick, CandleStick) else list(candlestick)
        )
        normalized = _normalize_and_merge_candlesticks(
            incoming,
            self._align_to_minutes,
            exchange_id=self._exchange_id,
            kind=self._kind,
            symbol=self._symbol,
        )
        key = _candlestick_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = self._redis.get(key)
        current = _parse_candlestick_list_from_redis(raw) or []
        final = _merge_candlestick_lists_newer_wins(
            normalized, current, self._align_to_minutes
        )
        value = json.dumps([c.as_dict() for c in final])
        self._redis.setex(key, int(self._cache_timeout), value)
        now = time.time()
        cutoff = now - self._align_to_minutes * 60
        for c in final:
            aligned_ts = _align_utc(c.utc_open_time, self._align_to_minutes)
            if aligned_ts is None or aligned_ts < cutoff:
                continue
            record = self._db_session.query(CandleStickSnapshot).filter_by(
                exchange_id=self._exchange_id,
                kind=self._kind,
                symbol=self._symbol,
                align_to_minutes=self._align_to_minutes,
                aligned_timestamp=aligned_ts,
            ).first()
            if record is None:
                record = CandleStickSnapshot(
                    exchange_id=self._exchange_id,
                    kind=self._kind,
                    symbol=self._symbol,
                    span_in_minutes=self._align_to_minutes,
                    utc_open_time=c.utc_open_time,
                    open_price=c.open_price,
                    high_price=c.high_price,
                    low_price=c.low_price,
                    close_price=c.close_price,
                    coin_volume=c.coin_volume,
                    usd_volume=c.usd_volume,
                    utc=c.utc_open_time,
                    align_to_minutes=self._align_to_minutes,
                    aligned_timestamp=aligned_ts,
                )
                self._db_session.add(record)
            else:
                if record.utc is None or c.utc_open_time > record.utc:
                    record.utc_open_time = c.utc_open_time
                    record.open_price = c.open_price
                    record.high_price = c.high_price
                    record.low_price = c.low_price
                    record.close_price = c.close_price
                    record.coin_volume = c.coin_volume
                    record.usd_volume = c.usd_volume
                    record.utc = c.utc_open_time
        self._db_session.commit()

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
        key = _book_depth_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = await self._redis.get(key)
        depth = _parse_depth_from_redis(raw)
        if depth is not None:
            return depth
        stmt = (
            select(BookDepthSnapshot)
            .where(
                BookDepthSnapshot.exchange_id == self._exchange_id,
                BookDepthSnapshot.kind == self._kind,
                BookDepthSnapshot.symbol == self._symbol,
                BookDepthSnapshot.align_to_minutes == self._align_to_minutes,
            )
            .order_by(BookDepthSnapshot.id.desc())
            .limit(1)
        )
        result = await self._db_session.execute(stmt)
        record = result.scalar_one_or_none()
        if record is None:
            return None
        bids_asks = record.bids_asks or {"bids": [], "asks": []}
        depth = BookDepth(
            symbol=record.symbol,
            bids=[BidAsk(price=p["price"], quantity=p["quantity"]) for p in (bids_asks.get("bids") or [])],
            asks=[BidAsk(price=p["price"], quantity=p["quantity"]) for p in (bids_asks.get("asks") or [])],
            exchange_symbol=record.exchange_symbol,
            last_update_id=record.last_update_id,
            utc=record.utc,
        )
        value = json.dumps(depth.as_dict())
        await self._redis.set(key, value, ex=int(self._cache_timeout))
        return depth

    async def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        key = _candlestick_redis_key(self._exchange_id, self._kind, self._symbol)
        raw = await self._redis.get(key)
        from_redis = _parse_candlestick_list_from_redis(raw) or []
        if limit is not None and len(from_redis) < limit:
            need = limit
            stmt = (
                select(CandleStickSnapshot)
                .where(
                    CandleStickSnapshot.exchange_id == self._exchange_id,
                    CandleStickSnapshot.kind == self._kind,
                    CandleStickSnapshot.symbol == self._symbol,
                    CandleStickSnapshot.align_to_minutes == self._align_to_minutes,
                )
                .order_by(CandleStickSnapshot.aligned_timestamp.desc())
                .limit(need)
            )
            result = await self._db_session.execute(stmt)
            db_records = result.scalars().all()
            from_db = [
                CandleStick(
                    utc_open_time=r.utc_open_time,
                    open_price=r.open_price,
                    high_price=r.high_price,
                    low_price=r.low_price,
                    close_price=r.close_price,
                    coin_volume=r.coin_volume,
                    usd_volume=r.usd_volume,
                )
                for r in db_records
            ]
            merged = _merge_candlestick_lists_newer_wins(
                from_redis, from_db, self._align_to_minutes
            )
        else:
            merged = list(from_redis)
            merged.sort(key=lambda x: x.utc_open_time, reverse=True)
        if limit is not None:
            merged = merged[:limit]
        return merged if merged else None

    async def get_funding_rate(self) -> FundingRate | None:
        raise NotImplementedError

    async def get_funding_rate_history(
        self, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        raise NotImplementedError