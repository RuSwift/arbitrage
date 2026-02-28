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


class SpotOrchestratorImpl:
    """
    Реализация оркестратора для spot: SpotPublisher + SpotRetriever.
    """

    def __init__(
        self,
        db_session: "Session",
        redis: "Redis",
        cache_timeout: float = 15,
        align_to_minutes: int = 1,  # выравнивание timestamp до N минут (timestamp % align_to_minutes == 0)
    ) -> None:
        self._db_session = db_session
        self._redis = redis
        self._cache_timeout = cache_timeout
        self._align_to_minutes = align_to_minutes

    # SpotRetriever
    def get_price(self) -> CurrencyPair | None:
        raise NotImplementedError

    def get_depth(self, limit: int = 100) -> BookDepth | None:
        raise NotImplementedError

    def get_klines(self, limit: int | None = None) -> list[CandleStick] | None:
        raise NotImplementedError

    def get_withdraw_info(self) -> dict[str, list[WithdrawInfo]] | None:
        raise NotImplementedError

    # SpotPublisher
    def publish_price(self, ticker: CurrencyPair) -> None:
        raise NotImplementedError

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
        cache_timeout: float = 15,
        align_to_minutes: int = 1,
    ) -> None:
        self._db_session = db_session
        self._redis = redis
        self._cache_timeout = cache_timeout
        self._align_to_minutes = align_to_minutes

    async def get_price(self) -> CurrencyPair | None:
        raise NotImplementedError

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
        cache_timeout: float = 15,
        align_to_minutes: int = 1,  # выравнивание timestamp до N минут (timestamp % align_to_minutes == 0)
    ) -> None:
        self._db_session = db_session
        self._redis = redis
        self._cache_timeout = cache_timeout
        self._align_to_minutes = align_to_minutes

    # PerpetualRetriever
    def get_price(self) -> CurrencyPair | None:
        raise NotImplementedError

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
        raise NotImplementedError

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
        cache_timeout: float = 15,
        align_to_minutes: int = 1,
    ) -> None:
        self._db_session = db_session
        self._redis = redis
        self._cache_timeout = cache_timeout
        self._align_to_minutes = align_to_minutes

    async def get_price(self) -> CurrencyPair | None:
        raise NotImplementedError

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