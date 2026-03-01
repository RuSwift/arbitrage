"""Сервис обхода perpetual (фьючерсных) котировок по CEX."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session as SyncDBSession

from app.cex.base import BaseCEXPerpetualConnector
from app.cex.dto import CurrencyPair
from app.db.models import CrawlerIteration, CrawlerJob, Token
from app.services.base import BaseService
from app.services.unit_of_work import UnitOfWork
from app.settings import ServiceConfigRegistry


class CEXPerpetualCrawler(BaseService):
    """Обход perpetual-рынка биржи: пары, стакан, свечи, funding rate."""

    class Config(BaseModel, extra="ignore"):
        """Конфиг краулера. Задаётся при создании или из БД (service_config)."""

        
        """Сколько токенов брать из CoinMarketCap (top N)."""
        window_sec: int = 60
        """Окно в секундах для rate-limit трекера CEX API."""

    ConfigModel: type[BaseModel] = Config

    def __init__(
        self,
        uow: UnitOfWork,
        exchange_id: str,
        config: Config | None = None,
    ) -> None:
        super().__init__(uow)
        self._exchange_id = exchange_id
        self._config = config
        self._connector: BaseCEXPerpetualConnector | None = None

    async def _resolve_config(self) -> Config:
        """Конфиг: переданный в __init__, из БД (CEXPerpetualCrawler) или дефолтный."""
        if self._config is not None:
            return self._config
        loaded = await ServiceConfigRegistry.aget(
            self.db, "CEXPerpetualCrawler", self.__class__.Config
        )
        return loaded or self.__class__.Config()

    def _get_connector(self) -> BaseCEXPerpetualConnector:
        """Ленивое создание коннектора по exchange_id через BaseCEXPerpetualConnector.Registry."""
        if self._connector is None:
            connector_cls = BaseCEXPerpetualConnector.Registry.get(self._exchange_id)
            if connector_cls is None:
                raise ValueError(
                    f"Unknown exchange_id={self._exchange_id!r}. "
                    f"Available: {list(BaseCEXPerpetualConnector.Registry.keys())}"
                )
            self._connector = connector_cls(log=self.log)
        return self._connector
    
    async def prepare_job(self) -> CrawlerJob:
        """Upsert CrawlerJob по (exchange, kind): найти по exchange_id и kind=perpetual, иначе создать."""
        kind = "perpetual"
        result = await self.db.execute(
            select(CrawlerJob).where(
                CrawlerJob.exchange == self._exchange_id,
                CrawlerJob.kind == kind,
            )
        )
        job = result.scalar_one_or_none()
        now = datetime.now(timezone.utc)
        if job is not None:
            job.start = now
            job.stop = None
            job.error = None
            await self.db.flush()
            await self.db.refresh(job)
            return job
        job = CrawlerJob(
            exchange=self._exchange_id,
            connector=kind,
            kind=kind,
            start=now,
            stop=None,
            error=None,
        )
        self.db.add(job)
        await self.db.flush()
        await self.db.refresh(job)
        return job
    
    def prepare_job_iterations(self, job_id: int, db: SyncDBSession) -> list[CrawlerIteration]:
        """Синхронно: коннектор + загрузка токенов и upsert итераций через переданную SyncDBSession. job_id — id CrawlerJob (в поток не передавать ORM-объекты от async-сессии)."""
        result = db.execute(select(Token).order_by(Token.id))
        tokens = list(result.scalars().all())
        symbols_ordered = list(dict.fromkeys(t.symbol for t in tokens))

        connector = self._get_connector()
        all_perpetuals = connector.get_all_perpetuals()
        tokens_set = set(symbols_ordered)
        tickers_in_scope = [t for t in all_perpetuals if t.base in tokens_set]
        symbols_for_get_pairs = [t.exchange_symbol for t in tickers_in_scope]
        pairs = connector.get_pairs(symbols=symbols_for_get_pairs)
        pair_by_base: dict[str, CurrencyPair] = {p.base: p for p in (pairs or [])}
        bases_on_exchange = {t.base for t in all_perpetuals}

        now = datetime.now(timezone.utc)
        iterations: list[CrawlerIteration] = []

        for symbol in symbols_ordered:
            r = db.execute(
                select(CrawlerIteration).where(
                    CrawlerIteration.crawler_job_id == job_id,
                    CrawlerIteration.token == symbol,
                )
            )
            it = r.scalar_one_or_none()
            if it is None:
                it = CrawlerIteration(
                    crawler_job_id=job_id,
                    token=symbol,
                    start=now,
                    stop=None,
                    done=False,
                    status="init",
                    last_update=now,
                )
                db.add(it)
                db.flush()

            it.last_update = now
            if symbol in pair_by_base:
                p = pair_by_base[symbol]
                it.symbol = p.code
                it.currency_pair = p.as_dict()
                it.status = "pending"
                it.comment = None
            else:
                it.status = "ignore"
                it.comment = "missing in exchange" if symbol not in bases_on_exchange else "missing in tokens list"
            iterations.append(it)

        db.flush()
        for it in iterations:
            db.refresh(it)
        return iterations

    async def run_once(self, iter: CrawlerIteration) -> None:
        asyncio.to_thread
        """Запуск обхода. job — существующий CrawlerJob или None (создать новый)."""
        raise NotImplementedError("Implement run(): load tokens, create iterations, fetch pairs/depth/klines/funding")
