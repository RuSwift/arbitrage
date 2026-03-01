#!/usr/bin/env python3
"""
Скрипт обхода (новая версия): пока только cmc_setup (TokensService). Логику crawler пишем с нуля.

Запуск:
  python scripts/crawler2.py --exchange-id bybit --kind spot --cmc-top 500
  python scripts/crawler2.py --exchange-id binance --kind perpetual --cmc-top 200
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

from sqlalchemy import create_engine, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session as SyncSession, sessionmaker

from app.db.models import CrawlerJob

import redis
from redis.asyncio import from_url as redis_from_url

from app.services.crawlers.perpetual import CEXPerpetualCrawler
from app.services.tokens import TokensService
from app.services.unit_of_work import UnitOfWork
from app.settings import ServiceConfigRegistry, Settings

logger = logging.getLogger(__name__)


async def _run_perpetual(
    log: logging.Logger,
    exchange_ids: tuple[str, ...],
    kind: str,
) -> None:
    """cmc_setup, затем для kind=perpetual — prepare_job и prepare_job_iterations по каждой бирже."""
    settings = Settings()
    engine = create_async_engine(
        settings.database.async_url,
        echo=settings.database.echo,
        pool_size=settings.database.pool_size,
        max_overflow=settings.database.max_overflow,
    )
    async_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        autocommit=False,
        autoflush=False,
    )
    try:
        async with redis_from_url(settings.redis.url) as redis_client:
            async with async_factory() as db:
                uow = UnitOfWork(db=db, redis=redis_client)
                svc = TokensService(uow)
                log.info("Running cmc_setup (TokensService)...")
                await svc.cmc_setup()
                await db.commit()
                log.info("cmc_setup completed.")

                if kind != "perpetual":
                    return
                config = await ServiceConfigRegistry.aget(
                    db, "PerpetualCrawler", CEXPerpetualCrawler.Config
                )
                if config is None:
                    config = CEXPerpetualCrawler.Config()
                    await ServiceConfigRegistry.aset(db, "PerpetualCrawler", config)
                # Явно синхронный URL (psycopg2), чтобы в потоках не использовать asyncpg — иначе MissingGreenlet
                sync_url = settings.database.url
                if "+asyncpg" in sync_url:
                    sync_url = sync_url.replace("+asyncpg", "+psycopg2")
                elif sync_url.startswith("postgresql://"):
                    sync_url = "postgresql+psycopg2://" + sync_url[len("postgresql://") :]
                # Пул достаточный для параллельных потоков run_once (по одному соединению на поток)
                sync_engine = create_engine(
                    sync_url,
                    echo=settings.database.echo,
                    pool_size=20,
                    max_overflow=30,
                )
                sync_factory = sessionmaker(
                    bind=sync_engine,
                    class_=SyncSession,
                    autocommit=False,
                    autoflush=False,
                )
                sync_redis = redis.from_url(settings.redis.url)
                exchanges_iterations: dict[str, tuple[CEXPerpetualCrawler, list[int]]] = {}
                try:
                    for exchange_id in exchange_ids:
                        job_id: int | None = None
                        try:
                            crawler = CEXPerpetualCrawler(uow, exchange_id)
                            log.info("prepare_job exchange_id=%s", exchange_id)
                            job = await crawler.prepare_job()
                            job_id = job.id
                            await db.commit()
                            iteration_ids = await asyncio.to_thread(
                                _prepare_job_iterations_in_thread,
                                crawler,
                                job_id,
                                sync_factory,
                                sync_redis,
                                config,
                            )
                            job.error = None
                            await db.commit()
                            exchanges_iterations[exchange_id] = (crawler, iteration_ids)
                            log.info(
                                "prepare_job_iterations exchange_id=%s job_id=%s iterations=%s",
                                exchange_id,
                                job_id,
                                len(iteration_ids),
                            )
                        except Exception as e:
                            log.exception(
                                "job failed exchange_id=%s job_id=%s: %s",
                                exchange_id,
                                job_id,
                                e,
                            )
                            if job_id is not None:
                                await db.execute(update(CrawlerJob).where(CrawlerJob.id == job_id).values(error=str(e)))
                                await db.commit()

                    # run_once по всем итерациям: для каждой биржи — в отдельных нитках, биржи параллельно
                    async def run_once_for_exchange(
                        exchange_id: str,
                        crawler: CEXPerpetualCrawler,
                        iter_ids: list[int],
                    ) -> None:
                        if not iter_ids:
                            return
                        results = await asyncio.gather(
                            *[
                                asyncio.to_thread(
                                    _run_once_in_thread,
                                    crawler,
                                    iter_id,
                                    sync_factory,
                                    sync_redis,
                                    config,
                                )
                                for iter_id in iter_ids
                            ],
                            return_exceptions=True,
                        )
                        failed = [(iter_ids[i], r) for i, r in enumerate(results) if isinstance(r, BaseException)]
                        if failed:
                            for iter_id, exc in failed:
                                log.warning(
                                    "run_once failed exchange_id=%s iter_id=%s: %s",
                                    exchange_id,
                                    iter_id,
                                    exc,
                                )
                            log.warning(
                                "run_once exchange_id=%s done=%s failed=%s",
                                exchange_id,
                                len(iter_ids) - len(failed),
                                len(failed),
                            )
                        else:
                            log.info("run_once completed exchange_id=%s iterations=%s", exchange_id, len(iter_ids))

                    exchange_results = await asyncio.gather(
                        *[
                            run_once_for_exchange(ex_id, crawler, iter_ids)
                            for ex_id, (crawler, iter_ids) in exchanges_iterations.items()
                        ],
                        return_exceptions=True,
                    )
                    for ex_id, r in zip(exchanges_iterations, exchange_results):
                        if isinstance(r, BaseException):
                            log.error("run_once_for_exchange failed exchange_id=%s: %s", ex_id, r)
                finally:
                    sync_redis.close()
                    sync_engine.dispose()
    finally:
        await engine.dispose()


def _prepare_job_iterations_in_thread(
    crawler: CEXPerpetualCrawler,
    job_id: int,
    sync_factory,
    sync_redis,
    config,
):
    """Выполняет prepare_job_iterations в отдельном потоке. Возвращает только id итераций, не ORM — иначе DetachedInstanceError при использовании в другом контексте."""
    with sync_factory() as sync_db:
        iterations = crawler.prepare_job_iterations(job_id, sync_db, sync_redis, config)
        sync_db.commit()
        return [it.id for it in iterations]


def _run_once_in_thread(
    crawler: CEXPerpetualCrawler,
    iter_id: int,
    sync_factory,
    sync_redis,
    config,
) -> None:
    """Выполняет crawler.run_once(iter_id, db, redis, config) в отдельном потоке с отдельной sync-сессией."""
    with sync_factory() as sync_db:
        crawler.run_once(iter_id, sync_db, sync_redis, config)
        sync_db.commit()


EXCHANGE_IDS = ("binance", "bitfinex", "bybit", "gate", "htx", "kucoin", "mexc", "okx")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Crawler v2: cmc_setup (далее логика с нуля)."
    )
    parser.add_argument(
        "--all-exchanges",
        action="store_true",
        help="Crawl all supported exchanges.",
    )
    parser.add_argument(
        "--exchange-id",
        choices=list(EXCHANGE_IDS),
        help="Exchange to crawl (e.g. bybit, binance). Ignored if --all-exchanges is set.",
    )
    parser.add_argument(
        "--kind",
        required=True,
        choices=["spot", "perpetual"],
        help="Connector kind: spot or perpetual.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    if args.all_exchanges:
        exchange_ids = EXCHANGE_IDS
    elif args.exchange_id is not None:
        exchange_ids = (args.exchange_id,)
    else:
        parser.error("either --exchange-id or --all-exchanges is required")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        logger.info(
            "crawler2: exchanges=%s kind=%s",
            exchange_ids if len(exchange_ids) <= 3 else f"{len(exchange_ids)} exchanges",
            args.kind,
        )
        asyncio.run(_run_perpetual(logger, exchange_ids=exchange_ids, kind=args.kind))
        logger.info("crawler2 finished")
        return 0
    except Exception as e:
        logger.exception("crawler2 failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
