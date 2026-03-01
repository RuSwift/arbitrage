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

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session as SyncSession, sessionmaker

from app.services.crawlers.perpetual import CEXPerpetualCrawler
from app.services.tokens import TokensService
from app.services.unit_of_work import UnitOfWork
from app.settings import Settings

logger = logging.getLogger(__name__)


async def _run(
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
        from redis.asyncio import from_url as redis_from_url

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
                sync_engine = create_engine(
                    settings.database.url,
                    echo=settings.database.echo,
                    pool_size=1,
                    max_overflow=0,
                )
                sync_factory = sessionmaker(
                    bind=sync_engine,
                    class_=SyncSession,
                    autocommit=False,
                    autoflush=False,
                )
                try:
                    for exchange_id in exchange_ids:
                        job = None
                        try:
                            crawler = CEXPerpetualCrawler(uow, exchange_id)
                            log.info("prepare_job exchange_id=%s", exchange_id)
                            job = await crawler.prepare_job()
                            job_id = job.id
                            await db.commit()
                            iterations = await asyncio.to_thread(
                                _prepare_job_iterations_in_thread,
                                crawler,
                                job_id,
                                sync_factory,
                            )
                            job.error = None
                            await db.commit()
                            log.info(
                                "prepare_job_iterations exchange_id=%s job_id=%s iterations=%s",
                                exchange_id,
                                job_id,
                                len(iterations),
                            )
                        except Exception as e:
                            log.exception(
                                "job failed exchange_id=%s job_id=%s: %s",
                                exchange_id,
                                job.id if job is not None else None,
                                e,
                            )
                            if job is not None:
                                job.error = str(e)
                                await db.commit()
                finally:
                    sync_engine.dispose()
    finally:
        await engine.dispose()


def _prepare_job_iterations_in_thread(
    crawler: CEXPerpetualCrawler,
    job_id: int,
    sync_factory,
):
    """Выполняет prepare_job_iterations в отдельном потоке. Передаём только job_id, не ORM job — иначе MissingGreenlet (async-сессия в другом потоке)."""
    with sync_factory() as sync_db:
        iterations = crawler.prepare_job_iterations(job_id, sync_db)
        sync_db.commit()
        return iterations


EXCHANGE_IDS = ("binance", "bitfinex", "bybit", "gate", "htx", "kucoin", "mexc", "okx")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Crawler v2: cmc_setup (далее логика с нуля)."
    )
    parser.add_argument(
        "--exchange-id",
        required=True,
        choices=list(EXCHANGE_IDS),
        help="Exchange to crawl (e.g. bybit, binance).",
    )
    parser.add_argument(
        "--kind",
        required=True,
        choices=["spot", "perpetual"],
        help="Connector kind: spot or perpetual.",
    )
    parser.add_argument(
        "--window-sec",
        type=int,
        default=60,
        help="Rate limit window in seconds (default 60).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        logger.info(
            "crawler2: exchange_id=%s kind=%s",
            args.exchange_id,
            args.kind,
        )
        exchange_ids = (args.exchange_id,)
        asyncio.run(_run(logger, exchange_ids=exchange_ids, kind=args.kind))
        logger.info("crawler2 finished")
        return 0
    except Exception as e:
        logger.exception("crawler2 failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
