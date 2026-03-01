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

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.services.tokens import TokensService
from app.services.unit_of_work import UnitOfWork
from app.settings import Settings

logger = logging.getLogger(__name__)


async def _cmc_setup(log: logging.Logger) -> None:
    """Обновляет токены source=coinmarketcap через TokensService.cmc_setup."""
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
    finally:
        await engine.dispose()


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
        asyncio.run(_cmc_setup(logger))
        logger.info("crawler2 finished (cmc_setup done)")
        return 0
    except Exception as e:
        logger.exception("crawler2 failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
