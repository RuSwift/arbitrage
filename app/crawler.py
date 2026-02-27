"""
Скрипт обхода котировок по бирже: загрузка токенов из CMC, создание CrawlerJob/CrawlerIteration,
заполнение цен (get_pairs), затем по каждому токену depth, klines, funding_rate (для фьючерсов).

Запуск:
  python -m app.crawler --exchange-id bybit --kind spot --cmc-top 500
  python -m app.crawler --exchange-id binance --kind perpetual --cmc-top 200
"""

from __future__ import annotations

import argparse
import logging
import sys
import traceback
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.cex import (
    BinancePerpetualConnector,
    BinanceSpotConnector,
    BitfinexPerpetualConnector,
    BitfinexSpotConnector,
    BybitPerpetualConnector,
    BybitSpotConnector,
    GatePerpetualConnector,
    GateSpotConnector,
    HtxPerpetualConnector,
    HtxSpotConnector,
    KucoinPerpetualConnector,
    KucoinSpotConnector,
    MexcPerpetualConnector,
    MexcSpotConnector,
    OkxPerpetualConnector,
    OkxSpotConnector,
)
from app.cex.base import BaseCEXPerpetualConnector, BaseCEXSpotConnector
from app.cex.dto import (
    BookDepth,
    CandleStick,
    CurrencyPair,
    FundingRate,
    FundingRatePoint,
)
from app.db.models import CrawlerIteration, CrawlerJob
from app.market.coinmarketcap import CoinMarketCapConnector
from app.settings import Settings

logger = logging.getLogger(__name__)

# Маппинг exchange_id -> (SpotConnectorClass, PerpetualConnectorClass)
_EXCHANGE_CONNECTORS: dict[str, tuple[type[BaseCEXSpotConnector], type[BaseCEXPerpetualConnector]]] = {
    "binance": (BinanceSpotConnector, BinancePerpetualConnector),
    "bitfinex": (BitfinexSpotConnector, BitfinexPerpetualConnector),
    "bybit": (BybitSpotConnector, BybitPerpetualConnector),
    "gate": (GateSpotConnector, GatePerpetualConnector),
    "htx": (HtxSpotConnector, HtxPerpetualConnector),
    "kucoin": (KucoinSpotConnector, KucoinPerpetualConnector),
    "mexc": (MexcSpotConnector, MexcPerpetualConnector),
    "okx": (OkxSpotConnector, OkxPerpetualConnector),
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _get_connector(exchange_id: str, kind: str, log: logging.Logger | None = None) -> BaseCEXSpotConnector | BaseCEXPerpetualConnector:
    if exchange_id not in _EXCHANGE_CONNECTORS:
        raise ValueError(f"Unknown exchange_id={exchange_id!r}. Valid: {list(_EXCHANGE_CONNECTORS.keys())}")
    if kind not in ("spot", "perpetual"):
        raise ValueError(f"kind must be 'spot' or 'perpetual', got {kind!r}")
    spot_cls, perp_cls = _EXCHANGE_CONNECTORS[exchange_id]
    conn_cls = spot_cls if kind == "spot" else perp_cls
    return conn_cls(log=log)


def _run(
    exchange_id: str,
    kind: str,
    cmc_top: int,
    db_session: Session,
    log: logging.Logger | None = None,
) -> None:
    log = log or logger
    job_start = _utc_now()
    log.info(
        "Crawler run started: exchange_id=%s kind=%s cmc_top=%s",
        exchange_id,
        kind,
        cmc_top,
    )

    # 1. Загрузить список токенов CMC
    log.info("Loading tokens from CoinMarketCap (limit=%d)...", cmc_top)
    cmc = CoinMarketCapConnector()
    tokens = cmc.get_top_tokens(limit=cmc_top)
    if not tokens:
        raise RuntimeError("CoinMarketCap returned no tokens. Check API key and limit.")
    symbols = [t.symbol for t in tokens]
    log.info("Loaded %d tokens from CMC (top %d)", len(symbols), cmc_top)

    # 2. Создать CrawlerJob и CrawlerIteration (status=init) по каждому токену
    job = CrawlerJob(
        exchange=exchange_id,
        connector=kind,
        start=job_start,
        stop=None,
        error=None,
    )
    db_session.add(job)
    db_session.flush()

    pair_codes = [f"{s}/USDT" for s in symbols]
    now = _utc_now()
    for sym in symbols:
        it = CrawlerIteration(
            crawler_job_id=job.id,
            token=sym,
            start=now,
            stop=None,
            done=False,
            status="init",
            comment=None,
            error=None,
            last_update=now,
        )
        db_session.add(it)
    db_session.commit()
    log.info("Created CrawlerJob id=%s with %d iterations (status=init)", job.id, len(symbols))

    # 3. Заполнить цены через get_pairs(symbols)
    log.info("Connecting to %s %s and fetching pairs for %d symbols...", exchange_id, kind, len(pair_codes))
    conn = _get_connector(exchange_id, kind, log=log)
    if kind == "spot":
        conn.get_all_tickers()
    else:
        conn.get_all_perpetuals()

    pairs = conn.get_pairs(symbols=pair_codes)
    pair_by_code: dict[str, CurrencyPair] = {}
    for p in pairs or []:
        pair_by_code[p.code] = p

    for it in db_session.query(CrawlerIteration).filter(CrawlerIteration.crawler_job_id == job.id):
        pair_code = f"{it.token}/USDT"
        if pair_code in pair_by_code:
            it.currency_pair = pair_by_code[pair_code].as_dict()
            it.status = "pending"
        else:
            it.status = "ignore"
            it.comment = "not on exchange"
        it.last_update = _utc_now()
    db_session.commit()
    ignored_count = len(symbols) - len(pair_by_code)
    log.info(
        "Filled currency_pair for %d iterations, ignored %d (not on exchange)",
        len(pair_by_code),
        ignored_count,
    )

    # 4. Итеративно по токенам: get_depth, get_klines, get_funding_rate (для фьючерсов)
    pending = (
        db_session.query(CrawlerIteration)
        .filter(CrawlerIteration.crawler_job_id == job.id, CrawlerIteration.status == "pending")
        .order_by(CrawlerIteration.id)
        .all()
    )
    log.info("Processing %d pending iterations (depth, klines, funding_rate)...", len(pending))
    success_count = 0
    error_count = 0
    for idx, it in enumerate(pending):
        pair_code = f"{it.token}/USDT"
        it.status = "pending"
        it.last_update = _utc_now()
        try:
            depth = conn.get_depth(pair_code)
            if depth:
                it.book_depth = depth.as_dict()

            klines = conn.get_klines(pair_code)
            if klines:
                it.klines = [c.as_dict() for c in klines]

            if kind == "perpetual" and isinstance(conn, BaseCEXPerpetualConnector):
                fr = conn.get_funding_rate(pair_code)
                if fr:
                    it.funding_rate = fr.as_dict()
                hist = conn.get_funding_rate_history(pair_code)
                if hist:
                    it.funding_rate_history = [h.as_dict() for h in hist]

            it.done = True
            it.status = "success"
            it.stop = _utc_now()
            it.last_update = it.stop
            success_count += 1
        except Exception as e:
            it.status = "error"
            it.error = traceback.format_exc()
            it.last_update = _utc_now()
            it.stop = _utc_now()
            error_count += 1
            log.exception("Iteration token=%s failed: %s", it.token, e)
        db_session.commit()
        if (idx + 1) % 50 == 0 or idx + 1 == len(pending):
            log.info("Progress: %d/%d iterations (success=%d, error=%d)", idx + 1, len(pending), success_count, error_count)

    job.stop = _utc_now()
    db_session.commit()
    log.info(
        "CrawlerJob id=%s finished: success=%d, error=%d, ignored=%d, total_time_sec=%.1f",
        job.id,
        success_count,
        error_count,
        ignored_count,
        (job.stop - job_start).total_seconds() if job.stop and job_start else 0,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Crawl exchange quotes: CMC tokens -> CrawlerJob/CrawlerIteration -> get_pairs, depth, klines, funding_rate."
    )
    parser.add_argument(
        "--exchange-id",
        required=True,
        choices=list(_EXCHANGE_CONNECTORS.keys()),
        help="Exchange to crawl (e.g. bybit, binance).",
    )
    parser.add_argument(
        "--kind",
        required=True,
        choices=["spot", "perpetual"],
        help="Connector kind: spot or perpetual.",
    )
    parser.add_argument(
        "--cmc-top",
        type=int,
        default=500,
        help="Number of top tokens from CoinMarketCap (default 500).",
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

    db_url = Settings().database.url
    engine = create_engine(db_url, echo=False)
    SessionLocal = sessionmaker(engine, autocommit=False, autoflush=False)
    session = SessionLocal()

    try:
        logger.info(
            "Starting crawler: exchange_id=%s kind=%s cmc_top=%s",
            args.exchange_id,
            args.kind,
            args.cmc_top,
        )
        _run(
            exchange_id=args.exchange_id,
            kind=args.kind,
            cmc_top=args.cmc_top,
            db_session=session,
            log=logger,
        )
        logger.info("Crawler finished successfully")
        return 0
    except Exception as e:
        logger.exception("Crawler failed: %s", e)
        return 1
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
