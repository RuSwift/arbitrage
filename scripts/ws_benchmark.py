#!/usr/bin/env python3
"""
Бенчмарк WebSocket по биржам: частота событий, замедление при росте числа символов,
порог отказа. Токены — топ CMC, шаг +10 символов.

Требует: Redis (для Throttler). CMC API ключ — из .env (COINMARKETCAP_API_KEY).

Запуск:
  python scripts/ws_benchmark.py --measure-sec 45 --step 10 --max-symbols 80
  python scripts/ws_benchmark.py --exchanges binance,gate --kind perpetual --cmc-top 100
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

# project root and .env
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

from dotenv import load_dotenv

_env_file = _project_root / ".env"
if _env_file.exists():
    load_dotenv(_env_file, override=False)

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
from app.cex.base import BaseCEXPerpetualConnector, BaseCEXSpotConnector, Callback
from app.cex.dto import BookDepth, BookTicker, CandleStick
from app.cex.ws_klines import WS_KLINE_UNSUPPORTED
from app.cex.rest_rate_limit import get_tracker
from app.market.coinmarketcap import CoinMarketCapConnector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EXCHANGES: dict[str, tuple[type[BaseCEXSpotConnector], type[BaseCEXPerpetualConnector]]] = {
    "binance": (BinanceSpotConnector, BinancePerpetualConnector),
    "bitfinex": (BitfinexSpotConnector, BitfinexPerpetualConnector),
    "bybit": (BybitSpotConnector, BybitPerpetualConnector),
    "gate": (GateSpotConnector, GatePerpetualConnector),
    "htx": (HtxSpotConnector, HtxPerpetualConnector),
    "kucoin": (KucoinSpotConnector, KucoinPerpetualConnector),
    "mexc": (MexcSpotConnector, MexcPerpetualConnector),
    "okx": (OkxSpotConnector, OkxPerpetualConnector),
}


@dataclass
class EventCounter(Callback):
    """Считает события по типам (ticker, bookdepth, kline) и фиксирует время каждого."""

    timestamps: list[float] = field(default_factory=list)
    n_ticker: int = 0
    n_bookdepth: int = 0
    n_kline: int = 0

    def handle(
        self,
        book: BookTicker | None = None,
        depth: BookDepth | None = None,
        kline: CandleStick | None = None,
    ) -> None:
        t = time.time()
        self.timestamps.append(t)
        if book is not None:
            self.n_ticker += 1
        if depth is not None:
            self.n_bookdepth += 1
        if kline is not None:
            self.n_kline += 1


@dataclass
class RunResult:
    exchange: str
    kind: str
    n_symbols: int
    events_total: int
    measure_sec: float
    events_per_sec: float
    avg_gap_ms: float | None
    max_gap_ms: float | None
    status: str
    # По типам событий (kline через WS в коннекторах не шлётся — 0)
    events_ticker: int = 0
    events_bookdepth: int = 0
    events_kline: int = 0
    error: str | None = None


def get_available_symbols(
    connector: BaseCEXSpotConnector | BaseCEXPerpetualConnector,
    kind: str,
    pair_codes: list[str],
) -> list[str]:
    """Символы (pair_codes), которые есть на бирже, в порядке pair_codes."""
    if kind == "spot":
        pairs = connector.get_pairs(symbols=pair_codes)
    else:
        pairs = connector.get_pairs(symbols=pair_codes)
    if not pairs:
        return []
    codes = [p.code for p in pairs]
    return codes


def run_one(
    exchange_id: str,
    kind: str,
    symbols: list[str],
    measure_sec: float,
    baseline_eps: float | None,
) -> RunResult:
    """Один прогон: подписка на symbols, сбор событий measure_sec, статистика."""
    spot_cls, perp_cls = EXCHANGES[exchange_id]
    conn: BaseCEXSpotConnector | BaseCEXPerpetualConnector = (
        spot_cls(log=logger) if kind == "spot" else perp_cls(log=logger)
    )
    cb = EventCounter()
    try:
        conn.start(cb, symbols=symbols, depth=True, klines=True)
    except Exception as e:
        return RunResult(
            exchange=exchange_id,
            kind=kind,
            n_symbols=len(symbols),
            events_total=0,
            measure_sec=measure_sec,
            events_per_sec=0.0,
            avg_gap_ms=None,
            max_gap_ms=None,
            status="broken",
            error=str(e),
            events_ticker=0,
            events_bookdepth=0,
            events_kline=0,
        )
    time.sleep(measure_sec)
    try:
        conn.stop()
    except Exception:
        pass
    time.sleep(1)

    timestamps = cb.timestamps
    total = len(timestamps)
    elapsed = measure_sec
    eps = total / elapsed if elapsed > 0 else 0.0

    gaps_ms: list[float] = []
    if len(timestamps) >= 2:
        for i in range(1, len(timestamps)):
            gaps_ms.append((timestamps[i] - timestamps[i - 1]) * 1000)
    avg_gap = (sum(gaps_ms) / len(gaps_ms)) if gaps_ms else None
    max_gap = max(gaps_ms) if gaps_ms else None

    if total == 0:
        status = "broken"
    elif baseline_eps is not None and baseline_eps > 0 and eps < baseline_eps * 0.2:
        status = "degraded"
    else:
        status = "ok"

    return RunResult(
        exchange=exchange_id,
        kind=kind,
        n_symbols=len(symbols),
        events_total=total,
        measure_sec=measure_sec,
        events_per_sec=round(eps, 2),
        avg_gap_ms=round(avg_gap, 2) if avg_gap is not None else None,
        max_gap_ms=round(max_gap, 2) if max_gap is not None else None,
        status=status,
        events_ticker=cb.n_ticker,
        events_bookdepth=cb.n_bookdepth,
        events_kline=cb.n_kline,
    )


def build_markdown(
    results: list[RunResult],
    exchange_ids: list[str],
    kinds: list[str],
    step: int,
    measure_sec: float,
    available_counts: dict[tuple[str, str], int] | None = None,
) -> tuple[str, list[str]]:
    """Собирает markdown-таблицу и summary из текущих results."""
    rows: list[list[str]] = []
    rows.append([
        "exchange", "kind", "n_symbols", "events_total", "events/s",
        "ticker (n/s)", "bookdepth (n/s)", "kline (n/s)",
        "avg_gap_ms", "max_gap_ms", "status",
    ])
    for r in results:
        sec = r.measure_sec or measure_sec
        ticker_s = f"{r.events_ticker}/{r.events_ticker / sec:.1f}" if sec and r.events_ticker else "0/0"
        bd_s = f"{r.events_bookdepth}/{r.events_bookdepth / sec:.1f}" if sec and r.events_bookdepth else "0/0"
        kline_s = f"{r.events_kline}/{r.events_kline / sec:.1f}" if sec and r.events_kline else "0/0"
        rows.append([
            r.exchange,
            r.kind,
            str(r.n_symbols),
            str(r.events_total),
            f"{r.events_per_sec:.1f}",
            ticker_s,
            bd_s,
            kline_s,
            f"{r.avg_gap_ms:.1f}" if r.avg_gap_ms is not None else "-",
            f"{r.max_gap_ms:.1f}" if r.max_gap_ms is not None else "-",
            r.status,
        ])
    sep = "|"
    header = sep + sep.join(f" {rows[0][j]} " for j in range(len(rows[0]))) + sep
    align = sep + sep.join(" --- " for _ in rows[0]) + sep
    body = [sep + sep.join(f" {row[j]} " for j in range(len(row))) + sep for row in rows[1:]]
    table = "\n".join([header, align] + body)

    summary: list[str] = []
    for ex in exchange_ids:
        for k in kinds:
            run_results = [r for r in results if r.exchange == ex and r.kind == k]
            if not run_results:
                continue
            last_working_n = 0
            first_broken_n: int | None = None
            for r in run_results:
                if r.status == "broken":
                    if first_broken_n is None:
                        first_broken_n = r.n_symbols
                else:
                    last_working_n = r.n_symbols
            line = f"{ex} {k}: last_working n={last_working_n}"
            if first_broken_n is not None:
                line += f", first_broken n={first_broken_n}"
            if available_counts is not None:
                key = (ex, k)
                if key in available_counts:
                    line += f", available={available_counts[key]}"
            summary.append(line)

    title = "WebSocket benchmark: event rate vs symbol count (step +%d, measure %.0fs)" % (step, measure_sec)
    full = title + "\n\n"
    full += "*n_symbols ограничен числом доступных пар на бирже: max n = min(--max-symbols, available).*\n\n"
    full += table + "\n\n"
    if summary:
        full += "Summary:\n" + "\n".join(summary) + "\n"
    unsupported = ", ".join(f"{ex}/{k}" for ex, k in WS_KLINE_UNSUPPORTED)
    full += f"\n*Статистика: ticker = book_ticker, bookdepth = order_book_update; kline по WS — где поддерживается; 0 для CEX без WS klines: {unsupported}.*\n"
    return full, summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="WebSocket benchmark: event rate vs symbol count (CMC top tokens, step +10)."
    )
    parser.add_argument("--cmc-top", type=int, default=150, help="Top N CMC tokens to use (default 150)")
    parser.add_argument("--step", type=int, default=10, help="Symbol count step (default 10)")
    parser.add_argument("--max-symbols", type=int, default=100, help="Max symbols per run (default 100)")
    parser.add_argument("--measure-sec", type=float, default=45.0, help="Seconds to collect events per run (default 45)")
    parser.add_argument(
        "--exchanges",
        type=str,
        default=",".join(EXCHANGES),
        help="Comma-separated exchange ids (default: all)",
    )
    parser.add_argument(
        "--kind",
        choices=("spot", "perpetual", "both"),
        default="both",
        help="Market type (default both)",
    )
    parser.add_argument("--out", type=str, default="", help="Write markdown table to this file")
    args = parser.parse_args()

    get_tracker(window_sec=60.0)

    logger.info("Loading CMC top %d tokens...", args.cmc_top)
    cmc = CoinMarketCapConnector()
    tokens = cmc.get_top_tokens(limit=args.cmc_top)
    if not tokens:
        logger.error("No CMC tokens. Set COINMARKETCAP_API_KEY.")
        return 1
    pair_codes = [f"{t.symbol}/USDT" for t in tokens]
    logger.info("Loaded %d pair codes from CMC", len(pair_codes))

    exchange_ids = [x.strip() for x in args.exchanges.split(",") if x.strip()]
    kinds: list[str] = ["spot", "perpetual"] if args.kind == "both" else [args.kind]
    results: list[RunResult] = []
    available_counts: dict[tuple[str, str], int] = {}

    for exchange_id in exchange_ids:
        if exchange_id not in EXCHANGES:
            logger.warning("Unknown exchange %s, skip", exchange_id)
            continue
        for kind in kinds:
            spot_cls, perp_cls = EXCHANGES[exchange_id]
            conn = spot_cls(log=logger) if kind == "spot" else perp_cls(log=logger)
            logger.info("Resolving available symbols for %s %s...", exchange_id, kind)
            available = get_available_symbols(conn, kind, pair_codes)
            if len(available) < args.step:
                logger.warning("%s %s: only %d symbols available, skip", exchange_id, kind, len(available))
                continue
            available_counts[(exchange_id, kind)] = len(available)
            logger.info("%s %s: %d symbols available (max n_symbols = min(--max-symbols, %d))", exchange_id, kind, len(available), len(available))

            baseline_eps: float | None = None
            for n in range(args.step, min(args.max_symbols, len(available)) + 1, args.step):
                syms = available[:n]
                logger.info("Run %s %s n=%d (measure %.0fs)...", exchange_id, kind, n, args.measure_sec)
                r = run_one(exchange_id, kind, syms, args.measure_sec, baseline_eps)
                results.append(r)
                if r.status == "broken" and r.error:
                    logger.warning("%s %s n=%d broken: %s", exchange_id, kind, n, r.error)
                else:
                    logger.info(
                        "  %s n=%d -> %d events (ticker=%d bookdepth=%d), %.1f/s, avg_gap=%.0fms status=%s",
                        exchange_id, n, r.events_total, r.events_ticker, r.events_bookdepth,
                        r.events_per_sec, r.avg_gap_ms or 0, r.status,
                    )
                if baseline_eps is None and r.events_total > 0:
                    baseline_eps = r.events_per_sec
                # Обновить markdown вживую (файл и вывод)
                md_text, _ = build_markdown(
                    results, exchange_ids, kinds, args.step, args.measure_sec, available_counts
                )
                if args.out:
                    Path(args.out).write_text(md_text, encoding="utf-8")
                    logger.info("  Table updated (%d rows) -> %s", len(results), args.out)
                if r.status == "broken":
                    break

    md_text, summary = build_markdown(
        results, exchange_ids, kinds, args.step, args.measure_sec, available_counts
    )
    print("\n" + "=" * 80)
    print(md_text)
    if args.out:
        Path(args.out).write_text(md_text, encoding="utf-8")
        logger.info("Final table written to %s", args.out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
