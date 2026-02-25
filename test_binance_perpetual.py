#!/usr/bin/env python3
"""Quick test for Binance USD-M perpetual connector: REST + WebSocket stream (Ctrl+C to stop)."""

import signal
import sys
import time

from app.cex.binance import BinancePerpetualConnector
from app.cex.base import Callback
from app.cex.dto import BookDepth, BookTicker


class PrintCallback(Callback):
    def handle(
        self,
        book: BookTicker | None = None,
        depth: BookDepth | None = None,
    ) -> None:
        if book:
            print(f"  book {book.symbol} bid={book.bid_price} ask={book.ask_price}")
        if depth:
            bids = depth.bids[:3] if depth.bids else []
            asks = depth.asks[:3] if depth.asks else []
            print(f"  depth {depth.symbol} bids={bids} asks={asks}")


def main() -> None:
    conn = BinancePerpetualConnector(is_testing=False)
    print("Binance perpetual: get_all_perpetuals ...")
    perps = conn.get_all_perpetuals()
    print(f"  perpetuals: {len(perps)}")
    symbol = "BTC/USDT"
    print(f"  get_price({symbol!r}) ...")
    price = conn.get_price(symbol)
    if price:
        print(f"  price: {price.ratio}")
    else:
        print("  price: (none)")
    print("  get_depth ...")
    depth = conn.get_depth(symbol, limit=5)
    if depth:
        print(f"  depth bids={len(depth.bids)} asks={len(depth.asks)}")
    print("  get_klines ...")
    klines = conn.get_klines(symbol)
    if klines:
        print(f"  klines: {len(klines)} candles")
    print("  starting WebSocket for BTC/USDT (Ctrl+C to stop) ...")
    conn.start(cb=PrintCallback(), symbols=[symbol], depth=True)
    done = False

    def on_sig(_: int, __: object) -> None:
        nonlocal done
        done = True

    signal.signal(signal.SIGINT, on_sig)
    while not done:
        time.sleep(1)
    conn.stop()
    print("  stopped.")
    sys.exit(0)


if __name__ == "__main__":
    main()
