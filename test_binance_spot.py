#!/usr/bin/env python3
"""Quick test for Binance spot connector: REST + WebSocket stream (Ctrl+C to stop)."""

import signal
import sys
import time

from app.cex.binance import BinanceSpotConnector
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
    conn = BinanceSpotConnector(is_testing=False)
    print("Binance spot: get_all_tickers ...")
    tickers = conn.get_all_tickers()
    print(f"  tickers: {len(tickers)}")
    pair_code = "BTC/USDT"
    print(f"  get_price({pair_code!r}) ...")
    price = conn.get_price(pair_code)
    if price:
        print(f"  price: {price.ratio}")
    else:
        print("  price: (none)")
    print("  get_depth ...")
    depth = conn.get_depth(pair_code, limit=5)
    if depth:
        print(f"  depth bids={len(depth.bids)} asks={len(depth.asks)}")
    print("  get_klines ...")
    klines = conn.get_klines(pair_code)
    if klines:
        print(f"  klines: {len(klines)} candles")
    print("  starting WebSocket for BTC/USDT (Ctrl+C to stop) ...")
    conn.start(cb=PrintCallback(), symbols=[pair_code], depth=True)
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
