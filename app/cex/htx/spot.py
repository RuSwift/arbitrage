"""HTX (Huobi) spot connector (REST + WebSocket)."""

from __future__ import annotations

import gzip
import json
import logging
import threading
import time
from typing import Any

import websocket

from app.cex.base import BaseCEXSpotConnector, Callback
from app.cex.dto import (
    BidAsk,
    BookDepth,
    BookTicker,
    CandleStick,
    CurrencyPair,
    Ticker,
)

HTX_API_HOST = "https://api.huobi.pro"
HTX_WS_URL = "wss://api.huobi.pro/ws/"
QUOTES = ("USDT", "BTC", "ETH", "USDC", "TUSD", "HT")


def _utc_now_float() -> float:
    return time.time()


def _exchange_to_symbol(exchange_symbol: str) -> str | None:
    ex = exchange_symbol.upper()
    for q in QUOTES:
        if ex.endswith(q):
            return f"{ex[:-len(q)]}/{q}"
    return None


def _build_tickers_dict(tickers: list[Ticker]) -> dict[str, Ticker]:
    out: dict[str, Ticker] = {}
    for t in tickers:
        out[t.symbol] = t
        if t.exchange_symbol:
            out[t.exchange_symbol] = t
            out[t.exchange_symbol.upper()] = t
        out[t.symbol.replace("/", "")] = t
    return out


class HtxSpotConnector(BaseCEXSpotConnector):
    REQUEST_TIMEOUT_SEC = 15
    DEPTH_API_MAX = 20
    KLINE_SIZE = 60
    WS_CONNECT_WAIT_ATTEMPTS = 10
    WS_CONNECT_WAIT_SEC = 1

    def __init__(
        self,
        is_testing: bool = False,
        throttle_timeout: float = 1.0,
        log: logging.Logger | None = None,
    ) -> None:
        super().__init__(is_testing=is_testing, throttle_timeout=throttle_timeout, log=log)
        self._cached_tickers: list[Ticker] | None = None
        self._cached_tickers_dict: dict[str, Ticker] = {}
        self._ws: websocket.WebSocketApp | None = None
        self._ws_thread: threading.Thread | None = None
        self._cb: Callback | None = None

    @classmethod
    def exchange_id(cls) -> str:
        return "htx"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = HTX_API_HOST + path
        r = self._request_limited(url, params or {}, self.REQUEST_TIMEOUT_SEC)
        r.raise_for_status()
        return r.json()

    def start(
        self,
        cb: Callback,
        symbols: list[str] | None = None,
        depth: bool = True,
    ) -> None:
        if self._ws is not None:
            raise RuntimeError("WebSocket already active. Call stop() first.")
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        if symbols is None:
            syms = [t.exchange_symbol for t in self._cached_tickers if t.exchange_symbol]
        else:
            syms = [
                t.exchange_symbol
                for t in self._cached_tickers
                if t.exchange_symbol and t.symbol in symbols
            ]
        self._cb = cb
        self._ws = websocket.WebSocketApp(
            HTX_WS_URL,
            on_message=self._on_ws_message,
        )
        self._ws_thread = threading.Thread(target=lambda: self._ws.run_forever())
        self._ws_thread.daemon = True
        self._ws_thread.start()
        for _ in range(self.WS_CONNECT_WAIT_ATTEMPTS):
            if self._ws.sock and self._ws.sock.connected:
                break
            time.sleep(self.WS_CONNECT_WAIT_SEC)
        if not self._ws.sock or not self._ws.sock.connected:
            self._ws = None
            self._ws_thread = None
            raise RuntimeError("HTX spot WebSocket connection failed.")
        for ex_sym in syms:
            self._ws.send(
                json.dumps({"sub": f"market.{ex_sym.lower()}.bbo", "id": f"bbo_{ex_sym}"})
            )
            if depth:
                self._ws.send(
                    json.dumps(
                        {
                            "sub": f"market.{ex_sym.lower()}.depth.step0",
                            "id": f"depth_{ex_sym}",
                        }
                    )
                )

    def stop(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._ws_thread = None
        self._cb = None

    def get_all_tickers(self) -> list[Ticker]:
        if self._cached_tickers is not None:
            return self._cached_tickers
        data = self._get("/market/tickers")
        if "data" not in data:
            raise RuntimeError(data.get("err-msg", "Failed to get tickers"))
        tickers: list[Ticker] = []
        for d in data["data"]:
            sym = _exchange_to_symbol(d["symbol"])
            if not sym:
                continue
            base, quote = sym.split("/")
            tickers.append(
                Ticker(
                    symbol=sym,
                    base=base,
                    quote=quote,
                    is_spot_enabled=True,
                    is_margin_enabled=False,
                    exchange_symbol=d["symbol"],
                )
            )
        self._cached_tickers = tickers
        self._cached_tickers_dict = _build_tickers_dict(tickers)
        return tickers

    def get_price(self, pair_code: str) -> CurrencyPair | None:
        ex_sym = pair_code.replace("/", "").lower()
        data = self._get("/market/trade", {"symbol": ex_sym})
        if data.get("status") != "ok" or not data.get("tick", {}).get("data"):
            return None
        base, quote = pair_code.split("/")
        price = float(data["tick"]["data"][0]["price"])
        return CurrencyPair(
            base=base,
            quote=quote,
            ratio=price,
            utc=_utc_now_float(),
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        data = self._get("/market/tickers")
        if "data" not in data:
            raise RuntimeError(data.get("err-msg", "Failed to get tickers"))
        if symbols is None:
            want = set(self._cached_tickers_dict.keys()) | {
                t.exchange_symbol for t in self._cached_tickers if t.exchange_symbol
            }
        else:
            want = {s.replace("/", "").lower() for s in symbols}
        pairs: list[CurrencyPair] = []
        ts_ms = data.get("ts", 0) / 1000.0
        for d in data["data"]:
            if d["symbol"] not in want and d["symbol"].upper() not in want:
                continue
            sym = _exchange_to_symbol(d["symbol"])
            if not sym:
                continue
            base, quote = sym.split("/")
            mid = (float(d["bid"]) + float(d["ask"])) / 2
            pairs.append(
                CurrencyPair(base=base, quote=quote, ratio=mid, utc=ts_ms or _utc_now_float())
            )
        return pairs

    def get_depth(self, symbol: str, limit: int = 100) -> BookDepth | None:
        ex_sym = self._exchange_symbol(symbol)
        if not ex_sym:
            return None
        data = self._get(
            "/market/depth",
            {"symbol": ex_sym.lower(), "type": "step0", "depth": str(min(limit, self.DEPTH_API_MAX))},
        )
        if "tick" not in data:
            raise RuntimeError(data.get("err-msg", "Failed to get depth"))
        tick = data["tick"]
        bids = [BidAsk(price=float(r[0]), quantity=float(r[1])) for r in tick["bids"]]
        asks = [BidAsk(price=float(r[0]), quantity=float(r[1])) for r in tick["asks"]]
        return BookDepth(
            symbol=symbol,
            bids=bids,
            asks=asks,
            exchange_symbol=ex_sym,
            last_update_id=int(tick["ts"]),
            utc=float(tick["ts"]) / 1000,
        )

    def get_klines(self, symbol: str, limit: int | None = None) -> list[CandleStick] | None:
        ex_sym = self._exchange_symbol(symbol)
        if not ex_sym:
            return None
        n = limit if limit is not None else self.KLINE_SIZE
        data = self._get(
            "/market/history/kline",
            {"symbol": ex_sym.lower(), "period": "1min", "size": str(n)},
        )
        if "data" not in data:
            raise RuntimeError(data.get("err-msg", "Failed to get klines"))
        return [
            CandleStick(
                utc_open_time=float(k["id"]),
                open_price=float(k["open"]),
                high_price=float(k["high"]),
                low_price=float(k["low"]),
                close_price=float(k["close"]),
                coin_volume=float(k["amount"]),
                usd_volume=float(k["vol"]) if symbol.endswith("/USDT") else None,
            )
            for k in data["data"]
        ]

    def _exchange_symbol(self, symbol: str) -> str | None:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        t = (
            self._cached_tickers_dict.get(symbol)
            or self._cached_tickers_dict.get(symbol.replace("/", ""))
            or self._cached_tickers_dict.get(symbol.replace("/", "").upper())
        )
        return t.exchange_symbol if t else None

    def _on_ws_message(self, _: Any, raw: bytes | str) -> None:
        if not self._cb:
            return
        if isinstance(raw, bytes):
            raw = gzip.decompress(raw).decode()
        msg = json.loads(raw)
        if "ping" in msg:
            self._ws.send(json.dumps({"pong": msg["ping"]}))
            return
        ch = msg.get("ch", "")
        if ch.endswith(".bbo"):
            tick = msg.get("tick", {})
            if not tick:
                return
            sym = _exchange_to_symbol(tick.get("symbol", ""))
            if not sym or not self._throttler.may_pass(sym, tag="book"):
                return
            self._cb.handle(
                book=BookTicker(
                    symbol=sym,
                    bid_price=float(tick["bid"]),
                    bid_qty=float(tick["bidSize"]),
                    ask_price=float(tick["ask"]),
                    ask_qty=float(tick["askSize"]),
                    last_update_id=int(tick.get("seqId", 0)),
                    utc=float(tick.get("quoteTime", 0)) / 1000,
                )
            )
        elif ".depth." in ch:
            parts = ch.split(".")
            if len(parts) < 2:
                return
            ex_sym = parts[1]
            sym = _exchange_to_symbol(ex_sym)
            if not sym or not self._throttler.may_pass(sym, tag="depth"):
                return
            tick = msg.get("tick", {})
            self._cb.handle(
                depth=BookDepth(
                    symbol=sym,
                    bids=[BidAsk(price=float(r[0]), quantity=float(r[1])) for r in tick.get("bids", [])],
                    asks=[BidAsk(price=float(r[0]), quantity=float(r[1])) for r in tick.get("asks", [])],
                    exchange_symbol=ex_sym,
                    last_update_id=int(tick.get("ts", 0)),
                    utc=float(tick.get("ts", 0)) / 1000,
                )
            )
