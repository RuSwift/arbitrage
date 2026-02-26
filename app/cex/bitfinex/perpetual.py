"""Bitfinex perpetual derivatives connector (REST + WebSocket)."""

from __future__ import annotations

import json
import threading
import time
from typing import Any

import requests
import websocket

from app.cex.base import BaseCEXPerpetualConnector, Callback
from app.cex.dto import (
    BidAsk,
    BookDepth,
    BookTicker,
    CandleStick,
    CurrencyPair,
    PerpetualTicker,
)

BITFINEX_API = "https://api-pub.bitfinex.com/v2"
BITFINEX_WS = "wss://api-pub.bitfinex.com/ws/2"
QUOTES = ("UST", "USDT")


def _utc_now_float() -> float:
    return time.time()


def _bfx_deriv_to_symbol(key: str) -> str:
    """tBTCF0:USTF0 -> BTC/USDT."""
    if ":" not in key:
        return key
    base_part, quote_part = key.split(":", 1)
    if base_part.startswith("t"):
        base_part = base_part[1:]
    if base_part.endswith("F0"):
        base_part = base_part[:-2]
    if quote_part.endswith("F0"):
        quote_part = quote_part[:-2]
    if quote_part == "UST":
        quote_part = "USDT"
    return f"{base_part}/{quote_part}"


def _symbol_to_bfx_deriv(symbol: str) -> str:
    """BTC/USDT -> tBTCF0:USTF0."""
    base, quote = symbol.split("/") if "/" in symbol else (symbol, "USDT")
    if quote == "USDT":
        quote = "UST"
    return f"t{base}F0:{quote}F0"


def _build_perp_dict(tickers: list[PerpetualTicker]) -> dict[str, PerpetualTicker]:
    out: dict[str, PerpetualTicker] = {}
    for t in tickers:
        out[t.symbol] = t
        out[t.exchange_symbol] = t
        out[t.symbol.replace("/", "")] = t
    return out


class BitfinexPerpetualConnector(BaseCEXPerpetualConnector):
    REQUEST_TIMEOUT_SEC = 15
    DEPTH_API_MAX = 100
    KLINE_SIZE = 60
    WS_CONNECT_WAIT_ATTEMPTS = 10
    WS_CONNECT_WAIT_SEC = 1

    def __init__(self, is_testing: bool = False, throttle_timeout: float = 1.0) -> None:
        super().__init__(is_testing=is_testing, throttle_timeout=throttle_timeout)
        self._cached_perps: list[PerpetualTicker] | None = None
        self._cached_perps_dict: dict[str, PerpetualTicker] = {}
        self._ws: websocket.WebSocketApp | None = None
        self._ws_thread: threading.Thread | None = None
        self._cb: Callback | None = None
        self._chan_to_sym: dict[int, str] = {}

    @classmethod
    def exchange_id(cls) -> str:
        return "bitfinex"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = BITFINEX_API + path
        r = requests.get(url, params=params or {}, timeout=self.REQUEST_TIMEOUT_SEC)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and len(data) > 0 and data[0] == "error":
            raise RuntimeError(data[2] if len(data) > 2 else str(data))
        return data

    def start(
        self,
        cb: Callback,
        symbols: list[str] | None = None,
        depth: bool = True,
    ) -> None:
        if self._ws is not None:
            raise RuntimeError("WebSocket already active. Call stop() first.")
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        if symbols is None:
            syms = [t.exchange_symbol for t in self._cached_perps]
        else:
            syms = [
                t.exchange_symbol
                for t in self._cached_perps
                if t.symbol in symbols or t.exchange_symbol in symbols
            ]
        if not syms:
            raise RuntimeError("No symbols to subscribe")
        self._cb = cb
        self._ws = websocket.WebSocketApp(BITFINEX_WS, on_message=self._on_ws_message)
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
            raise RuntimeError("Bitfinex deriv WebSocket connection failed.")
        for ex_sym in syms:
            self._ws.send(json.dumps({"event": "subscribe", "channel": "status", "key": f"deriv:{ex_sym}"}))

    def stop(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._ws_thread = None
        self._cb = None
        self._chan_to_sym = {}

    def get_all_perpetuals(self) -> list[PerpetualTicker]:
        if self._cached_perps is not None:
            return self._cached_perps
        try:
            data = self._get("/status/deriv", {"keys": "ALL"})
        except Exception:
            data = []
        if not isinstance(data, list):
            data = []
        perps: list[PerpetualTicker] = []
        for row in data:
            if not isinstance(row, list) or len(row) < 2 or row[0] == "error":
                continue
            key = row[0]
            if not isinstance(key, str) or "F0" not in key or ":" not in key:
                continue
            symbol = _bfx_deriv_to_symbol(key)
            base = symbol.split("/")[0]
            quote = symbol.split("/")[1] if "/" in symbol else "USDT"
            perps.append(
                PerpetualTicker(
                    symbol=symbol,
                    base=base,
                    quote=quote,
                    exchange_symbol=key,
                    settlement=quote,
                )
            )
        self._cached_perps = perps
        self._cached_perps_dict = _build_perp_dict(perps)
        return self._cached_perps

    def get_price(self, symbol: str) -> CurrencyPair | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_bfx_deriv(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get("/status/deriv", {"keys": ex_sym})
        except Exception:
            return None
        if not isinstance(data, list) or not data or (isinstance(data[0], list) and data[0][0] == "error"):
            return None
        row = data[0] if isinstance(data[0], list) else data
        if not isinstance(row, list) or len(row) < 16:
            return None
        mark = row[15]
        if mark is None:
            mark = row[3]
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(symbol)
        if not ticker:
            return None
        return CurrencyPair(
            base=ticker.base,
            quote=ticker.quote,
            ratio=float(mark),
            utc=_utc_now_float(),
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        try:
            data = self._get("/status/deriv", {"keys": "ALL"})
        except Exception:
            return []
        if not isinstance(data, list):
            return []
        result: list[CurrencyPair] = []
        want = None if symbols is None else {self._exchange_symbol(s) or _symbol_to_bfx_deriv(s) for s in symbols}
        for row in data:
            if not isinstance(row, list) or len(row) < 16 or row[0] == "error":
                continue
            key = row[0]
            if want is not None and key not in want:
                sym_norm = _bfx_deriv_to_symbol(key)
                if sym_norm not in (symbols or []):
                    continue
            ticker = self._cached_perps_dict.get(key)
            if not ticker:
                continue
            mark = row[15] if len(row) > 15 else row[3]
            if mark is None:
                continue
            result.append(
                CurrencyPair(
                    base=ticker.base,
                    quote=ticker.quote,
                    ratio=float(mark),
                    utc=_utc_now_float(),
                )
            )
        return result

    def get_depth(self, symbol: str, limit: int = 100) -> BookDepth | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_bfx_deriv(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get(f"/book/{ex_sym}/P0", {"len": str(min(limit, self.DEPTH_API_MAX))})
        except Exception:
            return None
        if not isinstance(data, list):
            return None
        bids = [x for x in data if isinstance(x, list) and len(x) >= 3 and x[2] > 0]
        asks = [x for x in data if isinstance(x, list) and len(x) >= 3 and x[2] < 0]
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        return BookDepth(
            symbol=sym,
            exchange_symbol=ex_sym,
            bids=[BidAsk(price=float(b[0]), quantity=float(b[2])) for b in bids[:limit]],
            asks=[BidAsk(price=float(a[0]), quantity=float(-a[2])) for a in asks[:limit]],
            last_update_id=None,
            utc=_utc_now_float(),
        )

    def get_klines(self, symbol: str) -> list[CandleStick] | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_bfx_deriv(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get(f"/candles/trade:1m:{ex_sym}/hist", {"limit": str(self.KLINE_SIZE)})
        except Exception:
            return None
        if not isinstance(data, list):
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(symbol)
        quote = ticker.quote if ticker else ""
        usd_vol = quote in QUOTES
        result: list[CandleStick] = []
        for row in data[: self.KLINE_SIZE]:
            if not isinstance(row, list) or len(row) < 6:
                continue
            ts, o, c, h, l, vol = row[0], row[1], row[2], row[3], row[4], row[5]
            result.append(
                CandleStick(
                    utc_open_time=float(ts) / 1000,
                    open_price=float(o),
                    high_price=float(h),
                    low_price=float(l),
                    close_price=float(c),
                    coin_volume=float(vol),
                    usd_volume=float(vol) * float(c) if usd_vol else None,
                )
            )
        return result

    def _exchange_symbol(self, symbol: str) -> str | None:
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        t = self._cached_perps_dict.get(symbol) or self._cached_perps_dict.get(_symbol_to_bfx_deriv(symbol))
        return t.exchange_symbol if t else None

    def _on_ws_message(self, _: Any, raw: bytes | str) -> None:
        if not self._cb:
            return
        if isinstance(raw, bytes):
            raw = raw.decode()
        try:
            msg = json.loads(raw)
        except Exception:
            return
        if isinstance(msg, dict):
            if msg.get("event") == "subscribed" and msg.get("channel") == "status":
                self._chan_to_sym[msg.get("chanId", -1)] = msg.get("key", "").replace("deriv:", "")
            return
        if not isinstance(msg, list) or len(msg) < 2:
            return
        ch_id, payload = msg[0], msg[1]
        key = self._chan_to_sym.get(ch_id, "")
        ex_sym = key.replace("deriv:", "") if key else ""
        ticker = self._cached_perps_dict.get(ex_sym)
        if not ticker:
            return
        if isinstance(payload, list) and len(payload) >= 16:
            if not self._throttler.may_pass(ticker.symbol, tag="book"):
                return
            mark = payload[15] if len(payload) > 15 else payload[3]
            if mark is not None:
                self._cb.handle(
                    book=BookTicker(
                        symbol=ticker.symbol,
                        bid_price=float(mark),
                        bid_qty=0.0,
                        ask_price=float(mark),
                        ask_qty=0.0,
                        last_update_id=payload[1] if len(payload) > 1 else None,
                        utc=float(payload[1]) / 1000 if len(payload) > 1 and payload[1] else None,
                    )
                )
            deriv_mid = payload[3] if len(payload) > 3 else None
            if deriv_mid is not None and mark is None:
                self._cb.handle(
                    book=BookTicker(
                        symbol=ticker.symbol,
                        bid_price=float(deriv_mid),
                        bid_qty=0.0,
                        ask_price=float(deriv_mid),
                        ask_qty=0.0,
                        last_update_id=payload[1] if len(payload) > 1 else None,
                        utc=float(payload[1]) / 1000 if len(payload) > 1 and payload[1] else None,
                    )
                )