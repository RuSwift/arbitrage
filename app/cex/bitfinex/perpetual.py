"""Bitfinex perpetual derivatives connector (REST + WebSocket)."""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

import websocket

from app.cex.base import BaseCEXPerpetualConnector, Callback
from app.cex.base import DEFAULT_FUNDING_HISTORY_LIMIT
from app.cex.dto import (
    BidAsk,
    BookDepth,
    BookTicker,
    CandleStick,
    CurrencyPair,
    FundingRate,
    FundingRatePoint,
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
    BOOK_LEN = 25
    """Order book length for WS book subscription."""
    WS_CONNECT_WAIT_ATTEMPTS = 10
    WS_CONNECT_WAIT_SEC = 1

    def __init__(
        self,
        is_testing: bool = False,
        throttle_timeout: float = 1.0,
        log: logging.Logger | None = None,
    ) -> None:
        super().__init__(is_testing=is_testing, throttle_timeout=throttle_timeout, log=log)
        self._cached_perps: list[PerpetualTicker] | None = None
        self._cached_perps_dict: dict[str, PerpetualTicker] = {}
        self._ws: websocket.WebSocketApp | None = None
        self._ws_thread: threading.Thread | None = None
        self._cb: Callback | None = None
        self._chan_to_sym: dict[int, str] = {}
        self._chan_candle: set[int] = set()

    @classmethod
    def exchange_id(cls) -> str:
        return "bitfinex"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = BITFINEX_API + path
        r = self._request_limited(url, params or {}, self.REQUEST_TIMEOUT_SEC)
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
        klines: bool = True,
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
            if depth:
                self._ws.send(
                    json.dumps({
                        "event": "subscribe",
                        "channel": "book",
                        "symbol": ex_sym,
                        "prec": "P0",
                        "len": str(self.BOOK_LEN),
                    })
                )
            if klines:
                self._ws.send(json.dumps({"event": "subscribe", "channel": "candles", "key": f"trade:1m:{ex_sym}"}))

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
        self._chan_candle = set()

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

    def get_klines(self, symbol: str, limit: int | None = None) -> list[CandleStick] | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_bfx_deriv(symbol)
        if not ex_sym:
            return None
        n = limit if limit is not None else self.KLINE_SIZE
        try:
            data = self._get(f"/candles/trade:1m:{ex_sym}/hist", {"limit": str(n)})
        except Exception:
            return None
        if not isinstance(data, list):
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(symbol)
        quote = ticker.quote if ticker else ""
        usd_vol = quote in QUOTES
        result: list[CandleStick] = []
        for row in data[:n]:
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

    def get_funding_rate(self, symbol: str) -> FundingRate | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_bfx_deriv(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get("/status/deriv", {"keys": ex_sym})
        except Exception:
            return None
        if not isinstance(data, list) or not data:
            return None
        row = data[0] if isinstance(data[0], list) else data
        if not isinstance(row, list) or len(row) < 13:
            return None
        current_funding = row[12] if len(row) > 12 else None
        next_funding_mts = row[8] if len(row) > 8 else None
        spot_price = row[4] if len(row) > 4 else None  # SPOT_PRICE (index price)
        if current_funding is None:
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        next_utc = float(next_funding_mts) / 1000 if next_funding_mts is not None else 0.0
        try:
            index_price = float(spot_price) if spot_price is not None else None
        except (TypeError, ValueError):
            index_price = None
        return FundingRate(
            symbol=sym,
            rate=float(current_funding),
            next_funding_utc=next_utc,
            index_price=index_price,
            utc=_utc_now_float(),
        )

    def get_funding_rate_history(
        self, symbol: str, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_bfx_deriv(symbol)
        if not ex_sym:
            return None
        n = limit if limit is not None else DEFAULT_FUNDING_HISTORY_LIMIT
        try:
            data = self._get(f"/status/deriv/{ex_sym}/hist", {"limit": str(n)})
        except Exception:
            return None
        if not isinstance(data, list):
            return None
        result: list[FundingRatePoint] = []
        for row in data[:n]:
            if not isinstance(row, list) or len(row) <= 8:
                continue
            mts = row[0]
            funding = row[8]  # hist: funding rate at index 8 (current status uses 12)
            if mts is None:
                continue
            rate_val = 0.0 if funding is None else float(funding)
            result.append(
                FundingRatePoint(
                    funding_time_utc=float(mts) / 1000,
                    rate=rate_val,
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
            if msg.get("event") == "subscribed":
                cid = msg.get("chanId", -1)
                ch = msg.get("channel", "")
                key = msg.get("key", msg.get("symbol", ""))
                if ch == "status":
                    self._chan_to_sym[cid] = key.replace("deriv:", "")
                elif ch == "book":
                    self._chan_to_sym[cid] = msg.get("symbol", "")
                elif ch == "candles" and ":" in key:
                    self._chan_to_sym[cid] = key.replace("trade:1m:", "", 1)
                    self._chan_candle.add(cid)
            return
        if not isinstance(msg, list) or len(msg) < 2:
            return
        ch_id, payload = msg[0], msg[1]
        ex_sym = self._chan_to_sym.get(ch_id, "").replace("deriv:", "")
        if ch_id in self._chan_candle:
            ticker = self._cached_perps_dict.get(ex_sym)
            if not ticker or not self._throttler.may_pass(ticker.symbol, tag="kline"):
                return
            if isinstance(payload, list) and len(payload) >= 6 and isinstance(payload[0], (int, float)):
                self._cb.handle(
                    kline=CandleStick(
                        utc_open_time=float(payload[0]) / 1000,
                        open_price=float(payload[1]),
                        high_price=float(payload[3]),
                        low_price=float(payload[4]),
                        close_price=float(payload[2]),
                        coin_volume=float(payload[5]),
                        usd_volume=None,
                    )
                )
            elif isinstance(payload, list) and len(payload) > 0 and isinstance(payload[0], list) and len(payload[0]) >= 6:
                row = payload[0]
                self._cb.handle(
                    kline=CandleStick(
                        utc_open_time=float(row[0]) / 1000,
                        open_price=float(row[1]),
                        high_price=float(row[3]),
                        low_price=float(row[4]),
                        close_price=float(row[2]),
                        coin_volume=float(row[5]),
                        usd_volume=None,
                    )
                )
            return
        ticker = self._cached_perps_dict.get(ex_sym)
        if not ticker:
            return
        if not isinstance(payload, list):
            return
        # Status channel: single array with 16+ elements (deriv status)
        if len(payload) >= 16 and not isinstance(payload[0], list):
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
            else:
                deriv_mid = payload[3] if len(payload) > 3 else None
                if deriv_mid is not None:
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
            return
        # Book channel: snapshot (array of [price, count, amount]) or update (single [price, count, amount])
        if not self._throttler.may_pass(ticker.symbol, tag="depth"):
            return
        if len(payload) > 0 and isinstance(payload[0], list):
            # Snapshot
            bids = [BidAsk(price=float(x[0]), quantity=float(x[2])) for x in payload if len(x) >= 3 and x[2] > 0]
            asks = [BidAsk(price=float(x[0]), quantity=float(-x[2])) for x in payload if len(x) >= 3 and x[2] < 0]
        elif len(payload) >= 3 and isinstance(payload[0], (int, float)):
            # Single-level update
            price, count, amount = payload[0], payload[1], payload[2]
            if amount > 0:
                bids = [BidAsk(price=float(price), quantity=float(amount))]
                asks = []
            else:
                bids = []
                asks = [BidAsk(price=float(price), quantity=float(-amount))]
        else:
            return
        self._cb.handle(
            depth=BookDepth(
                symbol=ticker.symbol,
                exchange_symbol=ex_sym,
                bids=bids,
                asks=asks,
                last_update_id=ch_id,
                utc=_utc_now_float(),
            )
        )