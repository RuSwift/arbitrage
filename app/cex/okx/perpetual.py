"""OKX perpetual swap connector (REST + WebSocket)."""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

import requests
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

OKX_REST = "https://www.okx.com"
OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"
OKX_WS_TESTNET = "wss://wss.okx.com:8443/ws/v5/public"
QUOTES = ("USDT", "USDC")


def _utc_now_float() -> float:
    return time.time()


def _okx_swap_to_symbol(inst_id: str) -> str:
    """BTC-USDT-SWAP -> BTC/USDT."""
    if inst_id.endswith("-SWAP"):
        inst_id = inst_id[:-5]
    return inst_id.replace("-", "/", 1)


def _symbol_to_okx_swap(symbol: str) -> str:
    """BTC/USDT -> BTC-USDT-SWAP."""
    return symbol.replace("/", "-") + "-SWAP"


def _build_perp_dict(tickers: list[PerpetualTicker]) -> dict[str, PerpetualTicker]:
    out: dict[str, PerpetualTicker] = {}
    for t in tickers:
        out[t.symbol] = t
        out[t.exchange_symbol] = t
        out[t.symbol.replace("/", "")] = t
        out[t.symbol.replace("/", "-") + "-SWAP"] = t
    return out


class OkxPerpetualConnector(BaseCEXPerpetualConnector):
    REQUEST_TIMEOUT_SEC = 15
    DEPTH_API_MAX = 400
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
        self._cached_perps: list[PerpetualTicker] | None = None
        self._cached_perps_dict: dict[str, PerpetualTicker] = {}
        self._ws: websocket.WebSocketApp | None = None
        self._ws_thread: threading.Thread | None = None
        self._cb: Callback | None = None

    @classmethod
    def exchange_id(cls) -> str:
        return "okx"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = OKX_REST + path
        r = requests.get(url, params=params or {}, timeout=self.REQUEST_TIMEOUT_SEC)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "0":
            raise RuntimeError(data.get("msg", "OKX API error"))
        return data.get("data", [])

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
        ws_url = OKX_WS_TESTNET if self._is_testing else OKX_WS
        self._ws = websocket.WebSocketApp(ws_url, on_message=self._on_ws_message)
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
            raise RuntimeError("OKX swap WebSocket connection failed.")
        args = []
        for inst_id in syms:
            args.append({"channel": "bbo-tbt", "instId": inst_id})
            if depth:
                args.append({"channel": "books5", "instId": inst_id})
        self._ws.send(json.dumps({"op": "subscribe", "args": args}))

    def stop(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._ws_thread = None
        self._cb = None

    def get_all_perpetuals(self) -> list[PerpetualTicker]:
        if self._cached_perps is not None:
            return self._cached_perps
        data = self._get("/api/v5/public/instruments", {"instType": "SWAP"})
        if not isinstance(data, list):
            raise RuntimeError("Failed to get OKX swap instruments")
        perps: list[PerpetualTicker] = []
        for item in data:
            if item.get("state") != "live":
                continue
            inst_id = item.get("instId", "")
            if not inst_id.endswith("-SWAP"):
                continue
            uly = item.get("uly", inst_id.replace("-SWAP", ""))
            parts = uly.split("-") if "-" in uly else [uly, "USDT"]
            base, quote = parts[0], parts[1] if len(parts) > 1 else "USDT"
            perps.append(
                PerpetualTicker(
                    symbol=_okx_swap_to_symbol(inst_id),
                    base=base,
                    quote=quote,
                    exchange_symbol=inst_id,
                    settlement=quote,
                )
            )
        self._cached_perps = perps
        self._cached_perps_dict = _build_perp_dict(perps)
        return self._cached_perps

    def get_price(self, symbol: str) -> CurrencyPair | None:
        inst_id = self._exchange_symbol(symbol) or _symbol_to_okx_swap(symbol)
        if not inst_id:
            return None
        try:
            tickers_data = self._get("/api/v5/market/ticker", {"instId": inst_id})
        except RuntimeError:
            return None
        if not tickers_data or not isinstance(tickers_data, list):
            return None
        row = tickers_data[0]
        last = row.get("last")
        if last is None:
            return None
        ticker = self._cached_perps_dict.get(inst_id) or self._cached_perps_dict.get(symbol)
        if not ticker:
            return None
        return CurrencyPair(
            base=ticker.base,
            quote=ticker.quote,
            ratio=float(last),
            utc=_utc_now_float(),
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        data = self._get("/api/v5/market/tickers", {"instType": "SWAP"})
        if not isinstance(data, list):
            return []
        result: list[CurrencyPair] = []
        want = None if symbols is None else {self._exchange_symbol(s) or _symbol_to_okx_swap(s) for s in symbols}
        for row in data:
            inst_id = row.get("instId", "")
            if want is not None and inst_id not in want:
                continue
            ticker = self._cached_perps_dict.get(inst_id)
            if not ticker:
                continue
            last = row.get("last")
            if last is None or last == "":
                continue
            try:
                ratio = float(last)
            except (TypeError, ValueError):
                continue
            result.append(
                CurrencyPair(
                    base=ticker.base,
                    quote=ticker.quote,
                    ratio=ratio,
                    utc=_utc_now_float(),
                )
            )
        return result

    def get_depth(self, symbol: str, limit: int = 100) -> BookDepth | None:
        inst_id = self._exchange_symbol(symbol) or _symbol_to_okx_swap(symbol)
        if not inst_id:
            return None
        data = self._get("/api/v5/market/books", {"instId": inst_id, "sz": str(min(limit, self.DEPTH_API_MAX))})
        if not data or not isinstance(data, list):
            return None
        book = data[0] if isinstance(data[0], dict) else {}
        bids = [BidAsk(price=float(b[0]), quantity=float(b[1])) for b in book.get("bids", [])]
        asks = [BidAsk(price=float(a[0]), quantity=float(a[1])) for a in book.get("asks", [])]
        ticker = self._cached_perps_dict.get(inst_id) or self._cached_perps_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        return BookDepth(
            symbol=sym,
            exchange_symbol=inst_id,
            bids=bids,
            asks=asks,
            last_update_id=book.get("ts"),
            utc=_utc_now_float(),
        )

    def get_klines(self, symbol: str, limit: int | None = None) -> list[CandleStick] | None:
        inst_id = self._exchange_symbol(symbol) or _symbol_to_okx_swap(symbol)
        if not inst_id:
            return None
        n = limit if limit is not None else self.KLINE_SIZE
        data = self._get(
            "/api/v5/market/candles",
            {"instId": inst_id, "bar": "1m", "limit": str(n)},
        )
        if not isinstance(data, list):
            return None
        ticker = self._cached_perps_dict.get(inst_id) or self._cached_perps_dict.get(symbol)
        quote = ticker.quote if ticker else ""
        usd_vol = quote in QUOTES
        result: list[CandleStick] = []
        for row in data:
            if isinstance(row, list):
                ts, o, h, l, c, vol = row[0], row[1], row[2], row[3], row[4], row[5]
            else:
                ts, o, h, l, c, vol = row["ts"], row["o"], row["h"], row["l"], row["c"], row["vol"]
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
        inst_id = self._exchange_symbol(symbol) or _symbol_to_okx_swap(symbol)
        if not inst_id:
            return None
        try:
            data = self._get("/api/v5/public/funding-rate", {"instId": inst_id})
        except RuntimeError:
            return None
        if not data or not isinstance(data, list):
            return None
        row = data[0] if isinstance(data[0], dict) else {}
        funding_rate = row.get("fundingRate")
        next_ts = row.get("nextFundingTime")
        next_rate_raw = row.get("nextFundingRate")
        if funding_rate is None:
            return None
        ticker = self._cached_perps_dict.get(inst_id) or self._cached_perps_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        next_utc = float(next_ts) / 1000 if next_ts is not None else 0.0
        try:
            next_rate = float(next_rate_raw) if next_rate_raw is not None else None
        except (TypeError, ValueError):
            next_rate = None
        return FundingRate(
            symbol=sym,
            rate=float(funding_rate),
            next_funding_utc=next_utc,
            next_rate=next_rate,
            utc=_utc_now_float(),
        )

    def get_funding_rate_history(
        self, symbol: str, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        inst_id = self._exchange_symbol(symbol) or _symbol_to_okx_swap(symbol)
        if not inst_id:
            return None
        n = limit if limit is not None else DEFAULT_FUNDING_HISTORY_LIMIT
        try:
            data = self._get(
                "/api/v5/public/funding-rate-history",
                {"instId": inst_id, "limit": str(n)},
            )
        except RuntimeError:
            return None
        if not isinstance(data, list):
            return None
        return [
            FundingRatePoint(
                funding_time_utc=float(row["fundingTime"]) / 1000,
                rate=float(row["fundingRate"]),
            )
            for row in data
        ]

    def _exchange_symbol(self, symbol: str) -> str | None:
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        t = (
            self._cached_perps_dict.get(symbol)
            or self._cached_perps_dict.get(symbol.replace("/", "-") + "-SWAP")
            or self._cached_perps_dict.get(symbol.replace("/", ""))
        )
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
        if "event" in msg and msg.get("event") != "update":
            return
        if "data" not in msg:
            return
        data_list = msg["data"] if isinstance(msg["data"], list) else [msg["data"]]
        channel = msg.get("arg", {}).get("channel", "") if "arg" in msg else ""
        for data in data_list:
            if not isinstance(data, dict):
                continue
            inst_id = data.get("instId") or (msg.get("arg") or {}).get("instId", "")
            ticker = self._cached_perps_dict.get(inst_id)
            if not ticker:
                continue
            sym = ticker.symbol
            if "bbo" in channel or channel == "bbo-tbt":
                if not self._throttler.may_pass(sym, tag="book"):
                    continue
                bid_p = data.get("bidPx") or data.get("bidpx")
                ask_p = data.get("askPx") or data.get("askpx")
                bid_s = data.get("bidSz") or data.get("bidsz", "0")
                ask_s = data.get("askSz") or data.get("asksz", "0")
                self._cb.handle(
                    book=BookTicker(
                        symbol=sym,
                        bid_price=float(bid_p) if bid_p else 0.0,
                        bid_qty=float(bid_s),
                        ask_price=float(ask_p) if ask_p else 0.0,
                        ask_qty=float(ask_s),
                        last_update_id=data.get("ts"),
                        utc=float(data.get("ts", 0)) / 1000,
                    )
                )
            elif "books" in channel:
                if not self._throttler.may_pass(sym, tag="depth"):
                    continue
                bids = [BidAsk(price=float(b[0]), quantity=float(b[1])) for b in data.get("bids", [])]
                asks = [BidAsk(price=float(a[0]), quantity=float(a[1])) for a in data.get("asks", [])]
                self._cb.handle(
                    depth=BookDepth(
                        symbol=sym,
                        exchange_symbol=inst_id,
                        bids=bids,
                        asks=asks,
                        last_update_id=data.get("ts"),
                        utc=float(data.get("ts", 0)) / 1000,
                    )
                )
