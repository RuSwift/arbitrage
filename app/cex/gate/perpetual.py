"""Gate.io USDT perpetual connector (REST + WebSocket)."""

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

GATE_FUTURES_API = "https://fx-api.gateio.ws/api/v4"
GATE_FUTURES_WS = "wss://fx-ws.gateio.ws/v4/ws/usdt"
GATE_FUTURES_WS_TESTNET = "wss://fx-ws-testnet.gateio.ws/v4/ws/usdt"
SETTLE = "usdt"
QUOTES = ("USDT", "USDC")


def _utc_now_float() -> float:
    return time.time()


def _gate_to_symbol(contract: str) -> str:
    """BTC_USDT -> BTC/USDT."""
    return contract.replace("_", "/").upper()


def _symbol_to_gate(symbol: str) -> str:
    """BTC/USDT -> BTC_USDT."""
    return symbol.replace("/", "_").upper()


def _build_perp_dict(tickers: list[PerpetualTicker]) -> dict[str, PerpetualTicker]:
    out: dict[str, PerpetualTicker] = {}
    for t in tickers:
        out[t.symbol] = t
        out[t.exchange_symbol] = t
        out[t.symbol.replace("/", "")] = t
        out[t.symbol.replace("/", "_")] = t
    return out


class GatePerpetualConnector(BaseCEXPerpetualConnector):
    REQUEST_TIMEOUT_SEC = 15
    DEPTH_API_MAX = 100
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
        self._depth_cache: dict[str, dict] = {}

    @classmethod
    def exchange_id(cls) -> str:
        return "gate"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = GATE_FUTURES_API + path
        r = self._request_limited(url, params or {}, self.REQUEST_TIMEOUT_SEC)
        r.raise_for_status()
        return r.json()

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
        ws_url = GATE_FUTURES_WS_TESTNET if self._is_testing else GATE_FUTURES_WS
        self._ws = websocket.WebSocketApp(
            ws_url,
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
            raise RuntimeError("Gate futures WebSocket connection failed.")
        for contract in syms:
            self._ws_send("futures.book_ticker", "subscribe", [contract])
            if depth:
                self._ws_send("futures.order_book_update", "subscribe", [contract, "100ms", "100"])
            if klines:
                self._ws_send("futures.candlesticks", "subscribe", [contract, "1m"])

    def _ws_send(self, channel: str, event: str, payload: list[str]) -> None:
        if not self._ws or not self._ws.sock or not self._ws.sock.connected:
            return
        msg = {
            "time": int(time.time()),
            "channel": channel,
            "event": event,
            "payload": payload,
        }
        self._ws.send(json.dumps(msg))

    def stop(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._ws_thread = None
        self._cb = None
        self._depth_cache.clear()

    def get_all_perpetuals(self) -> list[PerpetualTicker]:
        if self._cached_perps is not None:
            return self._cached_perps
        data = self._get(f"/futures/{SETTLE}/contracts")
        if not isinstance(data, list):
            raise RuntimeError("Failed to get futures contracts")
        perps: list[PerpetualTicker] = []
        for item in data:
            name = item.get("name", "")
            if "_" not in name:
                continue
            base, quote = name.split("_", 1)
            if item.get("in_delisting", False):
                continue
            perps.append(
                PerpetualTicker(
                    symbol=_gate_to_symbol(name),
                    base=base,
                    quote=quote,
                    exchange_symbol=name,
                    settlement=quote,
                )
            )
        self._cached_perps = perps
        self._cached_perps_dict = _build_perp_dict(perps)
        return self._cached_perps

    def get_price(self, symbol: str) -> CurrencyPair | None:
        contract = self._exchange_symbol(symbol)
        if not contract:
            return None
        try:
            data = self._get("/futures/usdt/tickers", {"contract": contract})
        except Exception:
            return None
        if isinstance(data, list) and data:
            row = data[0]
        elif isinstance(data, dict):
            row = data
        else:
            return None
        if row.get("contract", "").upper() != contract.upper():
            return None
        last = row.get("last")
        if last is None:
            return None
        ticker = self._cached_perps_dict.get(contract) or self._cached_perps_dict.get(symbol)
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
        # One request for all tickers to avoid rate limit (Gate ~100 req/min)
        data = self._get("/futures/usdt/tickers")
        if symbols is not None:
            want = {s.upper() for s in symbols}
            raw = data if isinstance(data, list) else [data]
            data = [row for row in raw if _gate_to_symbol(row.get("contract", "")) in want]
        if not isinstance(data, list):
            data = [data] if isinstance(data, dict) else []
        result: list[CurrencyPair] = []
        for row in data:
            contract = row.get("contract", "")
            ticker = self._cached_perps_dict.get(contract) or self._cached_perps_dict.get(
                _gate_to_symbol(contract)
            )
            if not ticker:
                continue
            last = row.get("last")
            if last is None:
                continue
            result.append(
                CurrencyPair(
                    base=ticker.base,
                    quote=ticker.quote,
                    ratio=float(last),
                    utc=_utc_now_float(),
                )
            )
        return result

    def get_depth(self, symbol: str, limit: int = 100) -> BookDepth | None:
        contract = self._exchange_symbol(symbol)
        if not contract:
            return None
        try:
            data = self._get(
                f"/futures/{SETTLE}/order_book",
                {"contract": contract, "limit": str(min(limit, self.DEPTH_API_MAX))},
            )
        except Exception:
            return None
        if not data:
            return None
        ticker = self._cached_perps_dict.get(contract) or self._cached_perps_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        raw_bids = data.get("bids", [])
        raw_asks = data.get("asks", [])
        if raw_bids and isinstance(raw_bids[0], dict):
            bids = [BidAsk(price=float(x["p"]), quantity=float(x["s"])) for x in raw_bids]
            asks = [BidAsk(price=float(x["p"]), quantity=float(x["s"])) for x in raw_asks]
        else:
            bids = [BidAsk(price=float(p), quantity=float(q)) for p, q in raw_bids]
            asks = [BidAsk(price=float(p), quantity=float(q)) for p, q in raw_asks]
        return BookDepth(
            symbol=sym,
            exchange_symbol=contract,
            bids=bids,
            asks=asks,
            last_update_id=data.get("id"),
            utc=_utc_now_float(),
        )

    def get_klines(self, symbol: str, limit: int | None = None) -> list[CandleStick] | None:
        contract = self._exchange_symbol(symbol)
        if not contract:
            return None
        n = limit if limit is not None else self.KLINE_SIZE
        try:
            data = self._get(
                f"/futures/{SETTLE}/candlesticks",
                {"contract": contract, "interval": "1m", "limit": str(n)},
            )
        except Exception:
            return None
        if not isinstance(data, list):
            return None
        ticker = self._cached_perps_dict.get(contract) or self._cached_perps_dict.get(symbol)
        quote = ticker.quote if ticker else ""
        usd_vol = quote in QUOTES
        result: list[CandleStick] = []
        for row in data:
            if isinstance(row, list):
                t, o, h, l, c, v = row[0], row[1], row[2], row[3], row[4], row[5]
            else:
                t = row.get("t", row.get("timestamp", 0))
                o = row.get("o", row.get("open", 0))
                h = row.get("h", row.get("high", 0))
                l = row.get("l", row.get("low", 0))
                c = row.get("c", row.get("close", 0))
                v = row.get("v", row.get("volume", 0))
            ts = float(t)
            utc_open = ts / 1000.0 if ts > 1e12 else ts
            result.append(
                CandleStick(
                    utc_open_time=utc_open,
                    open_price=float(o),
                    high_price=float(h),
                    low_price=float(l),
                    close_price=float(c),
                    coin_volume=float(v),
                    usd_volume=float(v) * float(c) if usd_vol else None,
                )
            )
        return result

    def _exchange_symbol(self, symbol: str) -> str | None:
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        t = (
            self._cached_perps_dict.get(symbol)
            or self._cached_perps_dict.get(symbol.replace("/", "_"))
            or self._cached_perps_dict.get(symbol.replace("/", ""))
        )
        return t.exchange_symbol if t else None

    def get_funding_rate(self, symbol: str) -> FundingRate | None:
        contract = self._exchange_symbol(symbol)
        if not contract:
            return None
        try:
            data = self._get(f"/futures/{SETTLE}/funding_rate", {"contract": contract, "limit": "1"})
        except Exception:
            return None
        if not isinstance(data, list):
            return None
        ticker = self._cached_perps_dict.get(contract) or self._cached_perps_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        if not data:
            return FundingRate(
                symbol=sym,
                rate=0.0,
                next_funding_utc=0.0,
                utc=_utc_now_float(),
            )
        row = data[0] if isinstance(data[0], dict) else {}
        rate_val = row.get("r") or row.get("rate") or row.get("funding_rate")
        funding_time = row.get("t") or row.get("funding_time") or row.get("time")
        if rate_val is None:
            return FundingRate(
                symbol=sym,
                rate=0.0,
                next_funding_utc=0.0,
                utc=_utc_now_float(),
            )
        if funding_time is not None and isinstance(funding_time, (int, float)):
            next_utc = float(funding_time) + 8 * 3600
        else:
            next_utc = 0.0
        return FundingRate(
            symbol=sym,
            rate=float(rate_val),
            next_funding_utc=next_utc,
            utc=_utc_now_float(),
        )

    def get_funding_rate_history(
        self, symbol: str, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        contract = self._exchange_symbol(symbol)
        if not contract:
            return None
        n = limit if limit is not None else DEFAULT_FUNDING_HISTORY_LIMIT
        try:
            data = self._get(f"/futures/{SETTLE}/funding_rate", {"contract": contract, "limit": str(n)})
        except Exception:
            return None
        if not isinstance(data, list):
            return None
        return [
            FundingRatePoint(
                funding_time_utc=float(row.get("t", row.get("funding_time", row.get("time", 0)))),
                rate=float(row.get("r", row.get("rate", row.get("funding_rate", 0)))),
            )
            for row in data
        ]

    def _on_ws_message(self, _: Any, raw: bytes | str) -> None:
        if not self._cb:
            return
        if isinstance(raw, bytes):
            raw = raw.decode()
        try:
            msg = json.loads(raw)
        except Exception:
            return
        if msg.get("event") == "pong" or "error" in msg:
            return
        channel = msg.get("channel", "")
        event = msg.get("event", "")
        if event != "update":
            return
        result = msg.get("result")
        if not result:
            return
        # API may send result as list of one item (e.g. candlesticks)
        if isinstance(result, list) and len(result) > 0:
            result = result[0]
        if not isinstance(result, dict):
            return
        if channel == "futures.book_ticker":
            s = result.get("s", "")
            sym = _gate_to_symbol(s)
            ticker = self._cached_perps_dict.get(s) or self._cached_perps_dict.get(sym)
            if not ticker or not self._throttler.may_pass(ticker.symbol, tag="book"):
                return
            b, B = result.get("b", "0"), result.get("B", 0)
            a, A = result.get("a", "0"), result.get("A", 0)
            self._cb.handle(
                book=BookTicker(
                    symbol=ticker.symbol,
                    bid_price=float(b) if b else 0.0,
                    bid_qty=float(B) if B else 0.0,
                    ask_price=float(a) if a else 0.0,
                    ask_qty=float(A) if A else 0.0,
                    last_update_id=result.get("u"),
                    utc=float(result.get("t", 0)) / 1000,
                )
            )
        elif channel == "futures.order_book_update":
            s = result.get("s", "")
            sym = _gate_to_symbol(s)
            ticker = self._cached_perps_dict.get(s) or self._cached_perps_dict.get(sym)
            if not ticker or not self._throttler.may_pass(ticker.symbol, tag="depth"):
                return
            bids = result.get("b", [])
            asks = result.get("a", [])

            def _to_bidask_from_rows(rows: list) -> list:
                out = []
                for row in rows:
                    if isinstance(row, dict):
                        try:
                            p, s = row.get("p", 0), row.get("s", 0)
                            out.append(BidAsk(price=float(p), quantity=float(s)))
                        except (TypeError, ValueError):
                            pass  # skip header row {"p": "p", "s": "s"} or invalid
                    elif isinstance(row, (list, tuple)) and len(row) >= 2:
                        try:
                            out.append(BidAsk(price=float(row[0]), quantity=float(row[1])))
                        except (TypeError, ValueError):
                            pass  # skip header row ["p", "s"] or invalid
                return out

            bid_list = _to_bidask_from_rows(bids)
            ask_list = _to_bidask_from_rows(asks)
            u = result.get("u")
            t = float(result.get("t", 0)) / 1000
            cache = self._depth_cache.setdefault(ticker.symbol, {"bids": None, "asks": None, "u": None, "t": 0.0})
            if bid_list:
                cache["bids"] = bid_list
                cache["u"] = u
                cache["t"] = t
            if ask_list:
                cache["asks"] = ask_list
                cache["u"] = u
                cache["t"] = t
            final_bids = cache["bids"] or []
            final_asks = cache["asks"] or []
            if final_bids or final_asks:
                self._cb.handle(
                    depth=BookDepth(
                        symbol=ticker.symbol,
                        exchange_symbol=ticker.exchange_symbol or s,
                        bids=final_bids,
                        asks=final_asks,
                        last_update_id=cache["u"],
                        utc=cache["t"],
                    )
                )
                if final_bids and final_asks:
                    del self._depth_cache[ticker.symbol]
        elif channel == "futures.candlesticks":
            s = result.get("s", result.get("n", ""))
            if not s:
                return
            sym = _gate_to_symbol(s)
            ticker = self._cached_perps_dict.get(s) or self._cached_perps_dict.get(sym)
            if not ticker or not self._throttler.may_pass(ticker.symbol, tag="kline"):
                return
            t_ms = result.get("t", 0)
            self._cb.handle(
                kline=CandleStick(
                    utc_open_time=float(t_ms) / 1000 if t_ms else 0,
                    open_price=float(result.get("o", 0)),
                    high_price=float(result.get("h", 0)),
                    low_price=float(result.get("l", 0)),
                    close_price=float(result.get("c", 0)),
                    coin_volume=float(result.get("v", 0)),
                    usd_volume=None,
                )
            )
