"""Gate.io spot connector (REST + WebSocket)."""

from __future__ import annotations

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

GATE_SPOT_API = "https://api.gateio.ws/api/v4"
GATE_SPOT_WS = "wss://api.gateio.ws/ws/v4/"
GATE_SPOT_WS_TESTNET = "wss://ws-testnet.gate.io/v4/ws/spot"
QUOTES = ("USDT", "BTC", "ETH", "USDC")


def _utc_now_float() -> float:
    return time.time()


def _gate_to_symbol(currency_pair: str) -> str:
    """BTC_USDT -> BTC/USDT."""
    return currency_pair.replace("_", "/").upper()


def _symbol_to_gate(symbol: str) -> str:
    """BTC/USDT -> BTC_USDT."""
    return symbol.replace("/", "_").upper()


def _build_tickers_dict(tickers: list[Ticker]) -> dict[str, Ticker]:
    out: dict[str, Ticker] = {}
    for t in tickers:
        out[t.symbol] = t
        if t.exchange_symbol:
            out[t.exchange_symbol] = t
            out[t.exchange_symbol.upper()] = t
        out[t.symbol.replace("/", "")] = t
        out[t.symbol.replace("/", "_")] = t
    return out


class GateSpotConnector(BaseCEXSpotConnector):
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
        self._cached_tickers: list[Ticker] | None = None
        self._cached_tickers_dict: dict[str, Ticker] = {}
        self._ws: websocket.WebSocketApp | None = None
        self._ws_thread: threading.Thread | None = None
        self._cb: Callback | None = None

    @classmethod
    def exchange_id(cls) -> str:
        return "gate"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = GATE_SPOT_API + path
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
        if not syms:
            raise RuntimeError("No symbols to subscribe")
        self._cb = cb
        ws_url = GATE_SPOT_WS_TESTNET if self._is_testing else GATE_SPOT_WS
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
            raise RuntimeError("Gate spot WebSocket connection failed.")
        for cp in syms:
            self._ws_send("spot.book_ticker", "subscribe", [cp])
            if depth:
                self._ws_send("spot.order_book_update", "subscribe", [cp, "100ms"])

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

    def get_all_tickers(self) -> list[Ticker]:
        if self._cached_tickers is not None:
            return self._cached_tickers
        data = self._get("/spot/currency_pairs")
        if not isinstance(data, list):
            raise RuntimeError(data.get("message", "Failed to get currency pairs"))
        tickers: list[Ticker] = []
        for item in data:
            if item.get("trade_status") == "untradable":
                continue
            base = item.get("base", "")
            quote = item.get("quote", "")
            cp = item.get("id", f"{base}_{quote}")
            symbol = _gate_to_symbol(cp)
            tickers.append(
                Ticker(
                    symbol=symbol,
                    base=base,
                    quote=quote,
                    is_spot_enabled=True,
                    is_margin_enabled=False,
                    exchange_symbol=cp,
                )
            )
        self._cached_tickers = tickers
        self._cached_tickers_dict = _build_tickers_dict(tickers)
        return self._cached_tickers

    def get_price(self, pair_code: str) -> CurrencyPair | None:
        cp = _symbol_to_gate(pair_code)
        try:
            data = self._get("/spot/tickers", {"currency_pair": cp})
        except Exception:
            return None
        if isinstance(data, list) and data:
            row = data[0]
        elif isinstance(data, dict) and "currency_pair" in data:
            row = data
        else:
            return None
        if row.get("currency_pair", "").upper() != cp.upper():
            return None
        last = row.get("last")
        if last is None:
            return None
        if "/" in pair_code:
            base, quote = pair_code.split("/")
        else:
            base, quote = _gate_to_symbol(cp).split("/") if "_" in cp else (row.get("base", ""), row.get("quote", ""))
        return CurrencyPair(
            base=base,
            quote=quote,
            ratio=float(last),
            utc=_utc_now_float(),
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        # One request for all tickers to avoid rate limit (Gate ~100 req/min)
        pairs = self._get("/spot/tickers")
        if symbols is not None:
            want = {s.upper() for s in symbols}
            pairs = [p for p in (pairs if isinstance(pairs, list) else [pairs]) if _gate_to_symbol(p.get("currency_pair", "")) in want]
        if not isinstance(pairs, list):
            pairs = [pairs] if isinstance(pairs, dict) else []
        result: list[CurrencyPair] = []
        for p in pairs:
            cp = p.get("currency_pair", "")
            ticker = self._cached_tickers_dict.get(cp) or self._cached_tickers_dict.get(
                _gate_to_symbol(cp)
            )
            if not ticker:
                continue
            last = p.get("last")
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
        cp = self._exchange_symbol(symbol)
        if not cp:
            return None
        try:
            data = self._get(
                "/spot/order_book",
                {"currency_pair": cp, "limit": str(min(limit, self.DEPTH_API_MAX))},
            )
        except Exception:
            return None
        if not data:
            return None
        ticker = self._cached_tickers_dict.get(cp) or self._cached_tickers_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        bids = [BidAsk(price=float(p), quantity=float(q)) for p, q in data.get("bids", [])]
        asks = [BidAsk(price=float(p), quantity=float(q)) for p, q in data.get("asks", [])]
        return BookDepth(
            symbol=sym,
            exchange_symbol=cp,
            bids=bids,
            asks=asks,
            last_update_id=data.get("id"),
            utc=_utc_now_float(),
        )

    def get_klines(self, symbol: str, limit: int | None = None) -> list[CandleStick] | None:
        cp = self._exchange_symbol(symbol)
        if not cp:
            return None
        n = limit if limit is not None else self.KLINE_SIZE
        try:
            data = self._get(
                "/spot/candlesticks",
                {
                    "currency_pair": cp,
                    "interval": "1m",
                    "limit": str(n),
                },
            )
        except Exception:
            return None
        if not isinstance(data, list):
            return None
        ticker = self._cached_tickers_dict.get(cp) or self._cached_tickers_dict.get(symbol)
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
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        t = (
            self._cached_tickers_dict.get(symbol)
            or self._cached_tickers_dict.get(symbol.replace("/", "_"))
            or self._cached_tickers_dict.get(symbol.replace("/", ""))
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
        if not isinstance(msg, dict):
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
        # Gate may send result as a single dict or as a list of dicts (or mixed)
        if isinstance(result, dict):
            payloads = [result]
        elif isinstance(result, list):
            payloads = [r for r in result if isinstance(r, dict)]
        else:
            return
        for result in payloads:
            if not isinstance(result, dict):
                continue
            if channel == "spot.book_ticker":
                s = result.get("s", "")
                if not s:
                    continue
                s_key = s.upper() if isinstance(s, str) else s
                sym = _gate_to_symbol(s)
                ticker = (
                    self._cached_tickers_dict.get(s)
                    or self._cached_tickers_dict.get(s_key)
                    or self._cached_tickers_dict.get(sym)
                )
                if not ticker or not self._throttler.may_pass(ticker.symbol, tag="book"):
                    continue
                b, B = result.get("b", "0"), result.get("B", "0")
                a, A = result.get("a", "0"), result.get("A", "0")
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
            elif channel == "spot.order_book_update":
                s = result.get("s", "")
                if not s:
                    continue
                s_key = s.upper() if isinstance(s, str) else s
                sym = _gate_to_symbol(s)
                ticker = (
                    self._cached_tickers_dict.get(s)
                    or self._cached_tickers_dict.get(s_key)
                    or self._cached_tickers_dict.get(sym)
                )
                if not ticker or not self._throttler.may_pass(ticker.symbol, tag="depth"):
                    continue
                bids = result.get("b", [])
                asks = result.get("a", [])
                if bids and isinstance(bids[0], (list, tuple)):
                    bid_list = [BidAsk(price=float(p), quantity=float(q)) for p, q in bids]
                    ask_list = [BidAsk(price=float(p), quantity=float(q)) for p, q in asks]
                else:
                    bid_list = []
                    ask_list = []
                    for x in bids:
                        if isinstance(x, (list, tuple)) and len(x) >= 2:
                            bid_list.append(BidAsk(price=float(x[0]), quantity=float(x[1])))
                        elif isinstance(x, dict):
                            bid_list.append(BidAsk(price=float(x.get("p", 0)), quantity=float(x.get("s", 0))))
                    for x in asks:
                        if isinstance(x, (list, tuple)) and len(x) >= 2:
                            ask_list.append(BidAsk(price=float(x[0]), quantity=float(x[1])))
                        elif isinstance(x, dict):
                            ask_list.append(BidAsk(price=float(x.get("p", 0)), quantity=float(x.get("s", 0))))
                self._cb.handle(
                    depth=BookDepth(
                        symbol=ticker.symbol,
                        exchange_symbol=ticker.exchange_symbol or s,
                        bids=bid_list,
                        asks=ask_list,
                        last_update_id=result.get("u"),
                        utc=float(result.get("t", 0)) / 1000,
                    )
                )
