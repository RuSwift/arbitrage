"""Bitfinex spot connector (REST + WebSocket)."""

from __future__ import annotations

import json
import threading
import time
from typing import Any

import requests
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

BITFINEX_API = "https://api-pub.bitfinex.com/v2"
BITFINEX_WS = "wss://api-pub.bitfinex.com/ws/2"
QUOTES = ("USD", "UST", "USDT", "EUR", "BTC", "ETH")


def _utc_now_float() -> float:
    return time.time()


def _bfx_to_symbol(s: str) -> str:
    """tBTCUSD -> BTC/USD, tETHUST -> ETH/UST."""
    if s.startswith("t"):
        s = s[1:]
    for q in QUOTES:
        if s.endswith(q):
            return f"{s[:-len(q)]}/{q}"
    return s


def _symbol_to_bfx(symbol: str) -> str:
    """BTC/USDT -> tBTCUST (Bitfinex uses UST for USDT)."""
    base, quote = symbol.split("/") if "/" in symbol else (symbol, "USD")
    if quote == "USDT":
        quote = "UST"
    return f"t{base}{quote}"


def _build_tickers_dict(tickers: list[Ticker]) -> dict[str, Ticker]:
    out: dict[str, Ticker] = {}
    for t in tickers:
        out[t.symbol] = t
        if t.exchange_symbol:
            out[t.exchange_symbol] = t
        out[t.symbol.replace("/", "")] = t
    return out


class BitfinexSpotConnector(BaseCEXSpotConnector):
    REQUEST_TIMEOUT_SEC = 15
    DEPTH_API_MAX = 100
    KLINE_SIZE = 60
    BOOK_LEN = 25
    """Orderbook length for WS book subscription."""
    WS_CONNECT_WAIT_ATTEMPTS = 10
    WS_CONNECT_WAIT_SEC = 1

    def __init__(self, is_testing: bool = False, throttle_timeout: float = 1.0) -> None:
        super().__init__(is_testing=is_testing, throttle_timeout=throttle_timeout)
        self._cached_tickers: list[Ticker] | None = None
        self._cached_tickers_dict: dict[str, Ticker] = {}
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
            raise RuntimeError("Bitfinex spot WebSocket connection failed.")
        for ex_sym in syms:
            self._ws.send(json.dumps({"event": "subscribe", "channel": "ticker", "symbol": ex_sym}))
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

    def get_all_tickers(self) -> list[Ticker]:
        if self._cached_tickers is not None:
            return self._cached_tickers
        try:
            data = self._get("/conf/pub:list:pair:exchange")
        except Exception:
            data = []
        if not isinstance(data, list) or not data or (len(data) > 0 and data[0] == "error"):
            pairs = ["tBTCUSD", "tETHUSD", "tBTCUST", "tETHUST"]
        else:
            raw = data[0] if data and isinstance(data[0], list) else data
            pairs = raw if isinstance(raw, list) else [raw]
        tickers: list[Ticker] = []
        for p in pairs:
            sym = (p if isinstance(p, str) else str(p)).strip()
            if not sym or len(sym) < 4:
                continue
            if not sym.startswith("t"):
                sym = "t" + sym
            symbol = _bfx_to_symbol(sym)
            base = symbol.split("/")[0]
            quote = symbol.split("/")[1] if "/" in symbol else "USD"
            if quote == "UST":
                quote = "USDT"
                symbol = f"{base}/{quote}"
            tickers.append(
                Ticker(
                    symbol=symbol,
                    base=base,
                    quote=quote,
                    is_spot_enabled=True,
                    is_margin_enabled=False,
                    exchange_symbol=sym,
                )
            )
        self._cached_tickers = tickers
        self._cached_tickers_dict = _build_tickers_dict(tickers)
        return self._cached_tickers

    def get_price(self, pair_code: str) -> CurrencyPair | None:
        ex_sym = self._exchange_symbol(pair_code) or _symbol_to_bfx(pair_code)
        if not ex_sym:
            return None
        try:
            data = self._get("/tickers", {"symbols": ex_sym})
        except Exception:
            return None
        if not isinstance(data, list) or not data or (isinstance(data[0], list) and data[0][0] == "error"):
            return None
        row = data[0] if isinstance(data[0], list) else data
        if not isinstance(row, list) or len(row) < 8:
            return None
        last = row[7]
        ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(pair_code)
        if not ticker:
            base, quote = pair_code.split("/") if "/" in pair_code else (ex_sym[1:-3], "USD")
            if quote == "UST":
                quote = "USDT"
        else:
            base, quote = ticker.base, ticker.quote
        return CurrencyPair(
            base=base,
            quote=quote,
            ratio=float(last),
            utc=_utc_now_float(),
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        try:
            data = self._get("/tickers", {"symbols": "ALL"})
        except Exception:
            return []
        if not isinstance(data, list):
            return []
        result: list[CurrencyPair] = []
        want = None if symbols is None else {self._exchange_symbol(s) or _symbol_to_bfx(s) for s in symbols}
        for row in data:
            if not isinstance(row, list) or len(row) < 8 or row[0] == "error":
                continue
            ex_sym = row[0]
            if want is not None and ex_sym not in want:
                sym_norm = _bfx_to_symbol(ex_sym)
                if sym_norm not in (symbols or []) and (sym_norm.replace("UST", "USDT") if "UST" in sym_norm else sym_norm) not in (symbols or []):
                    continue
            ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(_bfx_to_symbol(ex_sym))
            if not ticker:
                continue
            last = row[7]
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
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_bfx(symbol)
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
        ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(symbol)
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
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_bfx(symbol)
        if not ex_sym:
            return None
        n = limit if limit is not None else self.KLINE_SIZE
        try:
            data = self._get(f"/candles/trade:1m:{ex_sym}/hist", {"limit": str(n)})
        except Exception:
            return None
        if not isinstance(data, list):
            return None
        ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(symbol)
        quote = ticker.quote if ticker else ""
        usd_vol = quote in ("USD", "UST", "USDT")
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

    def _exchange_symbol(self, symbol: str) -> str | None:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        t = self._cached_tickers_dict.get(symbol) or self._cached_tickers_dict.get(_symbol_to_bfx(symbol))
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
                self._chan_to_sym[msg.get("chanId", -1)] = msg.get("symbol", "")
            return
        if not isinstance(msg, list) or len(msg) < 2:
            return
        ch_id, payload = msg[0], msg[1]
        ex_sym = self._chan_to_sym.get(ch_id, "")
        if not ex_sym:
            return
        ticker = self._cached_tickers_dict.get(ex_sym)
        if not ticker:
            return
        if isinstance(payload, list):
            if len(payload) >= 8 and not isinstance(payload[0], list):
                if not self._throttler.may_pass(ticker.symbol, tag="book"):
                    return
                bid_p, bid_s, ask_p, ask_s = payload[1], payload[2], payload[3], payload[4]
                self._cb.handle(
                    book=BookTicker(
                        symbol=ticker.symbol,
                        bid_price=float(bid_p),
                        bid_qty=float(bid_s),
                        ask_price=float(ask_p),
                        ask_qty=float(ask_s),
                        last_update_id=ch_id,
                        utc=_utc_now_float(),
                    )
                )
            elif len(payload) >= 3 and isinstance(payload[0], (int, float)):
                if not self._throttler.may_pass(ticker.symbol, tag="depth"):
                    return
                price, count, amount = payload[0], payload[1], payload[2]
                if amount > 0:
                    bids = [BidAsk(price=float(price), quantity=float(amount))]
                    asks = []
                else:
                    bids = []
                    asks = [BidAsk(price=float(price), quantity=float(-amount))]
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
