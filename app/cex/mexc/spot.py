"""MEXC spot connector (REST + WebSocket)."""

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

MEXC_SPOT_API = "https://api.mexc.com"
MEXC_SPOT_WS = "wss://wbs-api.mexc.com/ws"
QUOTES = ("USDT", "USDC", "BTC", "ETH")


def _utc_now_float() -> float:
    return time.time()


def _mexc_to_symbol(s: str) -> str:
    """BTCUSDT -> BTC/USDT."""
    for q in QUOTES:
        if s.endswith(q):
            return f"{s[:-len(q)]}/{q}"
    return s


def _symbol_to_mexc(symbol: str) -> str:
    """BTC/USDT -> BTCUSDT."""
    return symbol.replace("/", "")


def _build_tickers_dict(tickers: list[Ticker]) -> dict[str, Ticker]:
    out: dict[str, Ticker] = {}
    for t in tickers:
        out[t.symbol] = t
        if t.exchange_symbol:
            out[t.exchange_symbol] = t
            out[t.exchange_symbol.upper()] = t
        out[t.symbol.replace("/", "")] = t
    return out


class MexcSpotConnector(BaseCEXSpotConnector):
    REQUEST_TIMEOUT_SEC = 15
    DEPTH_API_MAX = 5000
    KLINE_SIZE = 60
    DEPTH_POLL_INTERVAL_SEC = 1

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
        self._poll_run: bool = False
        self._poll_thread: threading.Thread | None = None
        self._poll_syms: list[str] = []
        self._poll_depth: bool = True

    @classmethod
    def exchange_id(cls) -> str:
        return "mexc"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = MEXC_SPOT_API + path
        r = self._request_limited(url, params or {}, self.REQUEST_TIMEOUT_SEC)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "code" in data and data.get("code") != 200 and data.get("code") != 0:
            raise RuntimeError(data.get("msg", str(data)))
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
        self._poll_syms = list(syms)
        self._poll_depth = depth
        self._poll_run = True
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()

    def _poll_loop(self) -> None:
        while self._poll_run and self._cb:
            for ex_sym in self._poll_syms:
                if not self._poll_run:
                    break
                ticker = self._cached_tickers_dict.get(ex_sym)
                if not ticker:
                    continue
                if not self._throttler.may_pass(ticker.symbol, tag="book"):
                    continue
                try:
                    data = self._get("/api/v3/ticker/bookTicker", {"symbol": ex_sym})
                except Exception:
                    continue
                if isinstance(data, dict) and data.get("symbol") == ex_sym:
                    bid_p = data.get("bidPrice", "0")
                    ask_p = data.get("askPrice", "0")
                    bid_s = data.get("bidQty", "0")
                    ask_s = data.get("askQty", "0")
                    self._cb.handle(
                        book=BookTicker(
                            symbol=ticker.symbol,
                            bid_price=float(bid_p),
                            bid_qty=float(bid_s),
                            ask_price=float(ask_p),
                            ask_qty=float(ask_s),
                            last_update_id=None,
                            utc=_utc_now_float(),
                        )
                    )
                if self._poll_depth and self._throttler.may_pass(ticker.symbol, tag="depth"):
                    try:
                        depth_data = self._get("/api/v3/depth", {"symbol": ex_sym, "limit": "20"})
                    except Exception:
                        continue
                    if isinstance(depth_data, dict):
                        bids = depth_data.get("bids", [])[:10]
                        asks = depth_data.get("asks", [])[:10]
                        self._cb.handle(
                            depth=BookDepth(
                                symbol=ticker.symbol,
                                exchange_symbol=ex_sym,
                                bids=[BidAsk(price=float(p), quantity=float(q)) for p, q in bids],
                                asks=[BidAsk(price=float(p), quantity=float(q)) for p, q in asks],
                                last_update_id=depth_data.get("lastUpdateId"),
                                utc=_utc_now_float(),
                            )
                        )
            time.sleep(self.DEPTH_POLL_INTERVAL_SEC)

    def stop(self) -> None:
        self._poll_run = False
        self._poll_thread = None
        self._poll_syms = []
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
        data = self._get("/api/v3/exchangeInfo")
        symbols_list = data.get("symbols", data) if isinstance(data, dict) else []
        if not isinstance(symbols_list, list):
            symbols_list = []
        tickers: list[Ticker] = []
        for item in symbols_list:
            if item.get("status") != "1":
                continue
            sym = item.get("symbol", "")
            base = item.get("baseAsset", "")
            quote = item.get("quoteAsset", "")
            if not sym:
                continue
            symbol = _mexc_to_symbol(sym) if not base else f"{base}/{quote}"
            tickers.append(
                Ticker(
                    symbol=symbol,
                    base=base or symbol.split("/")[0],
                    quote=quote or symbol.split("/")[1] if "/" in symbol else "USDT",
                    is_spot_enabled=True,
                    is_margin_enabled=bool(item.get("isMarginTradingAllowed")),
                    exchange_symbol=sym,
                )
            )
        self._cached_tickers = tickers
        self._cached_tickers_dict = _build_tickers_dict(tickers)
        return self._cached_tickers

    def get_price(self, pair_code: str) -> CurrencyPair | None:
        ex_sym = self._exchange_symbol(pair_code) or _symbol_to_mexc(pair_code)
        if not ex_sym:
            return None
        try:
            data = self._get("/api/v3/ticker/price", {"symbol": ex_sym})
        except Exception:
            return None
        if isinstance(data, list):
            row = next((x for x in data if isinstance(x, dict) and x.get("symbol") == ex_sym), None)
        else:
            row = data if isinstance(data, dict) and data.get("symbol") == ex_sym else None
        if not row or row.get("symbol") != ex_sym:
            return None
        price = row.get("price")
        if price is None:
            return None
        ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(pair_code)
        if not ticker:
            base, quote = pair_code.split("/") if "/" in pair_code else (ex_sym[:-4], "USDT")
        else:
            base, quote = ticker.base, ticker.quote
        return CurrencyPair(
            base=base,
            quote=quote,
            ratio=float(price),
            utc=_utc_now_float(),
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        try:
            data = self._get("/api/v3/ticker/24hr")
        except Exception:
            return []
        if isinstance(data, dict) and "symbol" in data:
            ticker_list = [data]
        elif isinstance(data, list):
            ticker_list = data
        else:
            ticker_list = []
        result: list[CurrencyPair] = []
        want = None if symbols is None else {self._exchange_symbol(s) or _symbol_to_mexc(s) for s in symbols}
        for row in ticker_list:
            if not isinstance(row, dict):
                continue
            ex_sym = row.get("symbol", "")
            if want is not None and ex_sym not in want:
                if _mexc_to_symbol(ex_sym) not in (symbols or []):
                    continue
            ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(_mexc_to_symbol(ex_sym))
            if not ticker:
                continue
            last = row.get("lastPrice")
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
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_mexc(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get("/api/v3/depth", {"symbol": ex_sym, "limit": str(min(limit, self.DEPTH_API_MAX))})
        except Exception:
            return None
        if not data or not isinstance(data, dict):
            return None
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        return BookDepth(
            symbol=sym,
            exchange_symbol=ex_sym,
            bids=[BidAsk(price=float(p), quantity=float(q)) for p, q in bids[:limit]],
            asks=[BidAsk(price=float(p), quantity=float(q)) for p, q in asks[:limit]],
            last_update_id=data.get("lastUpdateId"),
            utc=_utc_now_float(),
        )

    def get_klines(self, symbol: str, limit: int | None = None) -> list[CandleStick] | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_mexc(symbol)
        if not ex_sym:
            return None
        n = limit if limit is not None else self.KLINE_SIZE
        try:
            data = self._get("/api/v3/klines", {"symbol": ex_sym, "interval": "1m", "limit": str(n)})
        except Exception:
            return None
        if not isinstance(data, list):
            return None
        ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(symbol)
        quote = ticker.quote if ticker else ""
        usd_vol = quote in QUOTES
        result: list[CandleStick] = []
        for row in data[:n]:
            if not isinstance(row, list) or len(row) < 7:
                continue
            ts, o, h, l, c, vol = row[0], row[1], row[2], row[3], row[4], row[5]
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
        t = self._cached_tickers_dict.get(symbol) or self._cached_tickers_dict.get(symbol.replace("/", ""))
        return t.exchange_symbol if t else None

    def _on_ws_message(self, _: Any, raw: bytes | str) -> None:
        if not self._cb:
            return
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except Exception:
                return
        try:
            msg = json.loads(raw)
        except Exception:
            return
        if "channel" not in msg:
            return
        channel = msg.get("channel", "")
        ex_sym = msg.get("symbol", "")
        if not ex_sym:
            return
        ticker = self._cached_tickers_dict.get(ex_sym)
        if not ticker:
            return
        if "bookTicker" in channel:
            data = msg.get("publicbookticker", msg.get("publicBookTickerBatch", {}))
            if isinstance(data, dict) and "items" in data:
                items = data.get("items", [])
                data = items[0] if items else {}
            if not isinstance(data, dict):
                return
            if not self._throttler.may_pass(ticker.symbol, tag="book"):
                return
            bid_p = data.get("bidprice", data.get("bidPrice", "0"))
            ask_p = data.get("askprice", data.get("askPrice", "0"))
            bid_s = data.get("bidquantity", data.get("bidQuantity", "0"))
            ask_s = data.get("askquantity", data.get("askQuantity", "0"))
            sendtime = msg.get("sendtime", msg.get("sendTime", 0))
            self._cb.handle(
                book=BookTicker(
                    symbol=ticker.symbol,
                    bid_price=float(bid_p),
                    bid_qty=float(bid_s),
                    ask_price=float(ask_p),
                    ask_qty=float(ask_s),
                    last_update_id=sendtime,
                    utc=float(sendtime) / 1000 if sendtime else None,
                )
            )
        elif "depth" in channel:
            data = msg.get("publicincreasedepths", msg.get("publiclimitdepths", {}))
            if not isinstance(data, dict):
                return
            if not self._throttler.may_pass(ticker.symbol, tag="depth"):
                return
            bids = data.get("bidsList", data.get("bids", []))
            asks = data.get("asksList", data.get("asks", []))
            sendtime = msg.get("sendtime", msg.get("sendTime", 0))
            self._cb.handle(
                depth=BookDepth(
                    symbol=ticker.symbol,
                    exchange_symbol=ex_sym,
                    bids=[BidAsk(price=float(b.get("price", b[0] if isinstance(b, (list, tuple)) else 0)), quantity=float(b.get("quantity", b[1] if isinstance(b, (list, tuple)) and len(b) > 1 else 0))) for b in bids],
                    asks=[BidAsk(price=float(a.get("price", a[0] if isinstance(a, (list, tuple)) else 0)), quantity=float(a.get("quantity", a[1] if isinstance(a, (list, tuple)) and len(a) > 1 else 0))) for a in asks],
                    last_update_id=data.get("version", data.get("toVersion")),
                    utc=float(sendtime) / 1000 if sendtime else None,
                )
            )
