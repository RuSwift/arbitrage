"""OKX spot connector (REST + WebSocket)."""

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

OKX_REST = "https://www.okx.com"
OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"
OKX_WS_TESTNET = "wss://wss.okx.com:8443/ws/v5/public"
KLINE_SIZE = 60
QUOTES = ("USDT", "USDC", "BTC", "ETH")


def _utc_now_float() -> float:
    return time.time()


def _okx_to_symbol(inst_id: str) -> str:
    """BTC-USDT -> BTC/USDT."""
    return inst_id.replace("-", "/", 1)


def _symbol_to_okx(symbol: str) -> str:
    """BTC/USDT -> BTC-USDT."""
    return symbol.replace("/", "-")


def _build_tickers_dict(tickers: list[Ticker]) -> dict[str, Ticker]:
    out: dict[str, Ticker] = {}
    for t in tickers:
        out[t.symbol] = t
        if t.exchange_symbol:
            out[t.exchange_symbol] = t
        out[t.symbol.replace("/", "")] = t
        out[t.symbol.replace("/", "-")] = t
    return out


class OkxSpotConnector(BaseCEXSpotConnector):
    def __init__(self, is_testing: bool = False, throttle_timeout: float = 1.0) -> None:
        super().__init__(is_testing=is_testing, throttle_timeout=throttle_timeout)
        self._cached_tickers: list[Ticker] | None = None
        self._cached_tickers_dict: dict[str, Ticker] = {}
        self._ws: websocket.WebSocketApp | None = None
        self._ws_thread: threading.Thread | None = None
        self._cb: Callback | None = None

    @classmethod
    def exchange_id(cls) -> str:
        return "okx"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = OKX_REST + path
        r = requests.get(url, params=params or {}, timeout=15)
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
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        if symbols is None:
            syms = [t.exchange_symbol for t in self._cached_tickers if t.exchange_symbol][:200]
        else:
            syms = [
                t.exchange_symbol
                for t in self._cached_tickers
                if t.exchange_symbol and t.symbol in symbols
            ]
        if not syms:
            raise RuntimeError("No symbols to subscribe")
        self._cb = cb
        ws_url = OKX_WS_TESTNET if self._is_testing else OKX_WS
        self._ws = websocket.WebSocketApp(ws_url, on_message=self._on_ws_message)
        self._ws_thread = threading.Thread(target=lambda: self._ws.run_forever())
        self._ws_thread.daemon = True
        self._ws_thread.start()
        for _ in range(10):
            if self._ws.sock and self._ws.sock.connected:
                break
            time.sleep(1)
        if not self._ws.sock or not self._ws.sock.connected:
            self._ws = None
            self._ws_thread = None
            raise RuntimeError("OKX spot WebSocket connection failed.")
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

    def get_all_tickers(self) -> list[Ticker]:
        if self._cached_tickers is not None:
            return self._cached_tickers
        data = self._get("/api/v5/public/instruments", {"instType": "SPOT"})
        if not isinstance(data, list):
            raise RuntimeError("Failed to get OKX spot instruments")
        tickers: list[Ticker] = []
        for item in data:
            if item.get("state") != "live":
                continue
            inst_id = item.get("instId", "")
            base = item.get("baseCcy", inst_id.split("-")[0] if "-" in inst_id else "")
            quote = item.get("quoteCcy", inst_id.split("-")[1] if "-" in inst_id else "")
            tickers.append(
                Ticker(
                    symbol=_okx_to_symbol(inst_id),
                    base=base,
                    quote=quote,
                    is_spot_enabled=True,
                    is_margin_enabled=False,
                    exchange_symbol=inst_id,
                )
            )
        self._cached_tickers = tickers
        self._cached_tickers_dict = _build_tickers_dict(tickers)
        return self._cached_tickers

    def get_price(self, pair_code: str) -> CurrencyPair | None:
        inst_id = _symbol_to_okx(pair_code)
        tickers_data = self._get("/api/v5/market/ticker", {"instId": inst_id})
        if not tickers_data or not isinstance(tickers_data, list):
            return None
        row = tickers_data[0]
        last = row.get("last")
        if last is None:
            return None
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        ticker = self._cached_tickers_dict.get(inst_id) or self._cached_tickers_dict.get(pair_code)
        if not ticker:
            base, quote = pair_code.split("/") if "/" in pair_code else (row.get("instId", "").split("-")[0], row.get("instId", "").split("-")[1])
            return CurrencyPair(base=base, quote=quote, ratio=float(last), utc=_utc_now_float())
        return CurrencyPair(
            base=ticker.base,
            quote=ticker.quote,
            ratio=float(last),
            utc=_utc_now_float(),
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        data = self._get("/api/v5/market/tickers", {"instType": "SPOT"})
        if not isinstance(data, list):
            return []
        result: list[CurrencyPair] = []
        want = None if symbols is None else {_symbol_to_okx(s) for s in symbols} | {s.replace("/", "") for s in (symbols or [])}
        for row in data:
            inst_id = row.get("instId", "")
            if want is not None and inst_id not in want:
                continue
            ticker = self._cached_tickers_dict.get(inst_id)
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
        inst_id = self._exchange_symbol(symbol) or _symbol_to_okx(symbol)
        if not inst_id:
            return None
        data = self._get("/api/v5/market/books", {"instId": inst_id, "sz": str(min(limit, 400))})
        if not data or not isinstance(data, list):
            return None
        # OKX returns [{"bids":[[price,sz,qty,...]],"asks":[...],"ts":...}]
        book = data[0] if isinstance(data[0], dict) else {}
        bids_raw = book.get("bids", [])
        asks_raw = book.get("asks", [])
        bids = [BidAsk(price=float(b[0]), quantity=float(b[1])) for b in bids_raw]
        asks = [BidAsk(price=float(a[0]), quantity=float(a[1])) for a in asks_raw]
        ticker = self._cached_tickers_dict.get(inst_id) or self._cached_tickers_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        return BookDepth(
            symbol=sym,
            exchange_symbol=inst_id,
            bids=bids,
            asks=asks,
            last_update_id=book.get("ts"),
            utc=_utc_now_float(),
        )

    def get_klines(self, symbol: str) -> list[CandleStick] | None:
        inst_id = self._exchange_symbol(symbol) or _symbol_to_okx(symbol)
        if not inst_id:
            return None
        data = self._get(
            "/api/v5/market/candles",
            {"instId": inst_id, "bar": "1m", "limit": str(KLINE_SIZE)},
        )
        if not isinstance(data, list):
            return None
        # OKX: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        ticker = self._cached_tickers_dict.get(inst_id) or self._cached_tickers_dict.get(symbol)
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

    def _exchange_symbol(self, symbol: str) -> str | None:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        t = (
            self._cached_tickers_dict.get(symbol)
            or self._cached_tickers_dict.get(symbol.replace("/", "-"))
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
            ticker = self._cached_tickers_dict.get(inst_id)
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
