"""MEXC perpetual futures connector (REST + WebSocket)."""

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

MEXC_CONTRACT_API = "https://api.mexc.com"
MEXC_CONTRACT_WS = "wss://contract.mexc.com/edge"
KLINE_SIZE = 60
QUOTES = ("USDT", "USDC")


def _utc_now_float() -> float:
    return time.time()


def _mexc_contract_to_symbol(s: str) -> str:
    """BTC_USDT -> BTC/USDT."""
    return s.replace("_", "/")


def _symbol_to_mexc_contract(symbol: str) -> str:
    """BTC/USDT -> BTC_USDT."""
    return symbol.replace("/", "_")


def _build_perp_dict(tickers: list[PerpetualTicker]) -> dict[str, PerpetualTicker]:
    out: dict[str, PerpetualTicker] = {}
    for t in tickers:
        out[t.symbol] = t
        out[t.exchange_symbol] = t
        out[t.symbol.replace("/", "_")] = t
        out[t.symbol.replace("/", "")] = t
    return out


class MexcPerpetualConnector(BaseCEXPerpetualConnector):
    def __init__(self, is_testing: bool = False, throttle_timeout: float = 1.0) -> None:
        super().__init__(is_testing=is_testing, throttle_timeout=throttle_timeout)
        self._cached_perps: list[PerpetualTicker] | None = None
        self._cached_perps_dict: dict[str, PerpetualTicker] = {}
        self._ws: websocket.WebSocketApp | None = None
        self._ws_thread: threading.Thread | None = None
        self._cb: Callback | None = None

    @classmethod
    def exchange_id(cls) -> str:
        return "mexc"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = MEXC_CONTRACT_API + path
        r = requests.get(url, params=params or {}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and data.get("success") is False and data.get("code", 0) != 0:
            raise RuntimeError(data.get("msg", str(data)))
        return data.get("data", data) if isinstance(data, dict) else data

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
            syms = [t.exchange_symbol for t in self._cached_perps][:200]
        else:
            syms = [
                t.exchange_symbol
                for t in self._cached_perps
                if t.symbol in symbols or t.exchange_symbol in symbols
            ]
        if not syms:
            raise RuntimeError("No symbols to subscribe")
        self._cb = cb
        self._ws = websocket.WebSocketApp(MEXC_CONTRACT_WS, on_message=self._on_ws_message)
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
            raise RuntimeError("MEXC contract WebSocket connection failed.")
        for ex_sym in syms:
            self._ws.send(json.dumps({"method": "sub.ticker", "param": {"symbol": ex_sym}}))
            if depth:
                self._ws.send(json.dumps({"method": "sub.depth", "param": {"symbol": ex_sym}}))

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
        raw = self._get("/api/v1/contract/detail")
        if isinstance(raw, dict) and "symbol" in raw:
            contracts = [raw]
        elif isinstance(raw, list):
            contracts = raw
        else:
            try:
                r = requests.get(MEXC_CONTRACT_API + "/api/v1/contract/ticker", timeout=15)
                r.raise_for_status()
                data = r.json()
                ticker_data = data.get("data", data)
                if isinstance(ticker_data, dict) and "symbol" in ticker_data:
                    contracts = [{"symbol": ticker_data["symbol"], "baseCoin": ticker_data.get("baseCoin", "BTC"), "quoteCoin": ticker_data.get("quoteCoin", "USDT"), "settleCoin": ticker_data.get("settleCoin", "USDT")}]
                elif isinstance(ticker_data, list):
                    contracts = [x if isinstance(x, dict) else {"symbol": str(x)} for x in ticker_data]
                else:
                    contracts = []
            except Exception:
                contracts = []
        perps: list[PerpetualTicker] = []
        for item in contracts:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol", "")
            if not symbol or item.get("state", 0) not in (0, None):
                continue
            base = item.get("baseCoin", item.get("base_coin", ""))
            quote = item.get("quoteCoin", item.get("quote_coin", "USDT"))
            settle = item.get("settleCoin", item.get("settle_coin", quote))
            if not base:
                base = symbol.split("_")[0] if "_" in symbol else symbol[:-5]
                quote = symbol.split("_")[1] if "_" in symbol else "USDT"
            perps.append(
                PerpetualTicker(
                    symbol=_mexc_contract_to_symbol(symbol),
                    base=base,
                    quote=quote,
                    exchange_symbol=symbol,
                    settlement=settle,
                )
            )
        self._cached_perps = perps
        self._cached_perps_dict = _build_perp_dict(perps)
        return self._cached_perps

    def get_price(self, symbol: str) -> CurrencyPair | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_mexc_contract(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get(f"/api/v1/contract/fair_price/{ex_sym}")
        except Exception:
            return None
        if not data or not isinstance(data, dict):
            return None
        price = data.get("fairPrice") or data.get("indexPrice")
        if price is None:
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(symbol)
        if not ticker:
            return None
        return CurrencyPair(
            base=ticker.base,
            quote=ticker.quote,
            ratio=float(price),
            utc=_utc_now_float(),
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        try:
            data = self._get("/api/v1/contract/ticker")
        except Exception:
            return []
        if isinstance(data, dict) and "symbol" in data:
            ticker_list = [data]
        elif isinstance(data, list):
            ticker_list = data
        else:
            ticker_list = []
        result: list[CurrencyPair] = []
        want = None if symbols is None else {self._exchange_symbol(s) or _symbol_to_mexc_contract(s) for s in symbols}
        for row in ticker_list:
            if not isinstance(row, dict):
                continue
            ex_sym = row.get("symbol", "")
            if want is not None and ex_sym not in want and _mexc_contract_to_symbol(ex_sym) not in (symbols or []):
                continue
            ticker = self._cached_perps_dict.get(ex_sym)
            if not ticker:
                continue
            price = row.get("lastPrice") or row.get("fairPrice") or row.get("indexPrice")
            if price is None:
                continue
            result.append(
                CurrencyPair(
                    base=ticker.base,
                    quote=ticker.quote,
                    ratio=float(price),
                    utc=_utc_now_float(),
                )
            )
        return result

    def get_depth(self, symbol: str, limit: int = 100) -> BookDepth | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_mexc_contract(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get(f"/api/v1/contract/depth/{ex_sym}", {"limit": str(min(limit, 100))})
        except Exception:
            return None
        if not data or not isinstance(data, dict):
            return None
        bids_raw = data.get("bids", [])
        asks_raw = data.get("asks", [])
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        bids = [BidAsk(price=float(b[0]), quantity=float(b[2]) if len(b) > 2 else float(b[1])) for b in bids_raw[:limit]]
        asks = [BidAsk(price=float(a[0]), quantity=float(a[2]) if len(a) > 2 else float(a[1])) for a in asks_raw[:limit]]
        return BookDepth(
            symbol=sym,
            exchange_symbol=ex_sym,
            bids=bids,
            asks=asks,
            last_update_id=data.get("version"),
            utc=_utc_now_float(),
        )

    def get_klines(self, symbol: str) -> list[CandleStick] | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_mexc_contract(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get(f"/api/v1/contract/kline/{ex_sym}", {"interval": "Min1"})
        except Exception:
            return None
        if not data or not isinstance(data, dict):
            return None
        times = data.get("time", [])
        opens = data.get("open", [])
        highs = data.get("high", [])
        lows = data.get("low", [])
        closes = data.get("close", [])
        vols = data.get("vol", [])
        if not times:
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(symbol)
        quote = ticker.quote if ticker else ""
        usd_vol = quote in QUOTES
        result: list[CandleStick] = []
        for i in range(min(KLINE_SIZE, len(times))):
            o = opens[i] if i < len(opens) else 0
            h = highs[i] if i < len(highs) else 0
            l = lows[i] if i < len(lows) else 0
            c = closes[i] if i < len(closes) else 0
            v = vols[i] if i < len(vols) else 0
            result.append(
                CandleStick(
                    utc_open_time=float(times[i]),
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
        t = self._cached_perps_dict.get(symbol) or self._cached_perps_dict.get(symbol.replace("/", "_"))
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
        if msg.get("error"):
            return
        channel = msg.get("channel", "")
        if channel == "pong":
            return
        data = msg.get("data", msg)
        ex_sym = msg.get("symbol") or (data.get("symbol") if isinstance(data, dict) else "")
        if not ex_sym:
            return
        ticker = self._cached_perps_dict.get(ex_sym)
        if not ticker:
            return
        if channel == "push.ticker" and isinstance(data, dict):
            if not self._throttler.may_pass(ticker.symbol, tag="book"):
                return
            bid_p = data.get("bid1", data.get("maxBidPrice", "0"))
            ask_p = data.get("ask1", data.get("minAskPrice", "0"))
            ts = data.get("timestamp", msg.get("ts", 0))
            self._cb.handle(
                book=BookTicker(
                    symbol=ticker.symbol,
                    bid_price=float(bid_p),
                    bid_qty=0.0,
                    ask_price=float(ask_p),
                    ask_qty=0.0,
                    last_update_id=ts,
                    utc=float(ts) / 1000 if ts else None,
                )
            )
        elif channel in ("push.depth", "push.depth.step") and isinstance(data, dict):
            if not self._throttler.may_pass(ticker.symbol, tag="depth"):
                return
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            ts = data.get("ct", msg.get("ts", 0))
            self._cb.handle(
                depth=BookDepth(
                    symbol=ticker.symbol,
                    exchange_symbol=ex_sym,
                    bids=[BidAsk(price=float(b[0]), quantity=float(b[1])) for b in bids],
                    asks=[BidAsk(price=float(a[0]), quantity=float(a[1])) for a in asks],
                    last_update_id=data.get("version"),
                    utc=float(ts) / 1000 if ts else None,
                )
            )
