"""Binance USD-M perpetual connector (REST + WebSocket)."""

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

BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_FAPI_TESTNET = "https://testnet.binancefuture.com"
BINANCE_FSTREAM_WS = "wss://fstream.binance.com"
BINANCE_FSTREAM_WS_TESTNET = "wss://stream.binancefuture.com"
PERPETUAL_TOKENS = ("USDT", "USDC", "BUSD")


def _utc_now_float() -> float:
    return time.time()


def _build_perp_dict(tickers: list[PerpetualTicker]) -> dict[str, PerpetualTicker]:
    out: dict[str, PerpetualTicker] = {}
    for t in tickers:
        out[t.symbol] = t
        out[t.exchange_symbol] = t
        out[t.symbol.replace("/", "")] = t
    return out


class BinancePerpetualConnector(BaseCEXPerpetualConnector):
    """Binance USD-M perpetual. REST + WebSocket."""

    REQUEST_TIMEOUT_SEC = 15
    """HTTP request timeout for REST API."""
    DEPTH_API_MAX = 500
    """Max orderbook depth allowed by Binance FAPI."""
    KLINE_SIZE = 60
    """Number of 1m candles returned by get_klines."""
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
        self._base = BINANCE_FAPI_TESTNET if is_testing else BINANCE_FAPI
        self._ws_base = BINANCE_FSTREAM_WS_TESTNET if is_testing else BINANCE_FSTREAM_WS
        self._ws: websocket.WebSocketApp | None = None
        self._ws_thread: threading.Thread | None = None
        self._cb: Callback | None = None

    @classmethod
    def exchange_id(cls) -> str:
        return "binance"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = self._base + path
        r = requests.get(url, params=params or {}, timeout=self.REQUEST_TIMEOUT_SEC)
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
        syms = [s.lower() for s in syms]
        if not syms:
            raise RuntimeError("No symbols to subscribe")
        self._cb = cb
        streams = []
        for s in syms:
            streams.append(f"{s}@bookTicker")
            if depth:
                streams.append(f"{s}@depth20@100ms")
        stream_url = f"{self._ws_base}/stream?streams=" + "/".join(streams)
        self._ws = websocket.WebSocketApp(
            stream_url,
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
            raise RuntimeError("Binance futures WebSocket connection failed.")

    def stop(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception as e:
                self.log.debug("stop: ws close failed: %s", e)
            self._ws = None
        self._ws_thread = None
        self._cb = None

    def get_all_perpetuals(self) -> list[PerpetualTicker]:
        if self._cached_perps is not None:
            return self._cached_perps
        data = self._get("/fapi/v1/exchangeInfo")
        perps: list[PerpetualTicker] = []
        for s in data.get("symbols", []):
            if s.get("status") != "TRADING" or s.get("contractType") != "PERPETUAL":
                continue
            base = s.get("baseAsset", "")
            quote = s.get("quoteAsset", "")
            sym = s.get("symbol", "")
            perps.append(
                PerpetualTicker(
                    symbol=CurrencyPair.build_code(base=base, quote=quote),
                    base=base,
                    quote=quote,
                    exchange_symbol=sym,
                    settlement=quote,
                )
            )
        self._cached_perps = perps
        self._cached_perps_dict = _build_perp_dict(perps)
        return self._cached_perps

    def get_price(self, symbol: str) -> CurrencyPair | None:
        ex_sym = self._exchange_symbol(symbol)
        if not ex_sym:
            return None
        try:
            r = self._get("/fapi/v1/ticker/price", {"symbol": ex_sym})
        except Exception as e:
            self.log.exception("get_price failed for %s: %s", symbol, e)
            return None
        if not r or "price" not in r:
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(
            symbol.replace("/", "")
        )
        if not ticker:
            return None
        return CurrencyPair(
            base=ticker.base,
            quote=ticker.quote,
            ratio=float(r["price"]),
            utc=_utc_now_float(),
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        try:
            r = self._get("/fapi/v1/ticker/price")
        except Exception as e:
            self.log.exception("get_pairs failed: %s", e)
            return []
        if not isinstance(r, list):
            return []
        price_map = {
            item["symbol"]: float(item["price"])
            for item in r
            if item.get("symbol") and item.get("price") is not None
        }
        if symbols is None:
            sym_list = [t.exchange_symbol for t in self._cached_perps]
        else:
            sym_list = []
            for s in symbols:
                ex = self._exchange_symbol(s) or s.replace("/", "").upper()
                if ex:
                    sym_list.append(ex)
        pairs: list[CurrencyPair] = []
        for ex_sym in sym_list:
            if ex_sym not in price_map:
                continue
            ticker = self._cached_perps_dict.get(ex_sym)
            if ticker:
                pairs.append(
                    CurrencyPair(
                        base=ticker.base,
                        quote=ticker.quote,
                        ratio=price_map[ex_sym],
                        utc=_utc_now_float(),
                    )
                )
        return pairs

    def get_depth(self, symbol: str, limit: int = 100) -> BookDepth | None:
        ex_sym = self._exchange_symbol(symbol)
        if not ex_sym:
            return None
        try:
            r = self._get(
                "/fapi/v1/depth",
                {"symbol": ex_sym, "limit": str(min(limit, self.DEPTH_API_MAX))},
            )
        except Exception as e:
            self.log.exception("get_depth failed for %s: %s", symbol, e)
            return None
        if not r:
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(
            symbol.replace("/", "")
        )
        sym = ticker.symbol if ticker else symbol
        bids = [BidAsk(price=float(p), quantity=float(q)) for p, q in r.get("bids", [])]
        asks = [BidAsk(price=float(p), quantity=float(q)) for p, q in r.get("asks", [])]
        return BookDepth(
            symbol=sym,
            exchange_symbol=ex_sym,
            bids=bids,
            asks=asks,
            last_update_id=r.get("lastUpdateId"),
            utc=_utc_now_float(),
        )

    def get_klines(self, symbol: str, limit: int | None = None) -> list[CandleStick] | None:
        ex_sym = self._exchange_symbol(symbol)
        if not ex_sym:
            return None
        n = limit if limit is not None else self.KLINE_SIZE
        try:
            rows = self._get(
                "/fapi/v1/klines",
                {"symbol": ex_sym, "interval": "1m", "limit": str(n)},
            )
        except Exception as e:
            self.log.exception("get_klines failed for %s: %s", symbol, e)
            return None
        if not rows:
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(
            symbol.replace("/", "")
        )
        quote = ticker.quote if ticker else ""
        usd_vol = quote in PERPETUAL_TOKENS
        return [
            CandleStick(
                utc_open_time=float(r[0]) / 1000,
                open_price=float(r[1]),
                high_price=float(r[2]),
                low_price=float(r[3]),
                close_price=float(r[4]),
                coin_volume=float(r[5]),
                usd_volume=float(r[5]) * float(r[4]) if usd_vol else None,
            )
            for r in rows
        ]

    def get_funding_rate(self, symbol: str) -> FundingRate | None:
        ex_sym = self._exchange_symbol(symbol)
        if not ex_sym:
            return None
        try:
            r = self._get("/fapi/v1/premiumIndex", {"symbol": ex_sym})
        except Exception as e:
            self.log.exception("get_funding_rate failed for %s: %s", symbol, e)
            return None
        if not r or "lastFundingRate" not in r:
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(
            symbol.replace("/", "")
        )
        sym = ticker.symbol if ticker else symbol
        next_ms = r.get("nextFundingTime")
        next_utc = float(next_ms) / 1000 if next_ms is not None else 0.0
        try:
            index_price = float(r["indexPrice"]) if r.get("indexPrice") is not None else None
        except (TypeError, ValueError):
            index_price = None
        return FundingRate(
            symbol=sym,
            rate=float(r["lastFundingRate"]),
            next_funding_utc=next_utc,
            index_price=index_price,
            utc=_utc_now_float(),
        )

    def get_funding_rate_history(
        self, symbol: str, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        ex_sym = self._exchange_symbol(symbol)
        if not ex_sym:
            return None
        n = limit if limit is not None else DEFAULT_FUNDING_HISTORY_LIMIT
        try:
            rows = self._get("/fapi/v1/fundingRate", {"symbol": ex_sym, "limit": str(n)})
        except Exception as e:
            self.log.exception("get_funding_rate_history failed for %s: %s", symbol, e)
            return None
        if not isinstance(rows, list):
            return None
        return [
            FundingRatePoint(
                funding_time_utc=float(r["fundingTime"]) / 1000,
                rate=float(r["fundingRate"]),
            )
            for r in rows
        ]

    def _exchange_symbol(self, symbol: str) -> str | None:
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        t = (
            self._cached_perps_dict.get(symbol)
            or self._cached_perps_dict.get(symbol.replace("/", ""))
            or self._cached_perps_dict.get(symbol.replace("/", "").upper())
        )
        return t.exchange_symbol if t else None

    def _on_ws_message(self, _: Any, raw: bytes | str) -> None:
        if not self._cb:
            return
        if isinstance(raw, bytes):
            raw = raw.decode()
        try:
            msg = json.loads(raw)
        except Exception as e:
            self.log.exception("_on_ws_message json parse failed: %s", e)
            return
        if "stream" in msg:
            stream = msg.get("stream", "")
            data = msg.get("data", msg)
        else:
            data = msg
            stream = data.get("e", "")
        sym_key = data.get("s", "").upper()
        ticker = self._cached_perps_dict.get(sym_key) or self._cached_perps_dict.get(
            data.get("s", "")
        )
        if not ticker:
            return
        sym = ticker.symbol
        if "bookTicker" in stream or data.get("e") == "bookTicker":
            if not self._throttler.may_pass(sym, tag="book"):
                return
            self._cb.handle(
                book=BookTicker(
                    symbol=sym,
                    bid_price=float(data.get("b", 0)),
                    bid_qty=float(data.get("B", 0)),
                    ask_price=float(data.get("a", 0)),
                    ask_qty=float(data.get("A", 0)),
                    last_update_id=data.get("u"),
                    utc=float(data["E"]) / 1000 if data.get("E") else None,
                )
            )
        elif "depth" in stream or data.get("e") == "depthUpdate":
            if not self._throttler.may_pass(sym, tag="depth"):
                return
            bids = [BidAsk(price=float(p), quantity=float(q)) for p, q in data.get("b", [])]
            asks = [BidAsk(price=float(p), quantity=float(q)) for p, q in data.get("a", [])]
            if not bids and not asks:
                return
            self._cb.handle(
                depth=BookDepth(
                    symbol=sym,
                    exchange_symbol=ticker.exchange_symbol or sym_key,
                    bids=bids,
                    asks=asks,
                    last_update_id=data.get("u"),
                    utc=float(data["E"]) / 1000 if data.get("E") else None,
                )
            )
