"""Binance spot connector (REST + WebSocket)."""

from __future__ import annotations

import json
import time
from typing import Any

from binance.spot import Spot
from binance.websocket.websocket_client import BinanceWebsocketClient

from app.cex.base import BaseCEXSpotConnector, Callback
from app.cex.dto import (
    BidAsk,
    BookDepth,
    BookTicker,
    CandleStick,
    CurrencyPair,
    Ticker,
)

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
PERPETUAL_TOKENS = ("USDT", "USDC", "BUSD")


def _utc_now_float() -> float:
    return time.time()


def _build_tickers_dict(tickers: list[Ticker]) -> dict[str, Ticker]:
    out: dict[str, Ticker] = {}
    for t in tickers:
        out[t.symbol] = t
        if t.exchange_symbol:
            out[t.exchange_symbol] = t
            out[t.exchange_symbol.upper()] = t
        out[t.symbol.replace("/", "")] = t
    return out


class BinanceSpotConnector(BaseCEXSpotConnector):
    """Binance spot. REST + WebSocket."""

    DEPTH_API_MAX = 5000
    """Max orderbook depth allowed by Binance spot API."""
    KLINE_SIZE = 60
    """Number of 1m candles returned by get_klines."""

    def __init__(self, is_testing: bool = False, throttle_timeout: float = 1.0) -> None:
        super().__init__(is_testing=is_testing, throttle_timeout=throttle_timeout)
        self._cached_tickers: list[Ticker] | None = None
        self._cached_tickers_dict: dict[str, Ticker] = {}
        base_url = "https://testnet.binance.vision" if is_testing else "https://api.binance.com"
        self._api = Spot(base_url=base_url)
        self._ws: BinanceWebsocketClient | None = None
        self._cb: Callback | None = None

    @classmethod
    def exchange_id(cls) -> str:
        return "binance"

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
        syms = [s.lower() for s in syms]
        if not syms:
            raise RuntimeError("No symbols to subscribe")
        self._cb = cb
        streams: list[str] = []
        for s in syms:
            streams.append(f"{s}@bookTicker")
            if depth:
                streams.append(f"{s}@depth20@100ms")
        stream_url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)
        self._ws = BinanceWebsocketClient(stream_url, on_message=self._on_ws_message)

    def stop(self) -> None:
        if self._ws is not None:
            try:
                self._ws.stop()
            except Exception:
                pass
            self._ws = None
        self._cb = None

    def get_all_tickers(self) -> list[Ticker]:
        if self._cached_tickers is not None:
            return self._cached_tickers
        data = self._api.exchange_info()
        tickers: list[Ticker] = []
        for s in data.get("symbols", []):
            if s.get("status") != "TRADING":
                continue
            base = s.get("baseAsset", "")
            quote = s.get("quoteAsset", "")
            sym = s.get("symbol", "")
            tickers.append(
                Ticker(
                    symbol=CurrencyPair.build_code(base=base, quote=quote),
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
        sym = pair_code.replace("/", "").upper()
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        ticker = self._cached_tickers_dict.get(sym) or self._cached_tickers_dict.get(
            pair_code.replace("/", "")
        )
        if not ticker:
            return None
        try:
            r = self._api.ticker_price(symbol=ticker.exchange_symbol or sym)
        except Exception:
            return None
        if not r or "price" not in r:
            return None
        return CurrencyPair(
            base=ticker.base,
            quote=ticker.quote,
            ratio=float(r["price"]),
            utc=_utc_now_float(),
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        try:
            if symbols is None:
                prices = self._api.ticker_price()
            else:
                sym_list = []
                for s in symbols:
                    ex = self._exchange_symbol(s) or s.replace("/", "").upper()
                    if ex:
                        sym_list.append(ex)
                if not sym_list:
                    return []
                if len(sym_list) == 1:
                    prices = [self._api.ticker_price(symbol=sym_list[0])]
                else:
                    prices = self._api.ticker_price(symbols=sym_list)
        except Exception:
            return []
        if not isinstance(prices, list):
            prices = [prices]
        pairs: list[CurrencyPair] = []
        for p in prices:
            ex_sym = p.get("symbol", "")
            ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(
                ex_sym.upper()
            )
            if ticker:
                pairs.append(
                    CurrencyPair(
                        base=ticker.base,
                        quote=ticker.quote,
                        ratio=float(p.get("price", 0)),
                        utc=_utc_now_float(),
                    )
                )
        return pairs

    def get_depth(self, symbol: str, limit: int = 100) -> BookDepth | None:
        ex_sym = self._exchange_symbol(symbol)
        if not ex_sym:
            return None
        try:
            r = self._api.depth(symbol=ex_sym, limit=min(limit, self.DEPTH_API_MAX))
        except Exception:
            return None
        if not r:
            return None
        ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(
            ex_sym.upper()
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
            rows = self._api.klines(
                symbol=ex_sym,
                interval="1m",
                limit=n,
            )
        except Exception:
            return None
        if not rows:
            return None
        ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(
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

    def _exchange_symbol(self, symbol: str) -> str | None:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        t = (
            self._cached_tickers_dict.get(symbol)
            or self._cached_tickers_dict.get(symbol.replace("/", ""))
            or self._cached_tickers_dict.get(symbol.replace("/", "").upper())
        )
        return t.exchange_symbol if t else None

    def _on_ws_message(self, _socket_manager: Any, raw: str) -> None:
        if not self._cb:
            return
        try:
            msg = json.loads(raw)
        except Exception:
            return
        if "stream" in msg:
            stream = msg.get("stream", "")
            data = msg.get("data", msg)
        else:
            data = msg
            stream = data.get("e", "")
        sym_key = data.get("s", "").upper()
        ticker = self._cached_tickers_dict.get(sym_key) or self._cached_tickers_dict.get(
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
                    utc=float(data.get("E", 0)) / 1000,
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
                    utc=float(data.get("E", 0)) / 1000,
                )
            )
