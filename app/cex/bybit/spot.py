"""Bybit spot connector (REST + WebSocket)."""

from __future__ import annotations

import logging
import time
from typing import Any

from pybit.exceptions import InvalidRequestError
from pybit.unified_trading import MarketHTTP, WebSocket

from app.cex.base import BaseCEXSpotConnector, Callback
from app.cex.dto import (
    BidAsk,
    BookDepth,
    BookTicker,
    CandleStick,
    CurrencyPair,
    Ticker,
)

PERPETUAL_TOKENS = ("USDT", "USDC", "DAI")


def _utc_now_float() -> float:
    return time.time()


def _build_tickers_dict(tickers: list[Ticker]) -> dict[str, Ticker]:
    out: dict[str, Ticker] = {}
    for t in tickers:
        out[t.symbol] = t
        if t.exchange_symbol:
            out[t.exchange_symbol] = t
        out[t.symbol.replace("/", "")] = t
    return out


class BybitSpotConnector(BaseCEXSpotConnector):
    """Bybit spot. REST + WebSocket."""

    _subscription_batch_sec = 15.0  # reconnect-based; batch longer to reduce reconnects
    KLINE_WINDOW_SECS = 60 * 60
    """Time window for get_klines (1 hour)."""
    KLINE_SIZE = 60
    """Number of 1m candles (KLINE_WINDOW_SECS // 60)."""
    ORDERBOOK_BOOK_DEPTH = 1
    """Depth level for book-ticker stream."""
    ORDERBOOK_DEPTH_LEVELS = 50
    """Depth levels for orderbook stream when depth=True."""

    def __init__(
        self,
        is_testing: bool = False,
        throttle_timeout: float = 1.0,
        log: logging.Logger | None = None,
    ) -> None:
        super().__init__(is_testing=is_testing, throttle_timeout=throttle_timeout, log=log)
        self._cached_tickers: list[Ticker] | None = None
        self._cached_tickers_dict: dict[str, Ticker] = {}
        self._api = MarketHTTP(testnet=is_testing)
        self._ws: WebSocket | None = None
        self._cb: Callback | None = None
        self._ws_depth = True
        self._ws_subscribed_symbols: set[str] = set()

    @classmethod
    def exchange_id(cls) -> str:
        return "bybit"

    def start(
        self,
        cb: Callback,
        symbols: list[str] | None = None,
        depth: bool = True,
    ) -> None:
        if self._ws is not None:
            raise RuntimeError("WebSocket already active. Call stop() first.")
        self._ws = WebSocket(channel_type="spot", testnet=self._is_testing)
        if not self._ws.is_connected():
            self._ws = None
            raise RuntimeError("Bybit spot WebSocket connection failed.")
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        if symbols is None:
            syms = [t.exchange_symbol for t in self._cached_tickers if t.exchange_symbol]
        else:
            symbols_set = {s.lower() for s in symbols}
            syms = [
                t.exchange_symbol
                for t in self._cached_tickers
                if t.exchange_symbol
                and (t.symbol in symbols or (t.exchange_symbol or "").lower() in symbols_set)
            ]
        self._cb = cb
        self._ws_depth = depth
        self._ws_subscribed_symbols = set(syms)
        for sym in syms:
            self._ws.orderbook_stream(depth=self.ORDERBOOK_BOOK_DEPTH, symbol=sym, callback=self._on_ws_message)
            if depth:
                self._ws.orderbook_stream(depth=self.ORDERBOOK_DEPTH_LEVELS, symbol=sym, callback=self._on_ws_message)

    def stop(self) -> None:
        self._cancel_subscription_timer()
        if self._ws is not None:
            try:
                self._ws.exit()
            except Exception as e:
                self.log.debug("stop: ws exit failed: %s", e)
            self._ws = None
        self._cb = None
        self._ws_subscribed_symbols = set()
        self._ws_depth = True

    def _resolve_tokens_to_exchange_symbols(self, tokens: list[str]) -> list[str]:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        out: list[str] = []
        for t in tokens:
            ex = self._exchange_symbol(t)
            if ex:
                out.append(ex)
        return out

    def _apply_subscribe(self, tokens: list[str]) -> None:
        for ex_sym in self._resolve_tokens_to_exchange_symbols(tokens):
            self._ws_subscribed_symbols.add(ex_sym)

    def _apply_unsubscribe(self, tokens: list[str]) -> None:
        for ex_sym in self._resolve_tokens_to_exchange_symbols(tokens):
            self._ws_subscribed_symbols.discard(ex_sym)

    def _after_subscription_flush(self) -> None:
        if self._ws is None:
            return
        new_symbols = list(self._ws_subscribed_symbols)
        cb = self._cb
        depth = self._ws_depth
        self.stop()
        if new_symbols and cb is not None:
            self.start(cb, new_symbols, depth)

    def get_all_tickers(self) -> list[Ticker]:
        if self._cached_tickers is not None:
            return self._cached_tickers
        resp = self._api.get_instruments_info(category="spot", status="Trading")
        if resp.get("retCode") != 0:
            raise RuntimeError(resp.get("retMsg", "Failed to get spot instruments"))
        tickers: list[Ticker] = []
        for item in resp.get("result", {}).get("list", []):
            tickers.append(
                Ticker(
                    symbol=CurrencyPair.build_code(
                        base=item["baseCoin"], quote=item["quoteCoin"]
                    ),
                    base=item["baseCoin"],
                    quote=item["quoteCoin"],
                    is_spot_enabled=True,
                    is_margin_enabled=False,
                    exchange_symbol=item["symbol"],
                )
            )
        self._cached_tickers = tickers
        self._cached_tickers_dict = _build_tickers_dict(tickers)
        return tickers

    def get_price(self, pair_code: str) -> CurrencyPair | None:
        sym = pair_code.replace("/", "")
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        ticker = self._cached_tickers_dict.get(sym)
        if not ticker:
            return None
        try:
            r = self._api.get_tickers(category="spot", symbol=sym)
        except InvalidRequestError as e:
            self.log.exception("get_price failed for %s: %s", pair_code, e)
            return None
        if r.get("retCode") != 0:
            return None
        lst = r.get("result", {}).get("list", [])
        if not lst:
            return None
        rec = lst[0]
        return CurrencyPair(
            base=ticker.base,
            quote=ticker.quote,
            ratio=float(rec["lastPrice"]),
            utc=_utc_now_float(),
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        if symbols is None:
            want = set(self._cached_tickers_dict.keys())
        else:
            want = {s.replace("/", "") for s in symbols}
        r = self._api.get_tickers(category="spot")
        if r.get("retCode") != 0:
            raise RuntimeError(r.get("retMsg", "Failed to get tickers"))
        pairs: list[CurrencyPair] = []
        for p in r.get("result", {}).get("list", []):
            s = p.get("symbol")
            if s not in want:
                continue
            t = self._cached_tickers_dict.get(s)
            if t:
                pairs.append(
                    CurrencyPair(
                        base=t.base,
                        quote=t.quote,
                        ratio=float(p["lastPrice"]),
                        utc=_utc_now_float(),
                    )
                )
        return pairs

    def get_depth(self, symbol: str, limit: int = 100) -> BookDepth | None:
        ex_sym = self._exchange_symbol(symbol)
        if not ex_sym:
            return None
        r = self._api.get_orderbook(category="spot", symbol=ex_sym, limit=limit)
        if r.get("retCode") != 0:
            raise RuntimeError(r.get("retMsg", "Failed to get orderbook"))
        data = r.get("result", {})
        _, depth = self._raw_to_events(data)
        return depth

    def get_klines(self, symbol: str, limit: int | None = None) -> list[CandleStick] | None:
        ex_sym = self._exchange_symbol(symbol)
        if not ex_sym:
            return None
        n = limit if limit is not None else self.KLINE_SIZE
        r = self._api.get_kline(
            category="spot",
            symbol=ex_sym,
            interval="1",
            limit=n,
        )
        if r.get("retCode") != 0:
            raise RuntimeError(r.get("retMsg", "Failed to get klines"))
        items = r.get("result", {}).get("list", [])
        return [self._parse_candle(symbol, i) for i in items]

    def _exchange_symbol(self, symbol: str) -> str | None:
        if not self._cached_tickers_dict:
            self.get_all_tickers()
        t = self._cached_tickers_dict.get(symbol) or self._cached_tickers_dict.get(
            symbol.replace("/", "")
        )
        return t.exchange_symbol if t else None

    def _on_ws_message(self, message: dict[str, Any]) -> None:
        if not self._cb:
            return
        topic = message.get("topic", "")
        if not topic.startswith("orderbook."):
            return
        parts = topic.split(".")
        depth_level = parts[1] if len(parts) >= 2 else ""
        data = message.get("data", {})
        if not data:
            return
        book_ev, depth_ev = self._raw_to_events(data)
        if depth_level == "1":
            depth_ev = None
        else:
            book_ev = None
        if book_ev and not self._throttler.may_pass(book_ev.symbol, tag="book"):
            book_ev = None
        if depth_ev and not self._throttler.may_pass(depth_ev.symbol, tag="depth"):
            depth_ev = None
        if book_ev or depth_ev:
            self._cb.handle(book=book_ev, depth=depth_ev)

    def _raw_to_events(self, data: dict[str, Any]) -> tuple[BookTicker | None, BookDepth | None]:
        s = data.get("s")
        ticker = self._cached_tickers_dict.get(s) if s else None
        if not ticker:
            return None, None
        bids = data.get("b", [])
        asks = data.get("a", [])
        u = data.get("u")
        utc = _utc_now_float()
        book = None
        if bids and asks:
            b0, b1 = bids[0]
            a0, a1 = asks[0]
            book = BookTicker(
                symbol=ticker.symbol,
                bid_price=float(b0),
                bid_qty=float(b1),
                ask_price=float(a0),
                ask_qty=float(a1),
                last_update_id=u,
                utc=utc,
            )
        depth_bids = [BidAsk(price=float(p), quantity=float(q)) for p, q in bids]
        depth_asks = [BidAsk(price=float(p), quantity=float(q)) for p, q in asks]
        depth = BookDepth(
            symbol=ticker.symbol,
            exchange_symbol=ticker.exchange_symbol or "",
            bids=depth_bids,
            asks=depth_asks,
            last_update_id=u,
            utc=utc,
        )
        return book, depth

    @staticmethod
    def _parse_candle(symbol: str, row: list[Any]) -> CandleStick:
        parts = symbol.split("/")
        quote = parts[1] if len(parts) == 2 else ""
        quote_vol = float(row[6])
        usd_vol = quote_vol if quote in PERPETUAL_TOKENS else None
        return CandleStick(
            utc_open_time=float(row[0]) / 1000,
            open_price=float(row[1]),
            high_price=float(row[2]),
            low_price=float(row[3]),
            close_price=float(row[4]),
            coin_volume=float(row[5]),
            usd_volume=usd_vol,
        )
