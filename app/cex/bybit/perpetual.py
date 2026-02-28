"""Bybit perpetual (linear) connector (REST + WebSocket)."""

from __future__ import annotations

import logging
import time
from typing import Any

from pybit.exceptions import InvalidRequestError
from pybit.unified_trading import MarketHTTP, WebSocket

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

PERPETUAL_TOKENS = ("USDT", "USDC", "DAI")


def _utc_now_float() -> float:
    return time.time()


def _build_perp_dict(tickers: list[PerpetualTicker]) -> dict[str, PerpetualTicker]:
    out: dict[str, PerpetualTicker] = {}
    for t in tickers:
        out[t.symbol] = t
        out[t.exchange_symbol] = t
        out[t.symbol.replace("/", "")] = t
    return out


class BybitPerpetualConnector(BaseCEXPerpetualConnector):
    """Bybit linear perpetual. REST + WebSocket."""

    KLINE_WINDOW_SECS = 60 * 60
    KLINE_SIZE = 60
    ORDERBOOK_BOOK_DEPTH = 1
    ORDERBOOK_DEPTH_LEVELS = 50
    INSTRUMENTS_PAGE_LIMIT = 200

    def __init__(
        self,
        is_testing: bool = False,
        throttle_timeout: float = 1.0,
        log: logging.Logger | None = None,
    ) -> None:
        super().__init__(is_testing=is_testing, throttle_timeout=throttle_timeout, log=log)
        self._cached_perps: list[PerpetualTicker] | None = None
        self._cached_perps_dict: dict[str, PerpetualTicker] = {}
        self._api = MarketHTTP(testnet=is_testing)
        self._ws: WebSocket | None = None
        self._cb: Callback | None = None

    @classmethod
    def exchange_id(cls) -> str:
        return "bybit"

    def start(
        self,
        cb: Callback,
        symbols: list[str] | None = None,
        depth: bool = True,
        klines: bool = True,
    ) -> None:
        if self._ws is not None:
            raise RuntimeError("WebSocket already active. Call stop() first.")
        self._ws = WebSocket(channel_type="linear", testnet=self._is_testing)
        if not self._ws.is_connected():
            self._ws = None
            raise RuntimeError("Bybit linear WebSocket connection failed.")
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
        self._cb = cb
        for sym in syms:
            self._ws.orderbook_stream(depth=self.ORDERBOOK_BOOK_DEPTH, symbol=sym, callback=self._on_ws_message)
            if depth:
                self._ws.orderbook_stream(depth=self.ORDERBOOK_DEPTH_LEVELS, symbol=sym, callback=self._on_ws_message)
            if klines:
                self._ws.kline_stream(1, sym, callback=self._on_ws_message)

    def stop(self) -> None:
        if self._ws is not None:
            try:
                self._ws.exit()
            except Exception as e:
                self.log.debug("stop: ws exit failed: %s", e)
            self._ws = None
        self._cb = None

    def get_all_perpetuals(self) -> list[PerpetualTicker]:
        if self._cached_perps is not None:
            return self._cached_perps
        all_perps: list[PerpetualTicker] = []
        cursor: str | None = None
        while True:
            kwargs: dict[str, Any] = {
                "category": "linear",
                "status": "Trading",
                "limit": self.INSTRUMENTS_PAGE_LIMIT,
            }
            if cursor:
                kwargs["cursor"] = cursor
            resp = self._api.get_instruments_info(**kwargs)
            if resp.get("retCode") != 0:
                raise RuntimeError(resp.get("retMsg", "Failed to get linear instruments"))
            lst = resp.get("result", {}).get("list", [])
            for item in lst:
                base = item.get("baseCoin", "")
                quote = item.get("quoteCoin", "")  # settlement, e.g. USDT
                sym = item.get("symbol", "")
                symbol_code = CurrencyPair.build_code(base=base, quote=quote)
                all_perps.append(
                    PerpetualTicker(
                        symbol=symbol_code,
                        base=base,
                        quote=quote,
                        exchange_symbol=sym,
                        settlement=quote,
                    )
                )
            cursor = resp.get("result", {}).get("nextPageCursor")
            if not cursor or not lst:
                break
        self._cached_perps = all_perps
        self._cached_perps_dict = _build_perp_dict(all_perps)
        return self._cached_perps

    def get_price(self, symbol: str) -> CurrencyPair | None:
        sym = symbol.replace("/", "")
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        ticker = self._cached_perps_dict.get(sym)
        if not ticker:
            return None
        try:
            r = self._api.get_tickers(category="linear", symbol=ticker.exchange_symbol)
        except InvalidRequestError as e:
            self.log.exception("get_price failed for %s: %s", symbol, e)
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
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        if symbols is None:
            want = set(self._cached_perps_dict.keys())
        else:
            want = {s.replace("/", "") for s in symbols}
        r = self._api.get_tickers(category="linear")
        if r.get("retCode") != 0:
            raise RuntimeError(r.get("retMsg", "Failed to get linear tickers"))
        pairs: list[CurrencyPair] = []
        for p in r.get("result", {}).get("list", []):
            s = p.get("symbol")
            if s not in want:
                continue
            t = self._cached_perps_dict.get(s)
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
        r = self._api.get_orderbook(category="linear", symbol=ex_sym, limit=limit)
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
            category="linear",
            symbol=ex_sym,
            interval="1",
            limit=n,
        )
        if r.get("retCode") != 0:
            raise RuntimeError(r.get("retMsg", "Failed to get klines"))
        items = r.get("result", {}).get("list", [])
        return [self._parse_candle(symbol, i) for i in items]

    def get_funding_rate(self, symbol: str) -> FundingRate | None:
        ex_sym = self._exchange_symbol(symbol)
        if not ex_sym:
            return None
        try:
            r = self._api.get_tickers(category="linear", symbol=ex_sym)
        except InvalidRequestError as e:
            self.log.exception("get_pairs failed: %s", e)
            return None
        if r.get("retCode") != 0:
            return None
        lst = r.get("result", {}).get("list", [])
        if not lst:
            return None
        rec = lst[0]
        funding_rate = rec.get("fundingRate")
        next_ts = rec.get("nextFundingTime")
        if funding_rate is None or (isinstance(funding_rate, str) and not funding_rate.strip()):
            return None
        try:
            rate_val = float(funding_rate)
        except (TypeError, ValueError):
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(
            symbol.replace("/", "")
        )
        sym = ticker.symbol if ticker else symbol
        next_utc = float(next_ts) / 1000 if next_ts is not None else 0.0
        try:
            index_price = float(rec["indexPrice"]) if rec.get("indexPrice") is not None else None
        except (TypeError, ValueError):
            index_price = None
        return FundingRate(
            symbol=sym,
            rate=rate_val,
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
            r = self._api.get_funding_rate_history(
                category="linear", symbol=ex_sym, limit=min(n, 200)
            )
        except (InvalidRequestError, Exception) as e:
            self.log.exception("get_klines failed for %s: %s", symbol, e)
            return None
        if r.get("retCode") != 0:
            return None
        items = r.get("result", {}).get("list", [])
        if not isinstance(items, list):
            return None
        result: list[FundingRatePoint] = []
        for x in items:
            try:
                ts = x.get("fundingRateTimestamp")
                fr = x.get("fundingRate")
                if ts is None or fr is None or (isinstance(fr, str) and not fr.strip()):
                    continue
                result.append(
                    FundingRatePoint(
                        funding_time_utc=float(ts) / 1000,
                        rate=float(fr),
                    )
                )
            except (TypeError, ValueError):
                continue
        return result if result else None

    def _exchange_symbol(self, symbol: str) -> str | None:
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        t = self._cached_perps_dict.get(symbol) or self._cached_perps_dict.get(
            symbol.replace("/", "")
        )
        return t.exchange_symbol if t else None

    def _on_ws_message(self, message: dict[str, Any]) -> None:
        if not self._cb:
            return
        topic = message.get("topic", "")
        data = message.get("data", {})
        if not data:
            return
        if topic.startswith("kline."):
            parts = topic.split(".")
            ex_sym = parts[-1] if len(parts) >= 3 else data.get("symbol", "")
            ticker = self._cached_perps_dict.get(ex_sym) if ex_sym else None
            if not ticker or not self._throttler.may_pass(ticker.symbol, tag="kline"):
                return
            self._cb.handle(
                kline=CandleStick(
                    utc_open_time=float(data.get("start", 0)) / 1000,
                    open_price=float(data.get("open", 0)),
                    high_price=float(data.get("high", 0)),
                    low_price=float(data.get("low", 0)),
                    close_price=float(data.get("close", 0)),
                    coin_volume=float(data.get("volume", 0)),
                    usd_volume=None,
                )
            )
            return
        if not topic.startswith("orderbook."):
            return
        parts = topic.split(".")
        depth_level = parts[1] if len(parts) >= 2 else ""
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
        ticker = self._cached_perps_dict.get(s) if s else None
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
            exchange_symbol=ticker.exchange_symbol,
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
