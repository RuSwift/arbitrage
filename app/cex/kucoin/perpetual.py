"""KuCoin perpetual futures connector (REST + WebSocket)."""

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

KUCOIN_FUTURES_API = "https://api-futures.kucoin.com"
KUCOIN_FUTURES_WS = "wss://ws-api-futures.kucoin.com"
KUCOIN_FUTURES_WS_PUSH = "wss://x-push-futures.kucoin.com"
QUOTES = ("USDT", "USDC")


def _utc_now_float() -> float:
    return time.time()


def _kucoin_futures_to_symbol(symbol: str) -> str:
    """XBTUSDTM -> BTC/USDT. ETHUSDTM -> ETH/USDT."""
    s = symbol.upper()
    for q in QUOTES:
        if s.endswith(q + "M") or s.endswith(q):
            base = s[: -len(q) - 1] if s.endswith(q + "M") else s[: -len(q)]
            if base == "XBT":
                base = "BTC"
            return f"{base}/{q}"
    return symbol.replace("USDTM", "/USDT").replace("USDT", "/USDT")


def _symbol_to_kucoin_futures(symbol: str) -> str:
    """BTC/USDT -> XBTUSDTM (KuCoin uses XBT for BTC in futures)."""
    base, quote = symbol.split("/") if "/" in symbol else (symbol, "USDT")
    if base.upper() == "BTC":
        base = "XBT"
    return f"{base}{quote}M"


def _build_perp_dict(tickers: list[PerpetualTicker]) -> dict[str, PerpetualTicker]:
    out: dict[str, PerpetualTicker] = {}
    for t in tickers:
        out[t.symbol] = t
        out[t.exchange_symbol] = t
        out[t.symbol.replace("/", "")] = t
        out[t.exchange_symbol.upper()] = t
    return out


class KucoinPerpetualConnector(BaseCEXPerpetualConnector):
    REQUEST_TIMEOUT_SEC = 15
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
        self._pending_syms: list[str] = []
        self._pending_depth: bool = True

    @classmethod
    def exchange_id(cls) -> str:
        return "kucoin"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = KUCOIN_FUTURES_API + path
        r = self._request_limited(url, params or {}, self.REQUEST_TIMEOUT_SEC)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "200000":
            raise RuntimeError(data.get("msg", data.get("data", "KuCoin futures API error")))
        return data.get("data")

    def _get_ws_endpoint(self) -> str:
        """Get WebSocket URL with token (bullet-public)."""
        r = requests.post(KUCOIN_FUTURES_API + "/api/v1/bullet-public", timeout=self.REQUEST_TIMEOUT_SEC)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "200000":
            raise RuntimeError(data.get("msg", "KuCoin futures bullet-public failed"))
        body = data.get("data", {})
        token = body.get("token")
        servers = body.get("instanceServers", [])
        if not token or not servers:
            raise RuntimeError("KuCoin futures bullet-public: missing token or instanceServers")
        endpoint = servers[0].get("endpoint", "").rstrip("/")
        return f"{endpoint}?token={token}&connectId=cex-futures-{int(time.time() * 1000)}"

    def _send_pending_subscribes(self) -> None:
        if not self._ws or not self._ws.sock or not self._ws.sock.connected:
            return
        for i, ex_sym in enumerate(self._pending_syms):
            self._ws.send(
                json.dumps(
                    {
                        "id": f"ticker-{int(time.time() * 1000)}-{i}",
                        "type": "subscribe",
                        "topic": f"/contractMarket/tickerV2:{ex_sym}",
                        "response": True,
                    }
                )
            )
            if self._pending_depth:
                self._ws.send(
                    json.dumps(
                        {
                            "id": f"depth-{int(time.time() * 1000)}-{i}",
                            "type": "subscribe",
                            "topic": f"/contractMarket/level2Depth50:{ex_sym}",
                            "response": True,
                        }
                    )
                )

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
        if not syms:
            raise RuntimeError("No symbols to subscribe")
        self._cb = cb
        self._pending_syms = list(syms)
        self._pending_depth = depth
        ws_url = self._get_ws_endpoint()
        self._ws = websocket.WebSocketApp(ws_url, on_message=self._on_ws_message)
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
            self._pending_syms = []
            raise RuntimeError("KuCoin futures WebSocket connection failed.")

    def stop(self) -> None:
        self._cancel_subscription_timer()
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._ws_thread = None
        self._cb = None
        self._pending_syms = []

    def _resolve_tokens_to_ex_syms(self, tokens: list[str]) -> list[str]:
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        out: list[str] = []
        for t in tokens:
            ex = self._exchange_symbol(t)
            if ex:
                out.append(ex)
        return out

    def _apply_subscribe(self, tokens: list[str]) -> None:
        if not self._ws or not self._ws.sock or not self._ws.sock.connected:
            return
        depth = getattr(self, "_pending_depth", True)
        for i, ex_sym in enumerate(self._resolve_tokens_to_ex_syms(tokens)):
            self._ws.send(
                json.dumps(
                    {
                        "id": f"ticker-{int(time.time() * 1000)}-{i}",
                        "type": "subscribe",
                        "topic": f"/contractMarket/tickerV2:{ex_sym}",
                        "response": True,
                    }
                )
            )
            if depth:
                self._ws.send(
                    json.dumps(
                        {
                            "id": f"depth-{int(time.time() * 1000)}-{i}",
                            "type": "subscribe",
                            "topic": f"/contractMarket/level2Depth50:{ex_sym}",
                            "response": True,
                        }
                    )
                )

    def _apply_unsubscribe(self, tokens: list[str]) -> None:
        if not self._ws or not self._ws.sock or not self._ws.sock.connected:
            return
        depth = getattr(self, "_pending_depth", True)
        for ex_sym in self._resolve_tokens_to_ex_syms(tokens):
            self._ws.send(
                json.dumps(
                    {
                        "type": "unsubscribe",
                        "topic": f"/contractMarket/tickerV2:{ex_sym}",
                    }
                )
            )
            if depth:
                self._ws.send(
                    json.dumps(
                        {
                            "type": "unsubscribe",
                            "topic": f"/contractMarket/level2Depth50:{ex_sym}",
                        }
                    )
                )

    def get_all_perpetuals(self) -> list[PerpetualTicker]:
        if self._cached_perps is not None:
            return self._cached_perps
        data = self._get("/api/v1/contracts/active")
        if not isinstance(data, list):
            raise RuntimeError("Failed to get KuCoin futures contracts")
        perps: list[PerpetualTicker] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol", "")
            if not symbol or not symbol.endswith("M"):
                continue
            sym_norm = _kucoin_futures_to_symbol(symbol)
            base_cur = item.get("baseCurrency", item.get("base_currency", ""))
            quote_cur = item.get("quoteCurrency", item.get("quote_currency", "USDT"))
            if not base_cur or not quote_cur:
                base_cur = sym_norm.split("/")[0]
                quote_cur = sym_norm.split("/")[1] if "/" in sym_norm else "USDT"
            if base_cur == "XBT":
                base_cur = "BTC"
            settlement = quote_cur if quote_cur != "UST" else "USDT"
            perps.append(
                PerpetualTicker(
                    symbol=sym_norm,
                    base=base_cur,
                    quote=quote_cur if quote_cur != "UST" else "USDT",
                    exchange_symbol=symbol,
                    settlement=settlement,
                )
            )
        self._cached_perps = perps
        self._cached_perps_dict = _build_perp_dict(perps)
        return self._cached_perps

    def get_price(self, symbol: str) -> CurrencyPair | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_kucoin_futures(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get("/api/v1/ticker", {"symbol": ex_sym})
        except Exception:
            return None
        if not data or not isinstance(data, dict):
            return None
        price = data.get("price") or data.get("lastPrice")
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
            data = self._get("/api/v1/allTickers")
        except Exception:
            return []
        ticker_list = data if isinstance(data, list) else []
        if not ticker_list and isinstance(data, dict):
            ticker_list = data.get("ticker", data.get("data", [])) or []
        result: list[CurrencyPair] = []
        want = None if symbols is None else {self._exchange_symbol(s) or _symbol_to_kucoin_futures(s) for s in symbols}
        for row in ticker_list:
            if not isinstance(row, dict):
                continue
            ex_sym = row.get("symbol", "")
            if want is not None and ex_sym not in want:
                t = self._cached_perps_dict.get(_kucoin_futures_to_symbol(ex_sym))
                if not t or t.symbol not in (symbols or []):
                    continue
            ticker = self._cached_perps_dict.get(ex_sym)
            if not ticker:
                continue
            price = row.get("price") or row.get("lastPrice")
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
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_kucoin_futures(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get("/api/v1/level2/depth20", {"symbol": ex_sym})
        except Exception:
            return None
        if not data or not isinstance(data, dict):
            return None
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if not bids and not asks:
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        return BookDepth(
            symbol=sym,
            exchange_symbol=ex_sym,
            bids=[BidAsk(price=float(b[0]), quantity=float(b[1])) for b in bids[:limit]],
            asks=[BidAsk(price=float(a[0]), quantity=float(a[1])) for a in asks[:limit]],
            last_update_id=data.get("sequence"),
            utc=_utc_now_float(),
        )

    def get_klines(self, symbol: str, limit: int | None = None) -> list[CandleStick] | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_kucoin_futures(symbol)
        if not ex_sym:
            return None
        n = limit if limit is not None else self.KLINE_SIZE
        # KuCoin futures uses /api/v1/kline/query with startAt/endAt (ms)
        end_ms = int(_utc_now_float() * 1000)
        start_ms = end_ms - n * 60 * 1000
        try:
            data = self._get(
                "/api/v1/kline/query",
                {
                    "symbol": ex_sym,
                    "granularity": "1",
                    "startAt": str(start_ms),
                    "endAt": str(end_ms),
                },
            )
        except Exception:
            return None
        if not isinstance(data, list):
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(symbol)
        quote = ticker.quote if ticker else ""
        usd_vol = quote in QUOTES
        result: list[CandleStick] = []
        for row in data[:n]:
            if not isinstance(row, list) or len(row) < 6:
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
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        t = (
            self._cached_perps_dict.get(symbol)
            or self._cached_perps_dict.get(_symbol_to_kucoin_futures(symbol))
            or self._cached_perps_dict.get(symbol.replace("/", ""))
        )
        return t.exchange_symbol if t else None

    def get_funding_rate(self, symbol: str) -> FundingRate | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_kucoin_futures(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get(f"/api/v1/funding-rate/{ex_sym}/current")
        except Exception:
            return None
        if not data or not isinstance(data, dict):
            return None
        rate_val = data.get("value") or data.get("fundingRate")
        next_rate_raw = data.get("nextFundingRate")
        funding_time = data.get("fundingTime")
        if rate_val is None:
            return None
        ticker = self._cached_perps_dict.get(ex_sym) or self._cached_perps_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        next_utc = float(funding_time) / 1000 if funding_time is not None else 0.0
        try:
            next_rate = float(next_rate_raw) if next_rate_raw is not None else None
        except (TypeError, ValueError):
            next_rate = None
        idx_px = data.get("indexPrice")
        try:
            index_price = float(idx_px) if idx_px is not None else None
        except (TypeError, ValueError):
            index_price = None
        return FundingRate(
            symbol=sym,
            rate=float(rate_val),
            next_funding_utc=next_utc,
            next_rate=next_rate,
            index_price=index_price,
            utc=_utc_now_float(),
        )

    def get_funding_rate_history(
        self, symbol: str, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_kucoin_futures(symbol)
        if not ex_sym:
            return None
        n = limit if limit is not None else DEFAULT_FUNDING_HISTORY_LIMIT
        now_ms = int(_utc_now_float() * 1000)
        start_ms = now_ms - 31 * 24 * 3600 * 1000  # 31 days back (works for 1h and 8h intervals)
        try:
            data = self._get(
                "/api/v1/contract/funding-rates",
                {"symbol": ex_sym, "from": str(start_ms), "to": str(now_ms + 60_000)},
            )
        except Exception:
            return None
        # _get returns response["data"], which for this endpoint is the list
        lst = data if isinstance(data, list) else []
        # API returns [{ timepoint (ms), fundingRate }, ...]
        return [
            FundingRatePoint(
                funding_time_utc=float(x.get("timepoint", x.get("settleTime", x.get("ts", 0)))) / 1000,
                rate=float(x.get("fundingRate", 0)),
            )
            for x in lst[:n]
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
        if msg.get("type") == "welcome":
            self._send_pending_subscribes()
            return
        if msg.get("type") in ("ack", "pong"):
            return
        topic = msg.get("topic", "")
        data = msg.get("data", msg)
        if not isinstance(data, dict):
            return
        ex_sym = topic.split(":")[-1] if ":" in topic else data.get("symbol", "")
        ticker = self._cached_perps_dict.get(ex_sym)
        if not ticker:
            return
        if "tickerV2" in topic or "ticker" in topic:
            if not self._throttler.may_pass(ticker.symbol, tag="book"):
                return
            best_bid = data.get("bestBidPrice") or data.get("bestBid", "0")
            best_ask = data.get("bestAskPrice") or data.get("bestAsk", "0")
            bid_sz = data.get("bestBidSize") or data.get("bestBidSize", "0")
            ask_sz = data.get("bestAskSize") or data.get("bestAskSize", "0")
            self._cb.handle(
                book=BookTicker(
                    symbol=ticker.symbol,
                    bid_price=float(best_bid),
                    bid_qty=float(bid_sz),
                    ask_price=float(best_ask),
                    ask_qty=float(ask_sz),
                    last_update_id=data.get("sequence"),
                    utc=float(data.get("time", 0)) / 1e9 if data.get("time") else None,
                )
            )
        elif "level2Depth20" in topic or "level2" in topic:
            if not self._throttler.may_pass(ticker.symbol, tag="depth"):
                return
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            self._cb.handle(
                depth=BookDepth(
                    symbol=ticker.symbol,
                    exchange_symbol=ex_sym,
                    bids=[BidAsk(price=float(b[0]), quantity=float(b[1])) for b in bids],
                    asks=[BidAsk(price=float(a[0]), quantity=float(a[1])) for a in asks],
                    last_update_id=data.get("sequence"),
                    utc=float(data.get("time", 0)) / 1e9 if data.get("time") else None,
                )
            )
