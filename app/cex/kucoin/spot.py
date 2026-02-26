"""KuCoin spot connector (REST + WebSocket)."""

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

KUCOIN_SPOT_API = "https://api.kucoin.com"
KUCOIN_SPOT_WS = "wss://ws-api-spot.kucoin.com"
KUCOIN_SPOT_WS_PUSH = "wss://x-push-spot.kucoin.com"
KLINE_SIZE = 60
QUOTES = ("USDT", "USDC", "BTC", "ETH")


def _utc_now_float() -> float:
    return time.time()


def _kucoin_to_symbol(ex: str) -> str:
    """BTC-USDT -> BTC/USDT."""
    return ex.replace("-", "/", 1)


def _symbol_to_kucoin(symbol: str) -> str:
    """BTC/USDT -> BTC-USDT."""
    return symbol.replace("/", "-")


def _build_tickers_dict(tickers: list[Ticker]) -> dict[str, Ticker]:
    out: dict[str, Ticker] = {}
    for t in tickers:
        out[t.symbol] = t
        if t.exchange_symbol:
            out[t.exchange_symbol] = t
            out[t.exchange_symbol.upper()] = t
        out[t.symbol.replace("/", "")] = t
        out[t.symbol.replace("/", "-")] = t
    return out


class KucoinSpotConnector(BaseCEXSpotConnector):
    def __init__(self, is_testing: bool = False, throttle_timeout: float = 1.0) -> None:
        super().__init__(is_testing=is_testing, throttle_timeout=throttle_timeout)
        self._cached_tickers: list[Ticker] | None = None
        self._cached_tickers_dict: dict[str, Ticker] = {}
        self._ws: websocket.WebSocketApp | None = None
        self._ws_thread: threading.Thread | None = None
        self._cb: Callback | None = None
        self._pending_syms: list[str] = []
        self._pending_depth: bool = True

    @classmethod
    def exchange_id(cls) -> str:
        return "kucoin"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = KUCOIN_SPOT_API + path
        r = requests.get(url, params=params or {}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "200000":
            raise RuntimeError(data.get("msg", data.get("data", "KuCoin API error")))
        return data.get("data")

    def _get_ws_endpoint(self) -> str:
        """Get WebSocket URL with token (bullet-public)."""
        r = requests.post(KUCOIN_SPOT_API + "/api/v1/bullet-public", timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "200000":
            raise RuntimeError(data.get("msg", "KuCoin bullet-public failed"))
        body = data.get("data", {})
        token = body.get("token")
        servers = body.get("instanceServers", [])
        if not token or not servers:
            raise RuntimeError("KuCoin bullet-public: missing token or instanceServers")
        endpoint = servers[0].get("endpoint", "").rstrip("/")
        return f"{endpoint}?token={token}&connectId=cex-spot-{int(time.time() * 1000)}"

    def _send_pending_subscribes(self) -> None:
        if not self._ws or not self._ws.sock or not self._ws.sock.connected:
            return
        for i, ex_sym in enumerate(self._pending_syms):
            self._ws.send(
                json.dumps(
                    {
                        "id": f"ticker-{int(time.time() * 1000)}-{i}",
                        "type": "subscribe",
                        "topic": f"/market/ticker:{ex_sym}",
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
                            "topic": f"/market/level2Depth20:{ex_sym}",
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
        self._pending_syms = list(syms)
        self._pending_depth = depth
        ws_url = self._get_ws_endpoint()
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
            self._pending_syms = []
            raise RuntimeError("KuCoin spot WebSocket connection failed.")

    def stop(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._ws_thread = None
        self._cb = None
        self._pending_syms = []

    def get_all_tickers(self) -> list[Ticker]:
        if self._cached_tickers is not None:
            return self._cached_tickers
        data = self._get("/api/v2/symbols")
        if not isinstance(data, list):
            raise RuntimeError("Failed to get KuCoin symbols")
        tickers: list[Ticker] = []
        for item in data:
            if not item.get("enableTrading", True):
                continue
            sym = item.get("symbol", "")
            base = item.get("baseCurrency", "")
            quote = item.get("quoteCurrency", "")
            if not sym or not base or not quote:
                continue
            tickers.append(
                Ticker(
                    symbol=_kucoin_to_symbol(sym),
                    base=base,
                    quote=quote,
                    is_spot_enabled=True,
                    is_margin_enabled=bool(item.get("isMarginEnabled")),
                    exchange_symbol=sym,
                )
            )
        self._cached_tickers = tickers
        self._cached_tickers_dict = _build_tickers_dict(tickers)
        return self._cached_tickers

    def get_price(self, pair_code: str) -> CurrencyPair | None:
        ex_sym = self._exchange_symbol(pair_code) or _symbol_to_kucoin(pair_code)
        if not ex_sym:
            return None
        try:
            data = self._get("/api/v1/market/orderbook/level1", {"symbol": ex_sym})
        except Exception:
            return None
        if not data or not isinstance(data, dict):
            return None
        best_ask = data.get("bestAsk") or data.get("best_ask")
        best_bid = data.get("bestBid") or data.get("best_bid")
        price = best_ask or best_bid
        if price is None:
            return None
        ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(pair_code)
        if not ticker:
            base, quote = pair_code.split("/") if "/" in pair_code else (ex_sym.split("-")[0], ex_sym.split("-")[1] if "-" in ex_sym else "USDT")
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
            data = self._get("/api/v1/market/allTickers")
        except Exception:
            return []
        if not data or "ticker" not in data:
            ticker_list = data if isinstance(data, list) else []
        else:
            ticker_list = data.get("ticker", []) if isinstance(data, dict) else []
        if not isinstance(ticker_list, list):
            ticker_list = []
        result: list[CurrencyPair] = []
        want = None if symbols is None else {self._exchange_symbol(s) or _symbol_to_kucoin(s) for s in symbols}
        for row in ticker_list:
            if not isinstance(row, dict):
                continue
            ex_sym = row.get("symbol", "")
            if want is not None and ex_sym not in want:
                sym_norm = _kucoin_to_symbol(ex_sym)
                if sym_norm not in (symbols or []):
                    continue
            ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(_kucoin_to_symbol(ex_sym))
            if not ticker:
                continue
            last = row.get("last") or row.get("price")
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
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_kucoin(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get(
                "/api/v1/market/orderbook/level2_20",
                {"symbol": ex_sym},
            )
        except Exception:
            return None
        if not data or not isinstance(data, dict):
            return None
        bids = [[str(b[0]), str(b[1])] for b in data.get("bids", [])]
        asks = [[str(a[0]), str(a[1])] for a in data.get("asks", [])]
        if not bids and not asks:
            seq = data.get("sequence")
            bids = [[str(b[0]), str(b[1])] for b in data.get("data", {}).get("bids", [])]
            asks = [[str(a[0]), str(a[1])] for a in data.get("data", {}).get("asks", [])]
            if not bids and not asks:
                return None
        ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(symbol)
        sym = ticker.symbol if ticker else symbol
        return BookDepth(
            symbol=sym,
            exchange_symbol=ex_sym,
            bids=[BidAsk(price=float(p), quantity=float(q)) for p, q in bids[:limit]],
            asks=[BidAsk(price=float(p), quantity=float(q)) for p, q in asks[:limit]],
            last_update_id=data.get("sequence"),
            utc=_utc_now_float(),
        )

    def get_klines(self, symbol: str) -> list[CandleStick] | None:
        ex_sym = self._exchange_symbol(symbol) or _symbol_to_kucoin(symbol)
        if not ex_sym:
            return None
        try:
            data = self._get(
                "/api/v1/market/candles",
                {"symbol": ex_sym, "type": "1min"},
            )
        except Exception:
            return None
        if not isinstance(data, list):
            return None
        ticker = self._cached_tickers_dict.get(ex_sym) or self._cached_tickers_dict.get(symbol)
        quote = ticker.quote if ticker else ""
        usd_vol = quote in QUOTES
        result: list[CandleStick] = []
        for row in data[:KLINE_SIZE]:
            if not isinstance(row, list) or len(row) < 6:
                continue
            ts, o, c, h, l, vol = row[0], row[1], row[2], row[3], row[4], row[5]
            result.append(
                CandleStick(
                    utc_open_time=float(ts),
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
        if msg.get("type") == "welcome":
            self._send_pending_subscribes()
            return
        if msg.get("type") == "ack":
            return
        topic = msg.get("topic", "")
        data = msg.get("data", msg)
        if not isinstance(data, dict):
            return
        if "ticker" in topic:
            ex_sym = topic.split(":")[-1] if ":" in topic else ""
            if not ex_sym and topic == "/market/ticker:all":
                ex_sym = (msg.get("subject") or "").replace("ticker.", "")
            ex_sym = ex_sym or data.get("symbol", "")
            ticker = self._cached_tickers_dict.get(ex_sym)
            if not ticker:
                return
            if not self._throttler.may_pass(ticker.symbol, tag="book"):
                return
            best_bid = data.get("bestBid") or data.get("best_bid", "0")
            best_ask = data.get("bestAsk") or data.get("best_ask", "0")
            bid_sz = data.get("bestBidSize") or data.get("best_bid_size", "0")
            ask_sz = data.get("bestAskSize") or data.get("best_ask_size", "0")
            self._cb.handle(
                book=BookTicker(
                    symbol=ticker.symbol,
                    bid_price=float(best_bid),
                    bid_qty=float(bid_sz),
                    ask_price=float(best_ask),
                    ask_qty=float(ask_sz),
                    last_update_id=data.get("sequence"),
                    utc=float(t) / 1000.0 if (t := data.get("Time") or data.get("time")) else None,
                )
            )
        elif "level2Depth20" in topic or "level2" in topic:
            ex_sym = topic.split(":")[-1] if ":" in topic else data.get("symbol", "")
            ticker = self._cached_tickers_dict.get(ex_sym)
            if not ticker:
                return
            if not self._throttler.may_pass(ticker.symbol, tag="depth"):
                return
            bids = [[float(b[0]), float(b[1])] for b in data.get("bids", [])]
            asks = [[float(a[0]), float(a[1])] for a in data.get("asks", [])]
            self._cb.handle(
                depth=BookDepth(
                    symbol=ticker.symbol,
                    exchange_symbol=ex_sym,
                    bids=[BidAsk(price=p, quantity=q) for p, q in bids],
                    asks=[BidAsk(price=p, quantity=q) for p, q in asks],
                    last_update_id=data.get("sequence"),
                    utc=float(t) / 1000.0 if (t := data.get("Time") or data.get("time")) else None,
                )
            )
