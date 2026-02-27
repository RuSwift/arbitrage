"""HTX (Huobi) linear perpetual connector (REST + WebSocket)."""

from __future__ import annotations

import gzip
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

HTX_LINEAR_API = "https://api.hbdm.com"
HTX_LINEAR_WS = "wss://api.hbdm.com/linear-swap-ws"


def _utc_now_float() -> float:
    return time.time()


def _contract_to_symbol(contract_code: str) -> str:
    """BTC-USDT -> BTC/USDT."""
    return contract_code.replace("-", "/")


def _build_perp_dict(tickers: list[PerpetualTicker]) -> dict[str, PerpetualTicker]:
    out: dict[str, PerpetualTicker] = {}
    for t in tickers:
        out[t.symbol] = t
        out[t.exchange_symbol] = t
        out[t.exchange_symbol.replace("-", "/")] = t
    return out


class HtxPerpetualConnector(BaseCEXPerpetualConnector):
    REQUEST_TIMEOUT_SEC = 15
    KLINE_SIZE = 60
    WS_CONNECT_WAIT_ATTEMPTS = 10
    WS_CONNECT_WAIT_SEC = 1
    WS_CONNECT_BACKOFF_SEC = 0.5
    GET_PAIRS_MAX_CONTRACTS = 30
    def __init__(self, is_testing: bool = False, throttle_timeout: float = 1.0) -> None:
        super().__init__(is_testing=is_testing, throttle_timeout=throttle_timeout)
        self._cached_perps: list[PerpetualTicker] | None = None
        self._cached_perps_dict: dict[str, PerpetualTicker] = {}
        self._ws: websocket.WebSocketApp | None = None
        self._ws_thread: threading.Thread | None = None
        self._cb: Callback | None = None

    @classmethod
    def exchange_id(cls) -> str:
        return "htx"

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = HTX_LINEAR_API + path
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
        self._cb = cb
        self._ws = websocket.WebSocketApp(
            HTX_LINEAR_WS,
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
            raise RuntimeError("HTX linear WebSocket connection failed.")
        time.sleep(self.WS_CONNECT_BACKOFF_SEC)
        for contract in syms:
            self._ws.send(
                json.dumps(
                    {"sub": f"market.{contract}.depth.step6", "id": f"depth_{contract}"}
                )
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

    def get_all_perpetuals(self) -> list[PerpetualTicker]:
        if self._cached_perps is not None:
            return self._cached_perps
        data = self._get("/linear-swap-api/v1/swap_contract_info")
        if data.get("status") != "ok":
            raise RuntimeError(data.get("err_msg", "Failed to get contract info"))
        perps: list[PerpetualTicker] = []
        for d in data.get("data", []):
            contract_code = d.get("contract_code", "")
            if "-" not in contract_code:
                continue
            base, quote = contract_code.split("-", 1)
            symbol = _contract_to_symbol(contract_code)
            perps.append(
                PerpetualTicker(
                    symbol=symbol,
                    base=base,
                    quote=quote,
                    exchange_symbol=contract_code,
                    settlement=quote,
                )
            )
        self._cached_perps = perps
        self._cached_perps_dict = _build_perp_dict(perps)
        return self._cached_perps

    def get_price(self, symbol: str) -> CurrencyPair | None:
        contract = self._exchange_symbol(symbol)
        if not contract:
            return None
        data = self._get(
            "/linear-swap-ex/market/detail/merged",
            {"contract_code": contract},
        )
        if data.get("status") != "ok" or "tick" not in data:
            return None
        tick = data["tick"]
        base, quote = symbol.split("/")
        return CurrencyPair(
            base=base,
            quote=quote,
            ratio=float(tick.get("close", 0)),
            utc=_utc_now_float(),
        )

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        if symbols is None:
            contracts = [t.exchange_symbol for t in self._cached_perps][: self.GET_PAIRS_MAX_CONTRACTS]
        else:
            sym_set = set(symbols)
            contracts = [
                t.exchange_symbol
                for t in self._cached_perps
                if t.symbol in sym_set or t.exchange_symbol in sym_set
            ]
        if not contracts:
            return []
        # HTX batch_merged is unreliable; use single merged per contract
        pairs: list[CurrencyPair] = []
        for contract in contracts:
            data = self._get(
                "/linear-swap-ex/market/detail/merged",
                {"contract_code": contract},
            )
            if data.get("status") != "ok" or "tick" not in data:
                continue
            tick = data["tick"]
            t = self._cached_perps_dict.get(contract)
            if not t:
                continue
            close = float(tick.get("close", 0))
            pairs.append(
                CurrencyPair(
                    base=t.base, quote=t.quote, ratio=close, utc=_utc_now_float()
                )
            )
        return pairs

    def get_depth(self, symbol: str, limit: int = 100) -> BookDepth | None:
        contract = self._exchange_symbol(symbol)
        if not contract:
            return None
        data = self._get(
            "/linear-swap-ex/market/depth",
            {"contract_code": contract, "type": "step5"},
        )
        if data.get("status") != "ok" or "tick" not in data:
            raise RuntimeError(data.get("err_msg", "Failed to get depth"))
        tick = data["tick"]
        bids = [BidAsk(price=float(r[0]), quantity=float(r[1])) for r in tick["bids"]]
        asks = [BidAsk(price=float(r[0]), quantity=float(r[1])) for r in tick["asks"]]
        return BookDepth(
            symbol=symbol,
            bids=bids,
            asks=asks,
            exchange_symbol=contract,
            last_update_id=int(tick.get("id", 0)),
            utc=float(tick.get("ts", 0)) / 1000,
        )

    def get_klines(self, symbol: str) -> list[CandleStick] | None:
        contract = self._exchange_symbol(symbol)
        if not contract:
            return None
        data = self._get(
            "/linear-swap-ex/market/history/kline",
            {"contract_code": contract, "period": "1min", "size": str(self.KLINE_SIZE)},
        )
        if data.get("status") != "ok" or "data" not in data:
            raise RuntimeError(data.get("err_msg", "Failed to get klines"))
        result: list[CandleStick] = []
        for k in data["data"]:
            result.append(
                CandleStick(
                    utc_open_time=float(k["id"]),
                    open_price=float(k["open"]),
                    high_price=float(k["high"]),
                    low_price=float(k["low"]),
                    close_price=float(k["close"]),
                    coin_volume=float(k["vol"]),
                    usd_volume=float(k["vol"]) if symbol.endswith("/USDT") else None,
                )
            )
        return result

    def _exchange_symbol(self, symbol: str) -> str | None:
        if not self._cached_perps_dict:
            self.get_all_perpetuals()
        t = (
            self._cached_perps_dict.get(symbol)
            or self._cached_perps_dict.get(symbol.replace("/", "-"))
        )
        return t.exchange_symbol if t else None

    def _on_ws_message(self, _: Any, raw: bytes | str) -> None:
        if not self._cb:
            return
        if isinstance(raw, bytes):
            try:
                raw = gzip.decompress(raw).decode()
            except (OSError, ValueError):
                raw = raw.decode()
        msg = json.loads(raw)
        if "ping" in msg:
            self._ws.send(json.dumps({"pong": msg["ping"]}))
            return
        ch = msg.get("ch", "")
        if "depth" not in ch:
            return
        if "tick" not in msg:
            return
        contract = ch.split(".")[1] if "." in ch else ""
        sym = _contract_to_symbol(contract)
        tick = msg["tick"]
        if not self._throttler.may_pass(sym, tag="depth"):
            return
        bids = [BidAsk(price=float(r[0]), quantity=float(r[1])) for r in tick.get("bids", [])]
        asks = [BidAsk(price=float(r[0]), quantity=float(r[1])) for r in tick.get("asks", [])]
        depth = BookDepth(
            symbol=sym,
            bids=bids,
            asks=asks,
            exchange_symbol=contract,
            last_update_id=int(tick.get("id", 0)),
            utc=float(tick.get("ts", 0)) / 1000,
        )
        book = None
        if bids and asks:
            book = BookTicker(
                symbol=sym,
                bid_price=bids[0].price,
                bid_qty=bids[0].quantity,
                ask_price=asks[0].price,
                ask_qty=asks[0].quantity,
                last_update_id=depth.last_update_id,
                utc=depth.utc,
            )
        self._cb.handle(book=book, depth=depth)
