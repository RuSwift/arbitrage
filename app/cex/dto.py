"""DTOs for CEX connectors (spot and perpetual)."""

from __future__ import annotations

import hashlib
from dataclasses import MISSING, dataclass, fields, is_dataclass
from typing import Any, get_origin


def _as_dict(obj: Any) -> Any:
    if is_dataclass(obj) and not type(obj).__name__.startswith("_"):
        return {f.name: _as_dict(getattr(obj, f.name)) for f in fields(obj)}
    if isinstance(obj, list):
        return [_as_dict(x) for x in obj]
    return obj


def _from_dict(cls: type, data: dict[str, Any]) -> Any:
    if not is_dataclass(cls):
        raise TypeError(f"{cls} is not a dataclass")
    kwargs: dict[str, Any] = {}
    for f in fields(cls):
        if f.name not in data:
            if f.default is not MISSING:
                kwargs[f.name] = f.default
            elif f.default_factory is not MISSING:
                kwargs[f.name] = f.default_factory()
            else:
                raise ValueError(f"Missing required field {f.name!r} for {cls.__name__}")
            continue
        val = data[f.name]
        if val is None:
            kwargs[f.name] = None
        elif get_origin(f.type) is list:
            args = getattr(f.type, "__args__", ())
            item_cls = args[0] if args else None
            if item_cls and is_dataclass(item_cls):
                kwargs[f.name] = [_from_dict(item_cls, x) if isinstance(x, dict) else x for x in (val or [])]
            else:
                kwargs[f.name] = val
        elif is_dataclass(f.type):
            kwargs[f.name] = _from_dict(f.type, val) if isinstance(val, dict) else val
        else:
            kwargs[f.name] = val
    return cls(**kwargs)


@dataclass
class Ticker:
    symbol: str
    base: str
    quote: str
    is_spot_enabled: bool
    is_margin_enabled: bool
    exchange_symbol: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return _as_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Ticker:
        return _from_dict(cls, data)


@dataclass
class PerpetualTicker:
    """Perpetual (linear) contract descriptor."""

    symbol: str
    base: str
    quote: str
    exchange_symbol: str
    settlement: str  # e.g. USDT

    def as_dict(self) -> dict[str, Any]:
        return _as_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PerpetualTicker:
        return _from_dict(cls, data)


@dataclass
class BookTicker:
    symbol: str
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    last_update_id: Any = None
    utc: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return _as_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BookTicker:
        return _from_dict(cls, data)


@dataclass
class BidAsk:
    price: float
    quantity: float

    def as_dict(self) -> dict[str, Any]:
        return _as_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BidAsk:
        return _from_dict(cls, data)


@dataclass
class BookDepth:
    symbol: str
    bids: list[BidAsk]
    asks: list[BidAsk]
    exchange_symbol: str | None = None
    last_update_id: Any = None
    utc: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return _as_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BookDepth:
        return _from_dict(cls, data)


@dataclass
class CandleStick:
    utc_open_time: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    coin_volume: float
    usd_volume: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return _as_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CandleStick:
        return _from_dict(cls, data)


@dataclass
class CurrencyPair:
    base: str
    quote: str
    ratio: float
    utc: float | None = None

    @property
    def code(self) -> str:
        return self.build_code(self.base, self.quote)

    @staticmethod
    def build_code(base: str, quote: str) -> str:
        return f"{base}/{quote}"

    def as_dict(self) -> dict[str, Any]:
        return _as_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CurrencyPair:
        return _from_dict(cls, data)


@dataclass
class ExCredentials:
    api_key: str
    secret_key: str
    pass_phrase: str | None = None

    @property
    def md5(self) -> str:
        s = self.api_key + self.secret_key + (self.pass_phrase or "")
        return hashlib.md5(s.encode()).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        return _as_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExCredentials:
        return _from_dict(cls, data)


@dataclass
class WithdrawInfo:
    ex_code: str
    coin: str
    network_names: list[str]
    withdraw_enabled: bool
    deposit_enabled: bool
    fixed_withdraw_fee: float | None = None
    withdraw_min: float | None = None
    withdraw_max: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return _as_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WithdrawInfo:
        return _from_dict(cls, data)


@dataclass
class FundingRate:
    """Current funding rate and next funding time for a perpetual."""

    symbol: str
    rate: float
    next_funding_utc: float
    next_rate: float | None = None
    utc: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return _as_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FundingRate:
        return _from_dict(cls, data)


@dataclass
class FundingRatePoint:
    """Single point in funding rate history (for chart)."""

    funding_time_utc: float
    rate: float

    def as_dict(self) -> dict[str, Any]:
        return _as_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FundingRatePoint:
        return _from_dict(cls, data)
