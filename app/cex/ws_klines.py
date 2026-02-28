"""WebSocket kline support: list of CEX where WS klines are impossible or not implemented."""

# (exchange_id, kind) for connectors that do NOT support klines over WebSocket.
# - mexc/spot: no WS in this app (spot is REST-only).
# - mexc/perpetual: WS uses protobuf; kline stream not implemented.
WS_KLINE_UNSUPPORTED: list[tuple[str, str]] = [
    ("mexc", "spot"),
    ("mexc", "perpetual"),
]
