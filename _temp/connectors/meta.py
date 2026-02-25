from typing import ClassVar

from core.connectors import BaseExchangeConnector as _BaseExchangeConnector
from .binance import BinanceConnector as _BinanceConnector
from .okx import OkxConnector as _OkxConnector
from .bybit import ByBitConnector as _ByBitConnector
from .kucoin import KuCoinConnector as _KuCoinConnector
from .mexc import MexcConnector as _MexcConnector
from .gate import GateConnector as _GateConnector
from .htx import HtxConnector as _HtxConnector
from .bitget import BitgetConnector as _BitgetConnector
from .commex import CommexConnector as _CommexConnector
from .bitfinex import BitfinexConnector as _BitfinexConnector


EXCHANGE_CONNECTOR_CLASSES = [
    _BinanceConnector,
    _OkxConnector,
    _ByBitConnector,
    _KuCoinConnector,
    _MexcConnector,
    _GateConnector,
    _HtxConnector,
    _BitgetConnector,
    _CommexConnector,
    _BitfinexConnector
]


def get_connector_class(exchange_code: str) -> ClassVar[_BaseExchangeConnector]:
    filtered = [ec for ec in EXCHANGE_CONNECTOR_CLASSES if ec.exchange_id() == exchange_code]
    if filtered:
        cls = filtered[0]
        return cls
    else:
        raise RuntimeError(f'Unknown exchange code: {exchange_code}')
