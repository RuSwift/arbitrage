import pytest

from connectors.bitfinex import BitfinexConnector
from core.connectors import BaseExchangeConnector

from .test_connectors_common import ATestConnectorCommon


class TestBitfinexConnector(ATestConnectorCommon):

    @pytest.fixture
    def connector(self) -> BitfinexConnector:
        return BitfinexConnector()

    @pytest.fixture
    def valid_pair_code(self) -> str:
        return 'BTC/USD'
