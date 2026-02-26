
import pytest

from connectors.bitget import BitgetConnector

from .test_connectors_common import ATestConnectorCommon


class TestBitgetConnector(ATestConnectorCommon):

    @pytest.fixture
    def connector(self) -> BitgetConnector:
        return BitgetConnector()
