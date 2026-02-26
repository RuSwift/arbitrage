
import pytest

from connectors.commex import CommexConnector

from .test_connectors_common import ATestConnectorCommon


class TestCommexConnector(ATestConnectorCommon):

    @pytest.fixture
    def connector(self) -> CommexConnector:
        return CommexConnector()
