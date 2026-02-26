import pytest

from connectors.kucoin import KuCoinConnector
from core.connectors import BaseExchangeConnector

from .test_connectors_common import ATestConnectorCommon


class TestKuCoinConnector(ATestConnectorCommon):

    @pytest.fixture
    def connector(self) -> KuCoinConnector:
        return KuCoinConnector()

    def test_get_withdraw_info(self, connector: BaseExchangeConnector):
        info = connector.get_withdraw_info()
        assert info is not None
        assert len(info) > 100
        withdraw_list = info["USDT"]
        assert len(withdraw_list) > 0
        withdraw_info = withdraw_list[0]
        assert withdraw_info.ex_code == 'kucoin'
        assert withdraw_info.coin == "USDT"
        assert len(withdraw_info.network_names) > 0
        assert type(withdraw_info.withdraw_enabled) is bool
        assert type(withdraw_info.deposit_enabled) is bool
