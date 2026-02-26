from time import sleep

import pytest

from connectors.okx import OkxConnector

from .helpers import *


class TestOkxConnector:

    @pytest.fixture
    def connector(self) -> OkxConnector:
        return OkxConnector()

    def test_get_tickers_list(self, connector: OkxConnector):
        tickers = connector.get_all_tickers()
        assert len(tickers) > 0
        ticker_under_test = tickers[0]
        common_check_ticker_obj(ticker_under_test)

    def test_get_tickers_caching(self, connector: OkxConnector):
        tickers = connector.get_all_tickers()
        for n in range(3):
            other_connector = OkxConnector()
            other_tickers = other_connector.get_all_tickers()
            assert tickers == other_tickers

    def test_get_pairs(self, connector: OkxConnector):
        pairs = connector.get_pairs()
        assert len(pairs) > 0
        first_pair = pairs[0]
        common_check_curr_pair(first_pair)

    def test_get_price(self, connector: OkxConnector):
        pair = connector.get_price('BTC/USDT')
        assert pair
        invalid_code = 'XXX/BTC'
        pair = connector.get_price(invalid_code)
        assert pair is None

    def test_book_events(self, connector: OkxConnector):
        cb = TestableCallback()
        connector.start(cb, symbols=['XRP/USDT', 'BTC/INVALID'])
        sleep(5)
        connector.stop()
        assert len(cb.books) > 0
        assert len(cb.depths) > 0
        # books
        first_book = cb.books[0]
        common_check_book_ticker(first_book)
        # depths
        first_depth = cb.depths[0]
        common_check_book_depth(first_depth)

    def test_get_depth(self, connector: OkxConnector):
        book = connector.get_depth('BTC/USDT')
        assert book
        assert book.bids
        assert book.asks

    def test_get_klines(self, connector: OkxConnector):
        klines = connector.get_klines('BTC/USDT')
        assert isinstance(klines, list)
        assert klines[0].utc_open_time > klines[-1].utc_open_time
        assert 55 < len(klines) < 65
