from typing import List
from time import sleep

import pytest

from connectors.kucoin import KuCoinConnector
from core.connectors import Ticker, BookTicker, BookDepth

from .helpers import *


class ATestConnectorCommon:

    @pytest.fixture
    def valid_pair_code(self) -> str:
        return 'BTC/USDT'

    def test_get_tickers_list(self, connector: BaseExchangeConnector):
        tickers = connector.get_all_tickers()
        assert len(tickers) > 0
        ticker_under_test = tickers[0]
        common_check_ticker_obj(ticker_under_test)

    def test_get_tickers_caching(self, connector: BaseExchangeConnector):
        tickers = connector.get_all_tickers()
        for n in range(3):
            other_connector = connector.__class__()
            other_tickers = other_connector.get_all_tickers()
            assert tickers == other_tickers

    def test_get_pairs(self, connector: BaseExchangeConnector):
        pairs = connector.get_pairs()
        assert len(pairs) > 0
        first_pair = pairs[0]
        common_check_curr_pair(first_pair)

    def test_get_price(self, connector: BaseExchangeConnector, valid_pair_code: str):
        pair = connector.get_price(valid_pair_code)
        assert pair
        invalid_code = 'XXX/BTC'
        pair = connector.get_price(invalid_code)
        assert pair is None

    def test_book_events(self, connector: BaseExchangeConnector, valid_pair_code: str):
        cb = TestableCallback()
        connector.start(cb, symbols=[valid_pair_code, 'BTC/INVALID'])
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

    def test_get_depth(self, connector: BaseExchangeConnector, valid_pair_code: str):
        book = connector.get_depth(valid_pair_code)
        assert book
        assert book.bids
        assert book.asks

    def test_get_klines(self, connector: BaseExchangeConnector, valid_pair_code: str):
        klines = connector.get_klines(valid_pair_code)
        assert isinstance(klines, list)
        assert klines[0].utc_open_time > klines[-1].utc_open_time
        assert 55 < len(klines) < 65
