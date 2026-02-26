import datetime
from typing import List
from time import sleep

import pytest

from connectors.bybit import ByBitConnector, ByBitP2PService
from core.p2p import NO_SOCKS, TradeSide

from .helpers import *


class TestByBitConnector:

    @pytest.fixture
    def connector(self) -> ByBitConnector:
        return ByBitConnector()

    def test_get_tickers_list(self, connector: ByBitConnector):
        tickers = connector.get_all_tickers()
        assert len(tickers) > 0
        ticker_under_test = tickers[0]
        common_check_ticker_obj(ticker_under_test)

    def test_get_tickers_caching(self, connector: ByBitConnector):
        tickers = connector.get_all_tickers()
        for n in range(3):
            other_connector = ByBitConnector()
            other_tickers = other_connector.get_all_tickers()
            assert tickers == other_tickers

    def test_get_pairs(self, connector: ByBitConnector):
        pairs = connector.get_pairs()
        assert len(pairs) > 0
        first_pair = pairs[0]
        common_check_curr_pair(first_pair)

    def test_get_price(self, connector: ByBitConnector):
        pair = connector.get_price('BTC/USDT')
        assert pair
        invalid_code = 'XXX/BTC'
        pair = connector.get_price(invalid_code)
        assert pair is None

    def test_book_events(self, connector: ByBitConnector):
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

    def test_get_depth(self, connector: ByBitConnector):
        book = connector.get_depth('BTC/USDT')
        assert book
        assert book.bids
        assert book.asks

    def test_get_klines(self, connector: ByBitConnector):
        klines = connector.get_klines('BTC/USDT')
        assert isinstance(klines, list)
        assert klines[0].utc_open_time > klines[-1].utc_open_time
        assert 55 < len(klines) < 65


@pytest.mark.asyncio
class TestByBitP2P:

    @pytest.fixture
    def service(self) -> ByBitP2PService:
        ByBitP2PService._cache.REDIS_KEY_PREFIX = 'test-prefix-' + str(datetime.datetime.now())
        return ByBitP2PService(socks_addr=NO_SOCKS)

    async def test_load_fiat(self, service: ByBitP2PService):
        fiat = await service.load_fiat_list()
        assert len(fiat) > 15

    async def test_load_tokens(self, service: ByBitP2PService):
        trade_tokens = await service.load_tokens(fiat='RUB')
        assert len(trade_tokens) > 2

    async def test_load_payment_methods(self, service: ByBitP2PService):
        methods = await service.load_payment_methods(fiat='RUB')
        assert len(methods) > 2

    async def test_load_adverts(self, service: ByBitP2PService):
        # SELL
        adv_sell = await service.load_adv_page(
            side=TradeSide.SELL, fiat='RUB', token='USDT', page=1
        )
        assert len(adv_sell) > 0
        # BUY
        adv_buy = await service.load_adv_page(
            side=TradeSide.BUY, fiat='RUB', token='USDT', page=1
        )
        assert len(adv_buy) > 0

    async def test_load_adverts_pagination(self, service: ByBitP2PService):
        pg1 = await service.load_adv_page(
            side=TradeSide.SELL, fiat='RUB', token='USDT', page=1
        )
        assert len(pg1) > 0
        pg2 = await service.load_adv_page(
            side=TradeSide.SELL, fiat='RUB', token='USDT', page=2
        )
        assert len(pg2) > 0
        first_pg1 = pg1[0]
        first_pg2 = pg2[0]
        assert first_pg1.info.uid != first_pg2.info.uid
