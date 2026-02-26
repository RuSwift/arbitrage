import asyncio
import json
import os.path
from typing import Optional, List, Union

import pytest
import pytest_asyncio

from core.connectors import BookDepth, BookTicker
from core.repository import Repository
from core.defs import CurrencyPair
from connectors.facade import ExchangeFacade, build_book_ticker_key

from tests.helpers import DummyHelper


class TestableObserver(ExchangeFacade.Observer):

    def __init__(self):
        self.tickers = []

    async def on_ticker(self, event: BookTicker):
        self.tickers.append(event)


@pytest.mark.asyncio
class TestFacade:

    @pytest.fixture
    def dummy_dir(self) -> str:
        base_dir = os.path.realpath(os.path.dirname(__file__))
        dummy_dir = os.path.realpath(os.path.join(base_dir, '..', 'dummy'))
        return dummy_dir

    @pytest_asyncio.fixture
    async def repo(self) -> Repository:
        repo = Repository()
        await repo.flush_all()
        return repo

    @pytest.fixture
    def helper(self, repo: Repository, dummy_dir: str) -> DummyHelper:
        return DummyHelper(repo=repo, directory=dummy_dir)

    @pytest_asyncio.fixture()
    async def binance(self, helper: DummyHelper, repo: Repository) -> ExchangeFacade:
        await helper.load_dummy_data(file='binance.dump', flush_before=False)
        return ExchangeFacade(code='binance', repo=repo)

    async def test_read_pairs(self, binance: ExchangeFacade):
        # All
        pairs1 = await binance.read_pairs()
        assert len(pairs1) > 0
        assert all(isinstance(p, CurrencyPair) for p in pairs1)
        # Filtering
        pairs2 = await binance.read_pairs(base_filter='BTC')
        assert len(pairs2) < len(pairs1)

    async def test_read_book_tickers(self, binance: ExchangeFacade):
        # All
        book_tickers1 = await binance.read_book_tickers()
        assert len(book_tickers1) > 1
        # Filter
        sym1 = book_tickers1[0].symbol
        book_tickers2 = await binance.read_book_tickers(symbol=sym1)
        assert len(book_tickers2) == 1

    async def test_read_book_depths(self, binance: ExchangeFacade):
        # Many
        sym1 = 'BTC/USDT'
        sym2 = 'ETH/USDT'
        depths = await binance.read_book_depths(symbol=[sym1, sym2])
        assert len(depths) > 1
        # Filter
        depths2 = await binance.read_book_depths(symbol=sym1)
        assert len(depths2) == 1

    async def test_observer_for_tickers(self, binance: ExchangeFacade, dummy_dir: str, repo: Repository):
        observer = TestableObserver()
        with open(os.path.join(dummy_dir, 'binance.dump'), 'r') as f:
            dump = json.loads(f.read())
        book_tickers = {k: v for k, v in dump.items() if 'book_ticker' in k}

        async def __publisher():
            nonlocal book_tickers
            nonlocal repo
            while True:
                await asyncio.sleep(0.1)
                for k, v in book_tickers.items():
                    await repo.publish(k, v)

        tsk = asyncio.create_task(__publisher())
        try:
            k, v = list(book_tickers.items())[0]
            symbol = k.split('/')[-1].replace('-', '/')
            token = await binance.attach(
                observer=observer, symbol=symbol
            )
            try:
                await asyncio.sleep(1)
                assert len(observer.tickers) > 1
            finally:
                binance.detach(token)
        finally:
            tsk.cancel()

    async def test_observer_detaching_by_token(self, binance: ExchangeFacade, dummy_dir: str, repo: Repository):
        observer = TestableObserver()
        with open(os.path.join(dummy_dir, 'binance.dump'), 'r') as f:
            dump = json.loads(f.read())
        book_tickers = {k: v for k, v in dump.items() if 'book_ticker' in k}

        async def __publisher():
            nonlocal book_tickers
            nonlocal repo
            while True:
                await asyncio.sleep(0.1)
                for k, v in book_tickers.items():
                    await repo.publish(k, v)

        tsk = asyncio.create_task(__publisher())
        try:
            k, v = list(book_tickers.items())[0]
            symbol = k.split('/')[-1].replace('-', '/')
            token = await binance.attach(
                observer=observer, symbol=symbol
            )
            await asyncio.sleep(1)
            binance.detach(token)
            observer.tickers.clear()
            await asyncio.sleep(1)
            assert not observer.tickers
        finally:
            tsk.cancel()

    async def test_observer_detaching_by_ob_instance(self, binance: ExchangeFacade, dummy_dir: str, repo: Repository):
        observer = TestableObserver()
        with open(os.path.join(dummy_dir, 'binance.dump'), 'r') as f:
            dump = json.loads(f.read())
        book_tickers = {k: v for k, v in dump.items() if 'book_ticker' in k}

        async def __publisher():
            nonlocal book_tickers
            nonlocal repo
            while True:
                await asyncio.sleep(0.1)
                for k, v in book_tickers.items():
                    await repo.publish(k, v)

        tsk = asyncio.create_task(__publisher())
        try:
            k, v = list(book_tickers.items())[0]
            symbol = k.split('/')[-1].replace('-', '/')
            await binance.attach(
                observer=observer, symbol=symbol
            )
            await asyncio.sleep(1)
            binance.detach(observer)
            observer.tickers.clear()
            await asyncio.sleep(1)
            assert not observer.tickers
        finally:
            tsk.cancel()

    @pytest.mark.parametrize(
        ('symbols',),
        [
            (None,),
            (['BTC/USDT', 'ETH/USDT', 'XRP/USDT'],)
        ],
    )
    async def test_subscription_for_all_tickers(
            self, binance: ExchangeFacade, dummy_dir: str,
            repo: Repository, symbols: Optional[Union[str, List[str]]]
    ):
        """Stress-TEST"""
        if symbols is None:
            all_symbols = await binance.get_all_symbols()
        else:
            all_symbols = symbols
        tickers = await binance.read_book_tickers()
        emulate_ticker = tickers[0]
        # Эмулируем число > 500 и протестируем
        # что в observer все значения пришли
        observer = TestableObserver()
        await binance.attach(observer, symbol=symbols or 'all')
        await asyncio.sleep(0.1)
        for sym in all_symbols:
            repo_key = build_book_ticker_key(binance.code, sym)
            emulate_ticker.symbol = sym
            await repo.publish(repo_key, value=emulate_ticker.as_json())
            pass

        await asyncio.sleep(1)
        rcv_symbols = [tk.symbol for tk in observer.tickers]
        diff = set(all_symbols) - set(rcv_symbols)
        assert len(rcv_symbols) == len(all_symbols), print(diff)

    async def test_subscription_for_unknown_tickers(
            self, binance: ExchangeFacade, repo: Repository,
    ):
        observer = TestableObserver()
        unknown_symbols = ['Q1/USDT', 'Q2/USDT', 'Q3/USDT']
        tickers = await binance.read_book_tickers()
        emulate_ticker = tickers[0]

        # Эмулируем отправку ранее неизвестных токенов
        observer = TestableObserver()
        await binance.attach(observer, symbol='all')
        await asyncio.sleep(0.1)
        for sym in unknown_symbols:
            repo_key = build_book_ticker_key(binance.code, sym)
            emulate_ticker.symbol = sym
            await repo.publish(repo_key, value=emulate_ticker.as_json())
            pass

        await asyncio.sleep(1)
        rcv_symbols = [tk.symbol for tk in observer.tickers]
        assert len(rcv_symbols) == len(unknown_symbols)

    async def test_read_depth(self, binance: ExchangeFacade):
        depth = await binance.read_book_depths(symbol='BTC/USDT')
        assert depth

    async def test_read_candles(self, binance: ExchangeFacade):
        for n in range(2):
            candles = await binance.read_candles(symbol='BTC/USDT')
            assert candles

        # чек not-perpetual пары
        candles2 = await binance.read_candles(symbol='ETH/BTC')
        assert candles2[0].usd_volume is not None
