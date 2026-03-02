"""Тесты логики окон краулера perpetual: ключ Redis выставляется только после успешного ответа коннектора."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.cex.base import BaseCEXPerpetualConnector
from app.cex.dto import (
    BidAsk,
    BookDepth,
    CurrencyPair,
    FundingRate,
    FundingRatePoint,
    PerpetualTicker,
)
from app.services.crawlers.perpetual import CEXPerpetualCrawler


CRAWLER_TEST_PREFIX = "test:crawler:"


class FakePerpetualConnector(BaseCEXPerpetualConnector):
    """Фейковый коннектор с настраиваемыми возвратами для проверки логики окон."""

    def __init__(
        self,
        *,
        get_funding_rate_return: FundingRate | None = None,
        get_funding_rate_history_return: list[FundingRatePoint] | None = None,
        get_depth_return: BookDepth | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._fr_return = get_funding_rate_return
        self._hist_return = get_funding_rate_history_return
        self._depth_return = get_depth_return

    @classmethod
    def exchange_id(cls) -> str:
        return "fake_test"

    def start(self, cb, symbols=None, depth=True) -> None:
        pass

    def stop(self) -> None:
        pass

    def get_all_perpetuals(self) -> list[PerpetualTicker]:
        return []

    def get_price(self, symbol: str) -> CurrencyPair | None:
        return None

    def get_pairs(self, symbols: list[str] | None = None) -> list[CurrencyPair]:
        return []

    def get_depth(self, symbol: str, limit: int = 100) -> BookDepth | None:
        return self._depth_return

    def get_klines(self, symbol: str, limit: int | None = None) -> list:
        return []

    def get_funding_rate(self, symbol: str) -> FundingRate | None:
        return self._fr_return

    def get_funding_rate_history(
        self, symbol: str, limit: int | None = None
    ) -> list[FundingRatePoint] | None:
        return self._hist_return


def _sample_funding_rate(symbol: str = "BTC/USDT") -> FundingRate:
    return FundingRate(
        symbol=symbol,
        rate=0.0001,
        next_funding_utc=1000.0,
        next_rate=0.0001,
        utc=500.0,
    )


def _sample_funding_history() -> list[FundingRatePoint]:
    return [
        FundingRatePoint(funding_time_utc=900.0, rate=0.0001),
        FundingRatePoint(funding_time_utc=1000.0, rate=0.0002),
    ]


def _sample_book_depth(symbol: str = "BTC/USDT") -> BookDepth:
    return BookDepth(
        symbol=symbol,
        bids=[BidAsk(price=99.0, quantity=1.0), BidAsk(price=98.0, quantity=2.0)],
        asks=[BidAsk(price=101.0, quantity=1.0), BidAsk(price=102.0, quantity=2.0)],
        utc=1000.0,
    )


@pytest.fixture
def crawler(redis_client):
    """Краулер с тестовым префиксом ключей Redis и фейковым коннектором."""
    class FakeUoW:
        db = None
        redis = None
        log = logging.getLogger("test_crawler")

    # exchange_id может быть любым — коннектор подменим
    c = CEXPerpetualCrawler(
        FakeUoW(),
        "fake_test",
        redis_key_prefix=CRAWLER_TEST_PREFIX,
    )
    # Подменяем коннектор фейком (фейк регистрируется в Registry при определении класса — снимаем после теста)
    c._connector = FakePerpetualConnector(log=c.log)
    yield c
    # Снять фейк из Registry и очистить ключи с тестовым префиксом
    if "fake_test" in BaseCEXPerpetualConnector.Registry:
        del BaseCEXPerpetualConnector.Registry["fake_test"]
    keys = redis_client.keys(f"{CRAWLER_TEST_PREFIX}*")
    if keys:
        redis_client.delete(*keys)


@pytest.fixture
def mock_iteration():
    """Итерация с минимальными полями для _run_once_impl."""
    now = datetime.now(timezone.utc)
    it = SimpleNamespace(
        id=1,
        symbol="BTC/USDT",
        token="BTC",
        currency_pair={"ratio": 50000.0},
        funding_rate=None,
        next_funding_rate=None,
        funding_rate_history=None,
        book_depth=None,
        status="pending",
        inactive_till_timestamp=None,
        comment=None,
        last_update=None,
    )
    return it


@pytest.fixture
def mock_db():
    """Фейковая DB-сессия (flush — no-op)."""
    db = MagicMock()
    db.flush = MagicMock()
    return db


@pytest.fixture
def config():
    return CEXPerpetualCrawler.Config(
        funding_rate_window_min=15,
        funding_history_window_min=60,
        liquidity_book_window_min=30,
        liquidity_book_depth_factor=5,
        liquidity_book_amount_factor=1000,
    )


def test_window_funding_rate_key_set_only_on_success(
    crawler, redis_client, mock_iteration, mock_db, config
):
    """Ключ funding_rate выставляется только когда get_funding_rate вернул не None."""
    now_utc = datetime.now(timezone.utc)
    key_fr = crawler._redis_window_key("funding_rate", "BTC/USDT")

    # Коннектор возвращает None — ключ не должен появиться
    crawler._connector._fr_return = None
    crawler._run_once_impl(mock_iteration, now_utc, mock_db, redis_client, config)
    assert redis_client.get(key_fr) is None
    assert mock_iteration.funding_rate is None

    # Коннектор возвращает данные — ключ выставляется, данные пишутся в итерацию
    crawler._connector._fr_return = _sample_funding_rate()
    crawler._run_once_impl(mock_iteration, now_utc, mock_db, redis_client, config)
    assert redis_client.get(key_fr) == crawler._WINDOW_KEY_MAGIC.encode()
    assert mock_iteration.funding_rate is not None
    assert mock_iteration.funding_rate.get("rate") == 0.0001


def test_window_funding_history_key_set_only_on_success(
    crawler, redis_client, mock_iteration, mock_db, config
):
    """Ключ funding_history выставляется только когда get_funding_rate_history вернул не None."""
    now_utc = datetime.now(timezone.utc)
    key_hist = crawler._redis_window_key("funding_history", "BTC/USDT")

    crawler._connector._hist_return = None
    crawler._run_once_impl(mock_iteration, now_utc, mock_db, redis_client, config)
    assert redis_client.get(key_hist) is None
    assert mock_iteration.funding_rate_history is None

    crawler._connector._hist_return = _sample_funding_history()
    crawler._run_once_impl(mock_iteration, now_utc, mock_db, redis_client, config)
    assert redis_client.get(key_hist) == crawler._WINDOW_KEY_MAGIC.encode()
    assert mock_iteration.funding_rate_history is not None
    assert len(mock_iteration.funding_rate_history) == 2


def test_window_book_depth_key_set_only_on_success(
    crawler, redis_client, mock_iteration, mock_db, config
):
    """Ключ book_depth выставляется только когда get_depth вернул не None."""
    now_utc = datetime.now(timezone.utc)
    key_book = crawler._redis_window_key("book_depth", "BTC/USDT")

    crawler._connector._depth_return = None
    crawler._run_once_impl(mock_iteration, now_utc, mock_db, redis_client, config)
    assert redis_client.get(key_book) is None
    assert mock_iteration.book_depth is None

    crawler._connector._depth_return = _sample_book_depth()
    crawler._run_once_impl(mock_iteration, now_utc, mock_db, redis_client, config)
    assert redis_client.get(key_book) == crawler._WINDOW_KEY_MAGIC.encode()
    assert mock_iteration.book_depth is not None
    assert "bids" in mock_iteration.book_depth and "asks" in mock_iteration.book_depth


def test_window_fetch_allowed_blocks_after_success(
    crawler, redis_client, mock_iteration, mock_db, config
):
    """После успешного ответа ключ блокирует повторный запрос (fetch не разрешён)."""
    now_utc = datetime.now(timezone.utc)
    key_fr = crawler._redis_window_key("funding_rate", "BTC/USDT")

    crawler._connector._fr_return = _sample_funding_rate()
    crawler._run_once_impl(mock_iteration, now_utc, mock_db, redis_client, config)
    assert redis_client.get(key_fr) is not None

    # Меняем коннектор на None — но ключ уже стоит, запрос не должен выполняться
    crawler._connector._fr_return = None
    mock_iteration.funding_rate = None
    crawler._run_once_impl(mock_iteration, now_utc, mock_db, redis_client, config)
    # Итерация не получила данные (коннектор не вызывался для fr, т.к. окно занято)
    assert mock_iteration.funding_rate is None
