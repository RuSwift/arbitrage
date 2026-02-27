"""Tests for CoinMarketCap connector."""

import os

import pytest

from app.market import CMCListing, CoinMarketCapConnector


@pytest.fixture
def cmc_connector() -> CoinMarketCapConnector:
    return CoinMarketCapConnector()


@pytest.mark.timeout(5)
def test_top_tokens_include_btc_eth_xrp(cmc_connector: CoinMarketCapConnector) -> None:
    """Топ токенов по капитализации должен содержать BTC, ETH, XRP."""
    tokens = cmc_connector.get_top_tokens(limit=100)
    assert len(tokens) > 0
    symbols = {t.symbol for t in tokens}
    assert "BTC" in symbols, "BTC should be in top 100"
    assert "ETH" in symbols, "ETH should be in top 100"
    assert "XRP" in symbols, "XRP should be in top 100"


@pytest.mark.timeout(5)
def test_top_tokens_structure(cmc_connector: CoinMarketCapConnector) -> None:
    """Каждый элемент — CMCListing с symbol, name, cmc_rank, slug."""
    tokens = cmc_connector.get_top_tokens(limit=5)
    assert len(tokens) >= 1
    for t in tokens:
        assert isinstance(t, CMCListing)
        assert t.symbol
        assert t.name
        assert t.cmc_rank >= 1
        assert t.slug
