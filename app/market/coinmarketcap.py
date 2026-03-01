"""Коннектор к CoinMarketCap API: топ токенов по капитализации."""

from __future__ import annotations

from dataclasses import dataclass

import aiohttp
import requests

from app.settings import Settings

CMC_BASE = "https://pro-api.coinmarketcap.com/v1"
REQUEST_TIMEOUT_SEC = 30
PAGE_SIZE = 100  # максимум по API за один запрос


@dataclass
class CMCListing:
    """Один токен из списка CoinMarketCap (listings/latest)."""

    symbol: str
    name: str
    cmc_rank: int
    slug: str


def _parse_listings_page(raw: list) -> list[CMCListing]:
    """Парсит массив элементов из ответа API (data) в list[CMCListing]."""
    result: list[CMCListing] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol") or ""
        name = item.get("name") or ""
        rank = item.get("cmc_rank")
        slug = item.get("slug") or ""
        if symbol and rank is not None:
            result.append(
                CMCListing(
                    symbol=str(symbol),
                    name=str(name),
                    cmc_rank=int(rank),
                    slug=str(slug),
                )
            )
    return result


class CoinMarketCapConnector:
    """
    Запрашивает топ N токенов по рыночной капитализации (по умолчанию 600).
    API ключ обязателен: задаётся через settings (env COINMARKETCAP_API_KEY).
    """

    def __init__(self) -> None:
        self._settings = Settings().coinmarketcap
        self._session = requests.Session()

    def get_top_tokens(self, limit: int = 600) -> list[CMCListing]:
        """
        Возвращает топ limit токенов по капитализации.
        """
        key_value = self._settings.api_key.get_secret_value()
        if not key_value or not key_value.strip():
            raise ValueError(
                "COINMARKETCAP_API_KEY is required and must be non-empty. "
                "Set it in .env or environment."
            )
        headers = {"X-CMC_PRO_API_KEY": key_value, "Accept": "application/json"}
        result: list[CMCListing] = []
        start = 1
        while len(result) < limit:
            to_fetch = min(PAGE_SIZE, limit - len(result))
            params = {"start": start, "limit": to_fetch}
            try:
                r = self._session.get(
                    f"{CMC_BASE}/cryptocurrency/listings/latest",
                    params=params,
                    headers=headers,
                    timeout=REQUEST_TIMEOUT_SEC,
                )
                r.raise_for_status()
                data = r.json()
            except (requests.RequestException, ValueError) as _:
                break

            raw = data.get("data")
            if not isinstance(raw, list) or not raw:
                break

            parsed = _parse_listings_page(raw)
            for p in parsed:
                result.append(p)
                if len(result) >= limit:
                    break

            if len(raw) < to_fetch:
                break
            start += len(raw)

        return result[:limit]

    async def get_top_tokens_async(self, limit: int = 600) -> list[CMCListing]:
        """
        Асинхронная версия: возвращает топ limit токенов по капитализации (aiohttp).
        """
        key_value = self._settings.api_key.get_secret_value()
        if not key_value or not key_value.strip():
            raise ValueError(
                "COINMARKETCAP_API_KEY is required and must be non-empty. "
                "Set it in .env or environment."
            )
        headers = {"X-CMC_PRO_API_KEY": key_value, "Accept": "application/json"}
        result: list[CMCListing] = []
        start = 1
        async with aiohttp.ClientSession() as session:
            while len(result) < limit:
                to_fetch = min(PAGE_SIZE, limit - len(result))
                params = {"start": start, "limit": to_fetch}
                try:
                    async with session.get(
                        f"{CMC_BASE}/cryptocurrency/listings/latest",
                        params=params,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC),
                    ) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                except (aiohttp.ClientError, ValueError):
                    break

                raw = data.get("data")
                if not isinstance(raw, list) or not raw:
                    break

                parsed = _parse_listings_page(raw)
                for p in parsed:
                    result.append(p)
                    if len(result) >= limit:
                        break

                if len(raw) < to_fetch:
                    break
                start += len(raw)

        return result[:limit]
