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


def _get_cmc_error_message(data: dict | None) -> str | None:
    """Извлекает текст ошибки из ответа API (status.error_message)."""
    if not data or not isinstance(data, dict):
        return None
    status = data.get("status")
    if not isinstance(status, dict):
        return None
    msg = status.get("error_message")
    return str(msg) if msg is not None else None


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
                data = r.json() if r.content else {}
                if not r.ok:
                    msg = _get_cmc_error_message(data) or r.reason or f"HTTP {r.status_code}"
                    raise ValueError(msg)
                raw = data.get("data")
                if not isinstance(raw, list) or not raw:
                    msg = _get_cmc_error_message(data)
                    if msg:
                        raise ValueError(msg)
                    break
            except requests.RequestException as e:
                if isinstance(e, requests.HTTPError) and e.response is not None:
                    try:
                        data = e.response.json()
                    except Exception:
                        data = {}
                    msg = _get_cmc_error_message(data)
                    if msg:
                        raise ValueError(msg) from e
                raise

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
                        try:
                            data = await resp.json()
                        except Exception:
                            data = {}
                        if not resp.ok:
                            msg = _get_cmc_error_message(data) or resp.reason or f"HTTP {resp.status}"
                            raise ValueError(msg)
                        raw = data.get("data")
                        if not isinstance(raw, list) or not raw:
                            msg = _get_cmc_error_message(data)
                            if msg:
                                raise ValueError(msg)
                            break
                except ValueError:
                    raise
                except aiohttp.ClientResponseError as e:
                    try:
                        data = await e.response.json() if e.response.content else {}
                    except Exception:
                        data = {}
                    msg = _get_cmc_error_message(data)
                    if msg:
                        raise ValueError(msg) from e
                    raise
                except aiohttp.ClientError:
                    raise

                parsed = _parse_listings_page(raw)
                for p in parsed:
                    result.append(p)
                    if len(result) >= limit:
                        break

                if len(raw) < to_fetch:
                    break
                start += len(raw)

        return result[:limit]
