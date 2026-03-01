"""Сервис работы с таблицей Token."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel
from sqlalchemy import delete, select

from app.db.models import Token
from app.market.coinmarketcap import CoinMarketCapConnector
from app.services.base import BaseService
from app.settings import ServiceConfigRegistry

TokenSource = Literal["coinmarketcap", "manual"]

CMC_SETUP_CACHE_KEY = "tokens:cmc_setup:meta"


class TokensService(BaseService):
    """Сервис обновления и чтения токенов (таблица token)."""

    class Config(BaseModel, extra='ignore'):
        cmc_top: int = 1000
        """Количество токенов для загрузки из CoinMarketCap."""
        cmc_cache_timeout: int = 60 * 60 * 24
        """Таймаут кэша для токенов из CoinMarketCap."""

    ConfigModel: type[BaseModel] = Config

    async def upsert(self, symbol: str, source: TokenSource) -> Token:
        """Создаёт токен или обновляет updated_at при существующей паре (symbol, source). Возвращает экземпляр Token."""
        result = await self.db.execute(
            select(Token).where(Token.symbol == symbol, Token.source == source)
        )
        token = result.scalar_one_or_none()
        now = datetime.now(timezone.utc)
        if token:
            token.updated_at = now
            await self.db.flush()
            await self.db.refresh(token)
            return token
        token = Token(symbol=symbol, source=source, created_at=now, updated_at=now)
        self.db.add(token)
        await self.db.flush()
        await self.db.refresh(token)
        return token

    async def get_by_symbol_source(self, symbol: str, source: TokenSource) -> Token | None:
        """Возвращает токен по паре (symbol, source) или None."""
        result = await self.db.execute(
            select(Token).where(Token.symbol == symbol, Token.source == source)
        )
        return result.scalar_one_or_none()

    async def delete(self, symbol: str, source: TokenSource) -> bool:
        """Удаляет токен по паре (symbol, source). Возвращает True, если запись была удалена."""
        result = await self.db.execute(
            delete(Token).where(Token.symbol == symbol, Token.source == source)
        )
        return result.rowcount > 0

    async def list_all(self) -> list[Token]:
        """Возвращает все токены."""
        result = await self.db.execute(select(Token).order_by(Token.symbol, Token.source))
        return list(result.scalars().all())

    async def cmc_setup(self) -> None:
        """
        Обновляет токены с source='coinmarketcap' из CoinMarketCap API.
        Конфиг грузится из ServiceConfigRegistry. В кеш пишется мета (cmc_top).
        Если в кеше уже есть мета и cmc_top совпадает с config.cmc_top — пропуск.
        """
        config = await ServiceConfigRegistry.aget(
            self.db, "TokensService", self.__class__.Config
        )
        if config is None:
            config = self.__class__.Config()
            await ServiceConfigRegistry.aset(self.db, "TokensService", config)
        raw = await self.redis.get(CMC_SETUP_CACHE_KEY)
        if raw:
            try:
                meta = json.loads(raw)
                if isinstance(meta, dict) and meta.get("cmc_top") == config.cmc_top:
                    return
            except (json.JSONDecodeError, TypeError):
                pass
        try:
            connector = CoinMarketCapConnector()
            listings = await connector.get_top_tokens_async(limit=config.cmc_top)
        except ValueError as e:
            self.log.error("CoinMarketCap error: %s", e)
            return
        if not listings:
            self.log.error("CoinMarketCap returned empty list (check API key and plan)")
            return
        for item in listings:
            await self.upsert(item.symbol, "coinmarketcap")
        await self.redis.set(
            CMC_SETUP_CACHE_KEY,
            json.dumps({"cmc_top": config.cmc_top}),
            ex=config.cmc_cache_timeout,
        )
