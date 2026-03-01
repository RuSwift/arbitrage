"""Сервис работы с таблицей Token."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel
from sqlalchemy import delete, select

from app.db.models import Token
from app.services.base import BaseService

TokenSource = Literal["coinmarketcap", "manual"]


class TokensService(BaseService):
    """Сервис обновления и чтения токенов (таблица token)."""

    class Config(BaseModel, extra='ignore'):
        cmc_top: int = 500
        """Количество токенов для загрузки из CoinMarketCap."""
        cmc_cache_timeout: int = 60 * 60
        """Таймаут кэша для токенов из CoinMarketCap."""

    ConfigModel: type[BaseModel] = Config

    async def upsert(self, symbol: str, source: TokenSource) -> Token:
        """Создаёт токен или обновляет updated_at при существующей паре (symbol, source). Возвращает экземпляр Token."""
        result = await self.db.execute(
            select(Token).where(Token.symbol == symbol, Token.source == source)
        )
        token = result.scalar_one_or_none()
        if token:
            token.updated_at = datetime.now(timezone.utc)
            await self.db.flush()
            await self.db.refresh(token)
            return token
        token = Token(symbol=symbol, source=source)
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
