"""Unit of Work: async DB session + async Redis для передачи в сервисы."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis.asyncio import Redis as AsyncRedis
    from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True)
class UnitOfWork:
    """Единица работы: сессия БД и клиент Redis (async)."""

    db: AsyncSession
    redis: AsyncRedis
