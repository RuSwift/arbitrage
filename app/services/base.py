"""Базовый сервис с внедрением Unit of Work."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

from app.services.unit_of_work import UnitOfWork

if TYPE_CHECKING:
    from redis.asyncio import Redis as AsyncRedis
    from sqlalchemy.ext.asyncio import AsyncSession


class BaseService:
    """Базовый сервис. В конструктор передаётся unit of work (async db session + async redis)."""
    
    class Config(BaseModel, allow_extra=True):
        ...
    
    ConfigModel: type[BaseModel] = Config

    Registry: list[type[BaseService]] = []

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        BaseService.Registry.append(cls)

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    @property
    def db(self) -> AsyncSession:
        """Async DB session."""
        return self._uow.db

    @property
    def redis(self) -> AsyncRedis:
        """Async Redis client."""
        return self._uow.redis
