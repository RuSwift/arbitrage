"""Сервис выпуска и проверки JWT. Роль в токене, expiration 1ч, revocation через Redis."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt

from app.settings import Settings

ACCESS_TOKEN_EXPIRE_SECONDS = 3600  # 1 hour
REVOKED_KEY_PREFIX = "auth:revoked:"
DEFAULT_ROLE = "root"

_redis_client: Any = None


def _get_redis():
    """Ленивый Redis-клиент по Settings().redis.url (без импорта из dependencies)."""
    global _redis_client
    if _redis_client is None:
        import redis
        _redis_client = redis.from_url(Settings().redis.url)
    return _redis_client


def _secret() -> str:
    return Settings().secret.get_secret_value()


def create_token(login: str, role: str = DEFAULT_ROLE) -> tuple[str, str, int]:
    """Выпустить JWT. Возвращает (token, jti, expires_in_seconds)."""
    now = datetime.now(timezone.utc)
    exp = now + timedelta(seconds=ACCESS_TOKEN_EXPIRE_SECONDS)
    jti = str(uuid.uuid4())
    payload = {
        "sub": login,
        "role": role,
        "exp": exp,
        "iat": now,
        "jti": jti,
    }
    token = jwt.encode(
        payload,
        _secret(),
        algorithm="HS256",
    )
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token, jti, ACCESS_TOKEN_EXPIRE_SECONDS


def decode_token(token: str) -> dict[str, Any]:
    """Декодировать и проверить JWT (подпись, exp). Бросает jwt.ExpiredSignatureError, jwt.InvalidTokenError."""
    return jwt.decode(
        token,
        _secret(),
        algorithms=["HS256"],
    )


def is_revoked(jti: str) -> bool:
    """Проверить, отозван ли токен по jti."""
    r = _get_redis()
    key = f"{REVOKED_KEY_PREFIX}{jti}"
    return r.get(key) is not None


def revoke_jti(jti: str, ttl_seconds: int) -> None:
    """Пометить токен как отозванный в Redis с TTL."""
    r = _get_redis()
    key = f"{REVOKED_KEY_PREFIX}{jti}"
    r.setex(key, min(ttl_seconds, ACCESS_TOKEN_EXPIRE_SECONDS), "1")


async def is_revoked_async(redis: Any, jti: str) -> bool:
    """Проверить, отозван ли токен по jti (async Redis client)."""
    key = f"{REVOKED_KEY_PREFIX}{jti}"
    return (await redis.get(key)) is not None


async def revoke_jti_async(redis: Any, jti: str, ttl_seconds: int) -> None:
    """Пометить токен как отозванный в Redis с TTL (async Redis client)."""
    key = f"{REVOKED_KEY_PREFIX}{jti}"
    await redis.setex(key, min(ttl_seconds, ACCESS_TOKEN_EXPIRE_SECONDS), "1")
