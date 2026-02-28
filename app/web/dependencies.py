"""Dependencies for web app: DB session, Redis, current User/Admin (JWT + BasicAuth)."""

from __future__ import annotations

from collections.abc import Generator
from typing import Annotated, Any

import jwt
from fastapi import Depends, HTTPException, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBasic, HTTPBasicCredentials, HTTPBearer
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.settings import Settings
from app.web.services.token_service import (
    ACCESS_TOKEN_EXPIRE_SECONDS,
    create_token,
    decode_token,
    is_revoked,
)

_engine: Any = None
_SessionLocal: Any = None


def _get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            Settings().database.url,
            echo=Settings().database.echo,
            pool_size=Settings().database.pool_size,
            max_overflow=Settings().database.max_overflow,
        )
    return _engine


def _get_session_factory():
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=_get_engine(),
            autocommit=False,
            autoflush=False,
        )
    return _SessionLocal


def get_db() -> Generator[Session, None, None]:
    """Yield a DB session for FastAPI dependency injection."""
    SessionLocal = _get_session_factory()
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


_redis_client: Any = None


def get_redis():
    """Return a Redis client (lazy). Uses Settings().redis.url."""
    global _redis_client
    if _redis_client is None:
        import redis
        _redis_client = redis.from_url(Settings().redis.url)
    return _redis_client


# --- Current User / Admin (JWT + BasicAuth) ---

security_bearer = HTTPBearer(auto_error=False)
security_basic = HTTPBasic(auto_error=False)


class CurrentUser(BaseModel):
    """Текущий пользователь из JWT или BasicAuth (sub, role)."""

    sub: str
    role: str
    exp: int | None = None
    jti: str | None = None


def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security_bearer)],
) -> CurrentUser:
    """Зависимость: текущий пользователь из JWT (Bearer). Проверяет expiration и revocation."""
    if not credentials or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )
    try:
        payload = decode_token(credentials.credentials)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    jti = payload.get("jti")
    if not jti:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    if is_revoked(jti):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token revoked")
    return CurrentUser(
        sub=payload["sub"],
        role=payload["role"],
        exp=payload.get("exp"),
        jti=jti,
    )


def _validate_basic(credentials: HTTPBasicCredentials) -> CurrentUser | None:
    """Проверить Basic Auth по RootSettings. При успехе вернуть CurrentUser с role=root."""
    settings = Settings()
    if credentials.username != settings.root.login:
        return None
    if credentials.password != settings.root.password.get_secret_value():
        return None
    return CurrentUser(sub=credentials.username, role="root", exp=None, jti=None)


def get_current_admin(
    request: Request,
    response: Response,
    credentials_bearer: Annotated[HTTPAuthorizationCredentials | None, Depends(security_bearer)],
    credentials_basic: Annotated[HTTPBasicCredentials | None, Depends(security_basic)],
) -> CurrentUser:
    """
    Зависимость: текущий пользователь с ролью root (админ).
    Принимает Bearer JWT, cookie access_token или Basic Auth (RootSettings). При успешной Basic Auth выдаёт JWT в заголовке X-Auth-Token.
    """
    # 1) Bearer JWT или токен из cookie
    token = None
    if credentials_bearer and credentials_bearer.credentials:
        token = credentials_bearer.credentials
    if not token:
        token = _get_token_from_request(request)
    if token:
        try:
            payload = decode_token(token)
        except jwt.ExpiredSignatureError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired"
            )
        except jwt.InvalidTokenError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
            )
        if payload.get("role") != "root":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin role required")
        jti = payload.get("jti")
        if jti and is_revoked(jti):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token revoked")
        return CurrentUser(
            sub=payload["sub"],
            role=payload["role"],
            exp=payload.get("exp"),
            jti=jti,
        )

    # 2) Basic Auth -> проверка RootSettings и выпуск JWT
    if credentials_basic:
        user = _validate_basic(credentials_basic)
        if user:
            token, _jti, _exp = create_token(login=user.sub, role="root")
            response.headers["X-Auth-Token"] = token
            response.headers["X-Auth-Token-Expires-In"] = str(ACCESS_TOKEN_EXPIRE_SECONDS)
            return user

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated (Bearer or Basic auth required)",
    )


COOKIE_ACCESS_TOKEN = "access_token"


def _get_token_from_request(request: Request) -> str | None:
    """Токен из заголовка Authorization: Bearer или из cookie."""
    auth = request.headers.get("Authorization")
    if auth and auth.startswith("Bearer "):
        return auth[7:].strip()
    return request.cookies.get(COOKIE_ACCESS_TOKEN)


def get_current_admin_from_request(request: Request) -> CurrentUser | None:
    """
    Для страниц (GET /admin): получить админа из cookie или Bearer.
    Возвращает CurrentUser с role=root или None (не авторизован / не админ).
    """
    token = _get_token_from_request(request)
    if not token:
        return None
    try:
        payload = decode_token(token)
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None
    if payload.get("role") != "root":
        return None
    jti = payload.get("jti")
    if jti and is_revoked(jti):
        return None
    return CurrentUser(
        sub=payload["sub"],
        role=payload["role"],
        exp=payload.get("exp"),
        jti=jti,
    )
