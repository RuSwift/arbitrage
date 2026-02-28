"""JWT auth: login, logout, revocation. Роль в токене (root), expiration 1ч."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel, Field

from app.settings import Settings
from app.web.dependencies import COOKIE_ACCESS_TOKEN, _get_token_from_request, get_async_redis, security_bearer
from app.web.services.token_service import (
    ACCESS_TOKEN_EXPIRE_SECONDS,
    create_token,
    decode_token,
    revoke_jti_async,
)

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    login: str = Field(..., description="Логин")
    password: str = Field(..., description="Пароль")


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int = Field(..., description="Секунды до истечения")


@router.post("/login")
async def login(body: LoginRequest) -> JSONResponse:
    """Выдать JWT по логину и паролю (RootSettings). Устанавливает cookie access_token для браузера."""
    settings = Settings()
    if body.login != settings.root.login:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid login or password",
        )
    if body.password != settings.root.password.get_secret_value():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid login or password",
        )
    token, _jti, expires_in = create_token(login=body.login, role="root")
    response = JSONResponse(
        content={
            "access_token": token,
            "token_type": "bearer",
            "expires_in": expires_in,
        }
    )
    response.set_cookie(
        key=COOKIE_ACCESS_TOKEN,
        value=token,
        max_age=ACCESS_TOKEN_EXPIRE_SECONDS,
        httponly=True,
        samesite="lax",
        path="/",
    )
    return response


@router.post("/logout")
async def logout(
    credentials: HTTPAuthorizationCredentials | None = Depends(security_bearer),
    redis=Depends(get_async_redis),
) -> dict[str, str]:
    """Отозвать текущий токен (revocation)."""
    if not credentials or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )
    payload = decode_token(credentials.credentials)
    jti = payload.get("jti")
    exp = payload.get("exp")
    if not jti:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )
    now_ts = int(datetime.now(timezone.utc).timestamp())
    ttl = max(0, exp - now_ts) if exp else ACCESS_TOKEN_EXPIRE_SECONDS
    await revoke_jti_async(redis, jti, ttl)
    return {"status": "ok", "message": "Logged out"}


@router.post("/revoke", response_model=dict[str, str])
async def revoke(
    credentials: HTTPAuthorizationCredentials | None = Depends(security_bearer),
    redis=Depends(get_async_redis),
) -> dict[str, str]:
    """Отозвать текущий токен (то же, что logout)."""
    return await logout(credentials, redis)


@router.get("/logout")
async def logout_get(
    request: Request,
    redis=Depends(get_async_redis),
):
    """Выход для браузера: отзыв токена из cookie, удаление cookie, редирект на /login."""
    token = _get_token_from_request(request)
    if token:
        try:
            payload = decode_token(token)
            jti = payload.get("jti")
            exp = payload.get("exp")
            if jti:
                now_ts = int(datetime.now(timezone.utc).timestamp())
                ttl = max(0, exp - now_ts) if exp else ACCESS_TOKEN_EXPIRE_SECONDS
                await revoke_jti_async(redis, jti, ttl)
        except Exception:
            pass
    resp = RedirectResponse(url="/login", status_code=302)
    resp.delete_cookie(COOKIE_ACCESS_TOKEN, path="/")
    return resp
