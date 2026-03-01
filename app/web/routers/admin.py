"""Admin API: CrawlerJob and CrawlerIteration analysis."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import CrawlerIteration, CrawlerJob, Token
from app.web.dependencies import get_async_db, get_current_admin

router = APIRouter(prefix="/api/admin", tags=["admin"], dependencies=[Depends(get_current_admin)])


def _serialize_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _job_to_dict(job: CrawlerJob, iterations_count: int | None = None) -> dict[str, Any]:
    d: dict[str, Any] = {
        "id": job.id,
        "exchange": job.exchange,
        "connector": job.connector,
        "start": _serialize_dt(job.start),
        "stop": _serialize_dt(job.stop),
        "error": job.error,
    }
    if iterations_count is not None:
        d["iterations_count"] = iterations_count
    return d


def _iteration_to_dict(it: CrawlerIteration) -> dict[str, Any]:
    return {
        "id": it.id,
        "crawler_job_id": it.crawler_job_id,
        "token": it.token,
        "start": _serialize_dt(it.start),
        "stop": _serialize_dt(it.stop),
        "done": it.done,
        "status": it.status,
        "comment": it.comment,
        "error": it.error,
        "last_update": _serialize_dt(it.last_update),
        "currency_pair": it.currency_pair,
        "book_depth": it.book_depth,
        "klines": it.klines,
        "funding_rate": it.funding_rate,
        "next_funding_rate": it.next_funding_rate,
        "funding_rate_history": it.funding_rate_history,
    }


@router.get("/crawler/jobs")
async def list_crawler_jobs(
    db: AsyncSession = Depends(get_async_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    exchange: str | None = Query(None),
    connector: str | None = Query(None),
):
    """List CrawlerJob with optional filters and pagination."""
    base = select(CrawlerJob)
    if exchange:
        base = base.where(CrawlerJob.exchange == exchange)
    if connector:
        base = base.where(CrawlerJob.connector == connector)

    total_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = total_result.scalar() or 0

    stmt = (
        base.order_by(CrawlerJob.start.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(stmt)
    jobs = result.scalars().all()

    job_ids = [j.id for j in jobs]
    count_map: dict[int, int] = {}
    if job_ids:
        counts_stmt = (
            select(CrawlerIteration.crawler_job_id, func.count(CrawlerIteration.id))
            .where(CrawlerIteration.crawler_job_id.in_(job_ids))
            .group_by(CrawlerIteration.crawler_job_id)
        )
        counts_result = await db.execute(counts_stmt)
        count_map = dict(counts_result.all())

    return {
        "jobs": [_job_to_dict(j, iterations_count=count_map.get(j.id, 0)) for j in jobs],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/crawler/jobs/{job_id}")
async def get_crawler_job(
    job_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    """Get one CrawlerJob by id."""
    result = await db.execute(select(CrawlerJob).where(CrawlerJob.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="CrawlerJob not found")
    count_result = await db.execute(
        select(func.count(CrawlerIteration.id)).where(CrawlerIteration.crawler_job_id == job_id)
    )
    count = count_result.scalar() or 0
    return _job_to_dict(job, iterations_count=count)


@router.get("/crawler/jobs/{job_id}/iterations")
async def list_job_iterations(
    job_id: int,
    db: AsyncSession = Depends(get_async_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: str | None = Query(None),
):
    """List CrawlerIteration for a job with optional status filter."""
    base = select(CrawlerIteration).where(CrawlerIteration.crawler_job_id == job_id)
    if status:
        base = base.where(CrawlerIteration.status == status)

    total_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = total_result.scalar() or 0

    stmt = base.order_by(CrawlerIteration.id).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(stmt)
    iterations = result.scalars().all()

    return {
        "iterations": [_iteration_to_dict(it) for it in iterations],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/crawler/iterations/{iteration_id}")
async def get_iteration(
    iteration_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    """Get one CrawlerIteration by id."""
    result = await db.execute(select(CrawlerIteration).where(CrawlerIteration.id == iteration_id))
    it = result.scalar_one_or_none()
    if not it:
        raise HTTPException(status_code=404, detail="CrawlerIteration not found")
    return _iteration_to_dict(it)


@router.get("/crawler/stats")
async def crawler_stats(
    db: AsyncSession = Depends(get_async_db),
):
    """Summary stats: total jobs, by exchange/connector, last job."""
    total_jobs_result = await db.execute(select(func.count(CrawlerJob.id)))
    total_jobs = total_jobs_result.scalar() or 0
    total_iterations_result = await db.execute(select(func.count(CrawlerIteration.id)))
    total_iterations = total_iterations_result.scalar() or 0

    by_exchange_result = await db.execute(
        select(CrawlerJob.exchange, func.count(CrawlerJob.id)).group_by(CrawlerJob.exchange)
    )
    by_exchange = by_exchange_result.all()
    by_connector_result = await db.execute(
        select(CrawlerJob.connector, func.count(CrawlerJob.id)).group_by(CrawlerJob.connector)
    )
    by_connector = by_connector_result.all()
    by_status_result = await db.execute(
        select(CrawlerIteration.status, func.count(CrawlerIteration.id)).group_by(
            CrawlerIteration.status
        )
    )
    by_status = by_status_result.all()

    last_job_result = await db.execute(
        select(CrawlerJob).order_by(CrawlerJob.start.desc()).limit(1)
    )
    last_job = last_job_result.scalar_one_or_none()

    return {
        "total_jobs": total_jobs,
        "total_iterations": total_iterations,
        "by_exchange": [{"exchange": e, "count": c} for e, c in by_exchange],
        "by_connector": [{"connector": c, "count": n} for c, n in by_connector],
        "by_status": [{"status": s, "count": n} for s, n in by_status],
        "last_job": _job_to_dict(last_job) if last_job else None,
    }


# --- Tokens (admin CRUD) ---

def _token_to_dict(t: Token) -> dict[str, Any]:
    return {
        "id": t.id,
        "symbol": t.symbol,
        "source": t.source,
        "is_active": t.is_active,
        "created_at": _serialize_dt(t.created_at),
        "updated_at": _serialize_dt(t.updated_at),
    }


@router.get("/tokens")
async def list_tokens(
    db: AsyncSession = Depends(get_async_db),
    symbol: str | None = Query(None),
    source: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List tokens with optional symbol and source filters."""
    base = select(Token)
    if symbol:
        base = base.where(Token.symbol.ilike(f"%{symbol}%"))
    if source:
        base = base.where(Token.source == source)

    total_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = total_result.scalar() or 0

    stmt = (
        base.order_by(Token.id)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(stmt)
    tokens = result.scalars().all()

    return {
        "tokens": [_token_to_dict(t) for t in tokens],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.post("/tokens")
async def create_token(
    body: dict[str, str],
    db: AsyncSession = Depends(get_async_db),
):
    """Create a token with source='manual'. symbol required."""
    symbol = (body.get("symbol") or "").strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")

    existing = await db.execute(
        select(Token).where(Token.symbol == symbol, Token.source == "manual")
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=400,
            detail=f"Token with symbol '{symbol}' and source 'manual' already exists",
        )

    token = Token(symbol=symbol, source="manual", is_active=True)
    db.add(token)
    await db.commit()
    await db.refresh(token)
    return _token_to_dict(token)


@router.put("/tokens/{token_id}")
async def update_token(
    token_id: int,
    body: dict[str, str],
    db: AsyncSession = Depends(get_async_db),
):
    """Update a token (only source='manual')."""
    result = await db.execute(select(Token).where(Token.id == token_id))
    token = result.scalar_one_or_none()
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")
    if token.source != "manual":
        raise HTTPException(
            status_code=403,
            detail="Only tokens with source 'manual' can be edited",
        )

    symbol = (body.get("symbol") or "").strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")

    duplicate = await db.execute(
        select(Token).where(
            Token.symbol == symbol,
            Token.source == "manual",
            Token.id != token_id,
        )
    )
    if duplicate.scalar_one_or_none():
        raise HTTPException(
            status_code=400,
            detail=f"Another token with symbol '{symbol}' and source 'manual' already exists",
        )

    token.symbol = symbol
    token.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(token)
    return _token_to_dict(token)


@router.patch("/tokens/{token_id}")
async def patch_token(
    token_id: int,
    body: dict[str, Any],
    db: AsyncSession = Depends(get_async_db),
):
    """Toggle is_active (only source='manual')."""
    result = await db.execute(select(Token).where(Token.id == token_id))
    token = result.scalar_one_or_none()
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")
    if token.source != "manual":
        raise HTTPException(
            status_code=403,
            detail="Only tokens with source 'manual' can be modified",
        )
    if "is_active" in body:
        token.is_active = bool(body["is_active"])
    token.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(token)
    return _token_to_dict(token)


@router.delete("/tokens/{token_id}")
async def delete_token(
    token_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    """Delete token (only source='manual')."""
    result = await db.execute(select(Token).where(Token.id == token_id))
    token = result.scalar_one_or_none()
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")
    if token.source != "manual":
        raise HTTPException(
            status_code=403,
            detail="Only tokens with source 'manual' can be deleted",
        )
    await db.delete(token)
    await db.commit()
    return {"ok": True}
