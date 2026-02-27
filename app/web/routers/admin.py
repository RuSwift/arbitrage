"""Admin API: CrawlerJob and CrawlerIteration analysis."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import CrawlerIteration, CrawlerJob
from app.web.dependencies import get_db

router = APIRouter(prefix="/api/admin", tags=["admin"])


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
def list_crawler_jobs(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    exchange: str | None = Query(None),
    connector: str | None = Query(None),
):
    """List CrawlerJob with optional filters and pagination."""
    q = db.query(CrawlerJob)
    if exchange:
        q = q.filter(CrawlerJob.exchange == exchange)
    if connector:
        q = q.filter(CrawlerJob.connector == connector)
    total = q.count()
    q = q.order_by(CrawlerJob.start.desc())
    jobs = q.offset((page - 1) * page_size).limit(page_size).all()

    # Count iterations per job in one go
    job_ids = [j.id for j in jobs]
    if job_ids:
        counts_q = (
            db.query(CrawlerIteration.crawler_job_id, func.count(CrawlerIteration.id))
            .filter(CrawlerIteration.crawler_job_id.in_(job_ids))
            .group_by(CrawlerIteration.crawler_job_id)
        )
        count_map = dict(counts_q.all())
    else:
        count_map = {}

    return {
        "jobs": [_job_to_dict(j, iterations_count=count_map.get(j.id, 0)) for j in jobs],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/crawler/jobs/{job_id}")
def get_crawler_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    """Get one CrawlerJob by id."""
    job = db.query(CrawlerJob).filter(CrawlerJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="CrawlerJob not found")
    count = db.query(func.count(CrawlerIteration.id)).filter(CrawlerIteration.crawler_job_id == job_id).scalar()
    return _job_to_dict(job, iterations_count=count)


@router.get("/crawler/jobs/{job_id}/iterations")
def list_job_iterations(
    job_id: int,
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: str | None = Query(None),
):
    """List CrawlerIteration for a job with optional status filter."""
    q = db.query(CrawlerIteration).filter(CrawlerIteration.crawler_job_id == job_id)
    if status:
        q = q.filter(CrawlerIteration.status == status)
    total = q.count()
    iterations = q.order_by(CrawlerIteration.id).offset((page - 1) * page_size).limit(page_size).all()
    return {
        "iterations": [_iteration_to_dict(it) for it in iterations],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/crawler/iterations/{iteration_id}")
def get_iteration(
    iteration_id: int,
    db: Session = Depends(get_db),
):
    """Get one CrawlerIteration by id."""
    it = db.query(CrawlerIteration).filter(CrawlerIteration.id == iteration_id).first()
    if not it:
        raise HTTPException(status_code=404, detail="CrawlerIteration not found")
    return _iteration_to_dict(it)


@router.get("/crawler/stats")
def crawler_stats(
    db: Session = Depends(get_db),
):
    """Summary stats: total jobs, by exchange/connector, last job."""
    total_jobs = db.query(func.count(CrawlerJob.id)).scalar() or 0
    total_iterations = db.query(func.count(CrawlerIteration.id)).scalar() or 0

    by_exchange = (
        db.query(CrawlerJob.exchange, func.count(CrawlerJob.id))
        .group_by(CrawlerJob.exchange)
        .all()
    )
    by_connector = (
        db.query(CrawlerJob.connector, func.count(CrawlerJob.id))
        .group_by(CrawlerJob.connector)
        .all()
    )
    by_status = (
        db.query(CrawlerIteration.status, func.count(CrawlerIteration.id))
        .group_by(CrawlerIteration.status)
        .all()
    )

    last_job = db.query(CrawlerJob).order_by(CrawlerJob.start.desc()).first()

    return {
        "total_jobs": total_jobs,
        "total_iterations": total_iterations,
        "by_exchange": [{"exchange": e, "count": c} for e, c in by_exchange],
        "by_connector": [{"connector": c, "count": n} for c, n in by_connector],
        "by_status": [{"status": s, "count": n} for s, n in by_status],
        "last_job": _job_to_dict(last_job) if last_job else None,
    }
