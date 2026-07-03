from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import JobStatus, SyncJob


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


def build_claim_job_statement(*, worker_id: str) -> Select[tuple[SyncJob]]:
    return (
        select(SyncJob)
        .where(SyncJob.status == JobStatus.PENDING)
        .where((SyncJob.next_attempt_at.is_(None)) | (SyncJob.next_attempt_at <= utcnow()))
        .order_by(SyncJob.next_attempt_at.asc().nullsfirst(), SyncJob.created_at.asc())
        .limit(1)
        .with_for_update(skip_locked=True)
    )


async def claim_next_job(session: AsyncSession, *, worker_id: str) -> SyncJob | None:
    result = await session.execute(build_claim_job_statement(worker_id=worker_id))
    job = result.scalar_one_or_none()
    if job is None:
        return None

    job.status = JobStatus.PROCESSING
    job.locked_at = utcnow()
    job.locked_by = worker_id
    job.attempt_count += 1
    return job


async def reset_stale_processing_jobs(
    session: AsyncSession,
    *,
    older_than: timedelta,
    now: datetime | None = None,
) -> int:
    current_time = now or utcnow()
    cutoff = current_time - older_than
    result = await session.execute(
        select(SyncJob).where(
            SyncJob.status == JobStatus.PROCESSING,
            SyncJob.locked_at < cutoff,
        ),
    )
    jobs = list(result.scalars())
    for job in jobs:
        job.status = JobStatus.PENDING
        job.locked_at = None
        job.locked_by = None
    return len(jobs)
