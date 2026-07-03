from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

from sqlalchemy import Select, select, update
from sqlalchemy.engine import CursorResult
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
    await session.flush()
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
        update(SyncJob)
        .where(
            SyncJob.status == JobStatus.PROCESSING,
            SyncJob.locked_at < cutoff,
        )
        .values(
            status=JobStatus.PENDING,
            locked_at=None,
            locked_by=None,
        )
        .execution_options(synchronize_session=False),
    )
    cursor_result = cast(CursorResult[object], result)
    return cursor_result.rowcount or 0
