from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from app.db.models import JobStatus, JobType, SyncJob
from app.db.repositories import (
    build_claim_job_statement,
    claim_next_job,
    reset_stale_processing_jobs,
)


def _compile_statement() -> str:
    statement = build_claim_job_statement(worker_id="worker-1")
    return str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        ),
    )


def test_claim_job_statement_uses_skip_locked() -> None:
    compiled = _compile_statement()

    assert "FOR UPDATE" in compiled
    assert "SKIP LOCKED" in compiled
    assert "sync_jobs" in compiled


def test_claim_job_statement_selects_one_due_pending_job_in_fifo_order() -> None:
    compiled = _compile_statement()

    assert "sync_jobs.status = 'pending'" in compiled
    assert "sync_jobs.next_attempt_at IS NULL OR sync_jobs.next_attempt_at <=" in compiled
    assert (
        "ORDER BY sync_jobs.next_attempt_at ASC NULLS FIRST, sync_jobs.created_at ASC"
        in compiled
    )
    assert "LIMIT 1" in compiled


def test_claim_index_matches_global_claim_query_shape() -> None:
    claim_index = next(
        index for index in SyncJob.__table__.indexes if index.name == "ix_sync_jobs_claim"
    )

    assert [column.name for column in claim_index.columns] == [
        "status",
        "next_attempt_at",
        "created_at",
    ]


@pytest.mark.asyncio
async def test_claim_next_job_sets_lock_metadata_and_flushes() -> None:
    job = SyncJob(
        integration_id=uuid4(),
        job_type=JobType.CHATWOOT_PRIVATE_NOTE,
        status=JobStatus.PENDING,
        attempt_count=0,
        payload={},
    )
    session = _FakeSession(job=job)

    claimed = await claim_next_job(session, worker_id="worker-1")

    assert claimed is job
    assert job.status == JobStatus.PROCESSING
    assert job.locked_by == "worker-1"
    assert job.locked_at is not None
    assert job.attempt_count == 1
    assert session.flushed is True


@pytest.mark.asyncio
async def test_claim_next_job_does_not_flush_when_no_job_is_due() -> None:
    session = _FakeSession(job=None)

    claimed = await claim_next_job(session, worker_id="worker-1")

    assert claimed is None
    assert session.flushed is False


@pytest.mark.asyncio
async def test_reset_stale_processing_jobs_uses_conditional_update() -> None:
    now = datetime(2026, 7, 3, tzinfo=UTC)
    session = _FakeUpdateSession(rowcount=2)

    reset_count = await reset_stale_processing_jobs(
        session,
        older_than=timedelta(minutes=10),
        now=now,
    )

    assert reset_count == 2
    assert session.compiled_statement is not None
    assert session.compiled_statement.startswith("UPDATE sync_jobs")
    assert "sync_jobs.status = 'processing'" in session.compiled_statement
    assert "sync_jobs.locked_at <" in session.compiled_statement


class _ScalarResult:
    def __init__(self, job: SyncJob | None) -> None:
        self.job = job

    def scalar_one_or_none(self) -> SyncJob | None:
        return self.job


class _FakeSession:
    def __init__(self, *, job: SyncJob | None) -> None:
        self.result = _ScalarResult(job)
        self.flushed = False

    async def execute(self, statement: object) -> _ScalarResult:
        return self.result

    async def flush(self) -> None:
        self.flushed = True


class _RowcountResult:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class _FakeUpdateSession:
    def __init__(self, *, rowcount: int) -> None:
        self.result = _RowcountResult(rowcount)
        self.compiled_statement: str | None = None

    async def execute(self, statement: object) -> _RowcountResult:
        self.compiled_statement = str(
            statement.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            ),
        )
        return self.result
