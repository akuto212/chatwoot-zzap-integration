from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast
from uuid import uuid4

import pytest
from sqlalchemy import Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.schema import CreateIndex

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
    table = cast(Table, SyncJob.__table__)
    claim_index = next(
        index for index in table.indexes if index.name == "ix_sync_jobs_claim"
    )
    compiled = str(CreateIndex(claim_index).compile(dialect=postgresql.dialect()))

    assert [column.name for column in claim_index.columns] == [
        "status",
        "next_attempt_at",
        "created_at",
    ]
    assert "next_attempt_at ASC NULLS FIRST" in compiled


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

    claimed = await claim_next_job(cast(AsyncSession, session), worker_id="worker-1")

    assert claimed is job
    assert job.status == JobStatus.PROCESSING
    assert job.locked_by == "worker-1"
    assert job.locked_at is not None
    assert job.attempt_count == 1
    assert session.flushed is True
    assert session.flushed_status == JobStatus.PROCESSING
    assert session.flushed_locked_by == "worker-1"
    assert session.flushed_locked_at is not None
    assert session.flushed_attempt_count == 1


@pytest.mark.asyncio
async def test_claim_next_job_does_not_flush_when_no_job_is_due() -> None:
    session = _FakeSession(job=None)

    claimed = await claim_next_job(cast(AsyncSession, session), worker_id="worker-1")

    assert claimed is None
    assert session.flushed is False


@pytest.mark.asyncio
async def test_reset_stale_processing_jobs_uses_conditional_update() -> None:
    now = datetime(2026, 7, 3, tzinfo=UTC)
    session = _FakeUpdateSession(rowcounts=[1, 1, 2])

    reset_count = await reset_stale_processing_jobs(
        cast(AsyncSession, session),
        older_than=timedelta(minutes=10),
        now=now,
    )

    assert reset_count == 3
    assert len(session.compiled_statements) == 3
    mapping_update = session.compiled_statements[0]
    assert mapping_update.startswith("UPDATE message_mappings")
    assert "status='failed'" in mapping_update
    assert "message_mappings.id IN" in mapping_update
    assert "sync_jobs.job_type = 'inbound_zzap_message_to_chatwoot'" in mapping_update
    assert "sync_jobs.status = 'processing'" in mapping_update
    assert "sync_jobs.attempt_count >= 5" in mapping_update

    failed_update = session.compiled_statements[1]
    assert failed_update.startswith("UPDATE sync_jobs")
    assert "status='failed'" in failed_update
    assert "sync_jobs.status = 'processing'" in failed_update
    assert "sync_jobs.locked_at <" in failed_update
    assert "sync_jobs.attempt_count >= 5" in failed_update
    assert "sync_jobs.attempt_count >= 3" in failed_update

    pending_update = session.compiled_statements[2]
    assert pending_update.startswith("UPDATE sync_jobs")
    assert "status='pending'" in pending_update
    assert "sync_jobs.status = 'processing'" in pending_update
    assert "sync_jobs.locked_at <" in pending_update
    assert "sync_jobs.attempt_count < 5" in pending_update
    assert "sync_jobs.attempt_count < 3" in pending_update


class _ScalarResult:
    def __init__(self, job: SyncJob | None) -> None:
        self.job = job

    def scalar_one_or_none(self) -> SyncJob | None:
        return self.job


class _FakeSession:
    def __init__(self, *, job: SyncJob | None) -> None:
        self.result = _ScalarResult(job)
        self.flushed = False
        self.flushed_status: JobStatus | None = None
        self.flushed_locked_by: str | None = None
        self.flushed_locked_at: datetime | None = None
        self.flushed_attempt_count: int | None = None

    async def execute(self, statement: object) -> _ScalarResult:
        return self.result

    async def flush(self) -> None:
        self.flushed = True
        job = self.result.job
        if job is not None:
            self.flushed_status = job.status
            self.flushed_locked_by = job.locked_by
            self.flushed_locked_at = job.locked_at
            self.flushed_attempt_count = job.attempt_count


class _RowcountResult:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class _FakeUpdateSession:
    def __init__(self, *, rowcounts: list[int]) -> None:
        self.rowcounts = rowcounts
        self.compiled_statements: list[str] = []

    async def execute(self, statement: _CompilableStatement) -> _RowcountResult:
        self.compiled_statements.append(
            str(
                statement.compile(
                    dialect=postgresql.dialect(),
                    compile_kwargs={"literal_binds": True},
                ),
            ),
        )
        return _RowcountResult(self.rowcounts.pop(0))


class _CompilableStatement(Protocol):
    def compile(self, *args: Any, **kwargs: Any) -> object:
        pass
