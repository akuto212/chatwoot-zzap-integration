from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from app.workers.cleanup import cleanup_old_records
from app.workers.jobs import retry_delay_for_attempt
from app.workers.locks import (
    ADVISORY_LOCK_KEY,
    release_worker_advisory_lock,
    try_worker_advisory_lock,
)


def test_outbound_retry_schedule() -> None:
    assert retry_delay_for_attempt(direction="outbound", attempt_count=1) == timedelta(minutes=1)
    assert retry_delay_for_attempt(direction="outbound", attempt_count=2) == timedelta(minutes=5)
    assert retry_delay_for_attempt(direction="outbound", attempt_count=3) == timedelta(minutes=15)


def test_inbound_retry_schedule() -> None:
    assert retry_delay_for_attempt(direction="inbound", attempt_count=1) == timedelta(seconds=10)
    assert retry_delay_for_attempt(direction="inbound", attempt_count=2) == timedelta(seconds=30)
    assert retry_delay_for_attempt(direction="inbound", attempt_count=3) == timedelta(minutes=1)
    assert retry_delay_for_attempt(direction="inbound", attempt_count=4) == timedelta(minutes=5)
    assert retry_delay_for_attempt(direction="inbound", attempt_count=5) == timedelta(minutes=15)


def test_retry_schedule_returns_none_out_of_range() -> None:
    assert retry_delay_for_attempt(direction="outbound", attempt_count=0) is None
    assert retry_delay_for_attempt(direction="outbound", attempt_count=4) is None
    assert retry_delay_for_attempt(direction="inbound", attempt_count=0) is None
    assert retry_delay_for_attempt(direction="inbound", attempt_count=6) is None


@pytest.mark.asyncio
async def test_try_worker_advisory_lock_uses_configured_key() -> None:
    session = _FakeLockSession(lock_acquired=True)

    acquired = await try_worker_advisory_lock(cast(AsyncSession, session))

    assert acquired is True
    assert str(session.statements[0]) == "SELECT pg_try_advisory_lock(:key)"
    assert session.parameters[0] == {"key": ADVISORY_LOCK_KEY}


@pytest.mark.asyncio
async def test_release_worker_advisory_lock_uses_configured_key() -> None:
    session = _FakeLockSession(lock_acquired=True)

    await release_worker_advisory_lock(cast(AsyncSession, session))

    assert str(session.statements[0]) == "SELECT pg_advisory_unlock(:key)"
    assert session.parameters[0] == {"key": ADVISORY_LOCK_KEY}


@pytest.mark.asyncio
async def test_cleanup_deletes_old_records_and_preserves_cursor_guards() -> None:
    session = _FakeCleanupSession(rowcounts=[1, 2, 3, 4])

    result = await cleanup_old_records(
        cast(AsyncSession, session),
        successful_message_retention=timedelta(days=60),
        failed_record_retention=timedelta(days=30),
        webhook_delivery_retention=timedelta(days=30),
        now=datetime(2026, 7, 4, tzinfo=UTC),
    )

    assert result.successful_message_mappings_deleted == 1
    assert result.failed_message_mappings_deleted == 2
    assert result.failed_jobs_deleted == 3
    assert result.webhook_deliveries_deleted == 4

    successful_mappings_delete = session.compiled_statements[0]
    assert successful_mappings_delete.startswith("DELETE FROM message_mappings")
    assert "message_mappings.status = 'succeeded'" in successful_mappings_delete
    assert "message_mappings.is_cursor_guard IS false" in successful_mappings_delete
    assert "2026-05-05" in successful_mappings_delete

    failed_mappings_delete = session.compiled_statements[1]
    assert "DELETE FROM message_mappings" in failed_mappings_delete
    assert "message_mappings.status = 'failed'" in failed_mappings_delete
    assert "message_mappings.is_cursor_guard IS false" in failed_mappings_delete
    assert "2026-06-04" in failed_mappings_delete

    failed_jobs_delete = session.compiled_statements[2]
    assert failed_jobs_delete.startswith("DELETE FROM sync_jobs")
    assert "sync_jobs.status = 'failed'" in failed_jobs_delete
    assert "2026-06-04" in failed_jobs_delete

    webhook_deliveries_delete = session.compiled_statements[3]
    assert webhook_deliveries_delete.startswith("DELETE FROM webhook_deliveries")
    assert "2026-06-04" in webhook_deliveries_delete


class _ScalarResult:
    def __init__(self, value: bool) -> None:
        self.value = value

    def scalar_one(self) -> bool:
        return self.value


class _FakeLockSession:
    def __init__(self, *, lock_acquired: bool) -> None:
        self.result = _ScalarResult(lock_acquired)
        self.statements: list[object] = []
        self.parameters: list[object] = []

    async def execute(self, statement: object, parameters: object | None = None) -> _ScalarResult:
        self.statements.append(statement)
        self.parameters.append(parameters)
        return self.result


class _RowcountResult:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class _FakeCleanupSession:
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
