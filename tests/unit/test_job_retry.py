from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import (
    JobStatus,
    JobType,
    MessageDirection,
    MessageMapping,
    MessageStatus,
    SyncJob,
)
from app.workers import jobs
from app.workers.cleanup import cleanup_old_records
from app.workers.jobs import (
    RateLimitedZZapClient,
    process_claimed_job,
    process_next_zzap_action,
    retry_delay_for_attempt,
)
from app.workers.locks import (
    ADVISORY_LOCK_KEY,
    release_worker_advisory_lock,
    try_worker_advisory_lock,
)
from app.workers.rate_limit import ZZapRateLimiter
from app.workers.zzap_scheduler import ZZapActionQueue, ZZapActionType


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


@pytest.mark.asyncio
async def test_process_claimed_job_marks_success_and_clears_lock() -> None:
    job = SyncJob(
        integration_id="11111111-1111-4111-8111-111111111111",
        job_type=JobType.INBOUND_ZZAP_MESSAGE_TO_CHATWOOT,
        status=JobStatus.PROCESSING,
        attempt_count=1,
        locked_by="worker-1",
        locked_at=datetime(2026, 7, 4, tzinfo=UTC),
        payload={"message": "hello"},
    )
    session = _FakeJobSession()
    processor = _FakeProcessor()

    await process_claimed_job(
        cast(AsyncSession, session),
        job=job,
        inbound_processor=processor,
        outbound_processor=_FakeProcessor(),
        chatwoot_client=_FakeChatwootClient(),
        now=datetime(2026, 7, 4, tzinfo=UTC),
    )

    assert processor.processed_jobs == [job]
    assert job.status == JobStatus.SUCCEEDED
    assert job.payload == {}
    assert job.locked_by is None
    assert job.locked_at is None
    assert job.next_attempt_at is None
    assert len(session.executed_statements) == 1
    assert session.flush_count == 1


@pytest.mark.asyncio
async def test_process_claimed_job_retries_inside_transaction_and_preserves_payload() -> None:
    job = SyncJob(
        integration_id="11111111-1111-4111-8111-111111111111",
        job_type=JobType.OUTBOUND_CHATWOOT_MESSAGE_TO_ZZAP,
        status=JobStatus.PROCESSING,
        attempt_count=1,
        locked_by="worker-1",
        locked_at=datetime(2026, 7, 4, tzinfo=UTC),
        payload={"content": "hello"},
    )
    session = _FakeJobSession()
    processor = _FakeProcessor(
        error=RuntimeError("send failed"),
        payload_update={"content": "hello", "uploaded_file_urls": ["https://zzap.test/a.txt"]},
    )
    now = datetime(2026, 7, 4, 12, 0, tzinfo=UTC)

    await process_claimed_job(
        cast(AsyncSession, session),
        job=job,
        inbound_processor=_FakeProcessor(),
        outbound_processor=processor,
        chatwoot_client=_FakeChatwootClient(),
        now=now,
    )

    assert job.status == JobStatus.PENDING
    assert job.next_attempt_at == now + timedelta(minutes=1)
    assert job.payload == {
        "content": "hello",
        "uploaded_file_urls": ["https://zzap.test/a.txt"],
    }
    assert job.last_error == "send failed"
    assert job.locked_by is None
    assert job.locked_at is None
    assert session.flush_count == 2


@pytest.mark.asyncio
async def test_process_claimed_job_exhausts_outbound_and_creates_private_note() -> None:
    job = SyncJob(
        integration_id="11111111-1111-4111-8111-111111111111",
        job_type=JobType.OUTBOUND_CHATWOOT_MESSAGE_TO_ZZAP,
        status=JobStatus.PROCESSING,
        attempt_count=3,
        locked_by="worker-1",
        locked_at=datetime(2026, 7, 4, tzinfo=UTC),
        chatwoot_conversation_id=20,
        payload={
            "content": "hello",
            "uploaded_file_urls": ["https://zzap.test/a.txt"],
        },
    )
    session = _FakeJobSession()

    await process_claimed_job(
        cast(AsyncSession, session),
        job=job,
        inbound_processor=_FakeProcessor(),
        outbound_processor=_FakeProcessor(error=RuntimeError("send failed")),
        chatwoot_client=_FakeChatwootClient(),
        now=datetime(2026, 7, 4, tzinfo=UTC),
    )

    assert job.status == JobStatus.FAILED
    assert job.next_attempt_at is None
    assert job.locked_by is None
    assert job.locked_at is None
    assert session.added_jobs[0].job_type == JobType.CHATWOOT_PRIVATE_NOTE
    assert session.added_jobs[0].chatwoot_conversation_id == 20
    assert "send failed" in session.added_jobs[0].payload["content"]
    assert "https://zzap.test/a.txt" not in session.added_jobs[0].payload["content"]


@pytest.mark.asyncio
async def test_process_claimed_job_exhausts_inbound_and_marks_mapping_failed() -> None:
    mapping_id = uuid4()
    mapping = MessageMapping(
        id=mapping_id,
        integration_id="11111111-1111-4111-8111-111111111111",
        direction=MessageDirection.INBOUND,
        status=MessageStatus.PENDING,
        fingerprint="fingerprint",
        message_hash="hash",
    )
    job = SyncJob(
        integration_id="11111111-1111-4111-8111-111111111111",
        job_type=JobType.INBOUND_ZZAP_MESSAGE_TO_CHATWOOT,
        status=JobStatus.PROCESSING,
        attempt_count=5,
        locked_by="worker-1",
        locked_at=datetime(2026, 7, 4, tzinfo=UTC),
        message_mapping_id=mapping_id,
        payload={"message": "hello"},
    )
    session = _FakeJobSession(message_mappings={mapping_id: mapping})

    await process_claimed_job(
        cast(AsyncSession, session),
        job=job,
        inbound_processor=_FakeProcessor(error=RuntimeError("delivery failed")),
        outbound_processor=_FakeProcessor(),
        chatwoot_client=_FakeChatwootClient(),
        now=datetime(2026, 7, 4, tzinfo=UTC),
    )

    assert job.status == JobStatus.FAILED
    assert mapping.status == MessageStatus.FAILED
    assert job.next_attempt_at is None


@pytest.mark.asyncio
async def test_rate_limited_zzap_client_waits_between_requests() -> None:
    sleep_delays: list[float] = []
    clock = _MutableClock(10.0)
    inner = _FakeZZapApiClient(clock=clock)

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)
        clock.value += delay

    client = RateLimitedZZapClient(
        inner,
        limiter=ZZapRateLimiter(interval_seconds=3.0),
        sleep=fake_sleep,
        monotonic=clock,
    )

    await client.upload_file(file_name="a.txt", file_body_base64="abc")
    clock.value = 13.0
    await client.send_message(
        user_key="zzap-user",
        message="hello",
        message_date=datetime(2026, 7, 4, tzinfo=UTC),
        is_online=True,
    )

    assert sleep_delays == [1.0]
    assert inner.calls == ["upload", "send"]


@pytest.mark.asyncio
async def test_zzap_polling_limiter_waits_after_request_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    integration_id = uuid4()
    clock = _MutableClock(10.0)
    queue = ZZapActionQueue()
    queue.enqueue_summary_poll()
    zzap_client = _ClockedZZapApiClient(clock)
    rate_limiter = ZZapRateLimiter(interval_seconds=3.0)

    async def fake_process_summary_threads(*args: object, **kwargs: object) -> None:
        cast(ZZapActionQueue, kwargs["action_queue"]).enqueue_thread_fetch("thread-1")

    monkeypatch.setattr(jobs, "_process_summary_threads", fake_process_summary_threads)
    monkeypatch.setattr(jobs, "_set_auth_failure_state", _async_noop)
    monkeypatch.setattr(jobs, "session_scope", lambda session_factory: _FakeSessionScope())

    did_work = await process_next_zzap_action(
        session_factory=cast(Any, object()),
        settings=cast(Any, _FakeSettings(integration_id=integration_id)),
        zzap_client=zzap_client,
        action_queue=queue,
        rate_limiter=rate_limiter,
        monotonic=clock,
    )

    assert did_work is True
    assert zzap_client.calls == ["list_threads"]

    clock.value = 13.0
    did_work = await process_next_zzap_action(
        session_factory=cast(Any, object()),
        settings=cast(Any, _FakeSettings(integration_id=integration_id)),
        zzap_client=zzap_client,
        action_queue=queue,
        rate_limiter=rate_limiter,
        monotonic=clock,
    )

    assert did_work is False
    assert zzap_client.calls == ["list_threads"]


@pytest.mark.asyncio
async def test_failed_thread_fetch_is_requeued_after_zzap_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    integration_id = uuid4()
    thread = _FakeThread(id=uuid4(), integration_id=integration_id, user_key="thread-1")
    queue = ZZapActionQueue()
    queue.enqueue_thread_fetch("thread-1")

    async def fake_get_thread(*args: object, **kwargs: object) -> _FakeThread:
        return thread

    async def fake_record_external_auth_failure(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(jobs, "_get_thread_by_user_key", fake_get_thread)
    monkeypatch.setattr(jobs, "_record_external_auth_failure", fake_record_external_auth_failure)
    monkeypatch.setattr(jobs, "session_scope", lambda session_factory: _FakeSessionScope())

    did_work = await process_next_zzap_action(
        session_factory=cast(Any, object()),
        settings=cast(Any, _FakeSettings(integration_id=integration_id)),
        zzap_client=_FailingZZapClient(),
        action_queue=queue,
        rate_limiter=ZZapRateLimiter(interval_seconds=3.0),
        monotonic=lambda: 10.0,
    )

    assert did_work is True
    action = queue.pop_next(now=41.0)
    assert action is not None
    assert action.action_type == ZZapActionType.THREAD_FETCH
    assert action.thread_user_key == "thread-1"


@pytest.mark.asyncio
async def test_periodic_cleanup_runs_only_after_daily_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleanup_runs: list[datetime] = []
    settings = _FakeSettings(integration_id=uuid4())

    async def fake_run_cleanup_once(*args: object, **kwargs: object) -> None:
        cleanup_runs.append(cast(datetime, kwargs["now"]))

    monkeypatch.setattr(jobs, "_run_cleanup_once", fake_run_cleanup_once)

    last_cleanup_at = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    unchanged = await jobs.run_periodic_cleanup_if_due(
        session_factory=cast(Any, object()),
        settings=cast(Any, settings),
        last_cleanup_at=last_cleanup_at,
        now=datetime(2026, 7, 4, 23, 0, tzinfo=UTC),
    )
    assert unchanged == last_cleanup_at
    assert cleanup_runs == []

    due_at = datetime(2026, 7, 5, 0, 0, tzinfo=UTC)
    updated = await jobs.run_periodic_cleanup_if_due(
        session_factory=cast(Any, object()),
        settings=cast(Any, settings),
        last_cleanup_at=last_cleanup_at,
        now=due_at,
    )
    assert updated == due_at
    assert cleanup_runs == [due_at]


class _FakeJobSession:
    def __init__(
        self,
        *,
        message_mappings: dict[object, MessageMapping] | None = None,
    ) -> None:
        self.flush_count = 0
        self.added_jobs: list[SyncJob] = []
        self.message_mappings = message_mappings or {}
        self.executed_statements: list[object] = []

    def add(self, instance: object) -> None:
        if isinstance(instance, SyncJob):
            self.added_jobs.append(instance)

    async def flush(self) -> None:
        self.flush_count += 1

    async def get(self, model: object, instance_id: object) -> object | None:
        if model is MessageMapping:
            return self.message_mappings.get(instance_id)
        return None

    async def execute(self, statement: object) -> object:
        self.executed_statements.append(statement)
        return object()


class _FakeProcessor:
    def __init__(
        self,
        *,
        error: Exception | None = None,
        payload_update: dict[str, object] | None = None,
    ) -> None:
        self.error = error
        self.payload_update = payload_update
        self.processed_jobs: list[SyncJob] = []

    async def process_job(self, session: AsyncSession, job: SyncJob) -> None:
        self.processed_jobs.append(job)
        if self.payload_update is not None:
            job.payload = self.payload_update
            await session.flush()
        if self.error is not None:
            raise self.error


class _FakeChatwootClient:
    def __init__(self) -> None:
        self.private_notes: list[tuple[int, str]] = []

    async def create_private_note(self, *, conversation_id: int, content: str) -> int:
        self.private_notes.append((conversation_id, content))
        return 10


class _FakeZZapApiClient:
    def __init__(self, *, clock: _MutableClock | None = None) -> None:
        self.clock = clock
        self.calls: list[str] = []

    async def upload_file(
        self,
        *,
        file_name: str,
        file_body_base64: str,
        upload_type: int = 1,
    ) -> str:
        self.calls.append("upload")
        if self.clock is not None:
            self.clock.value = 11.0
        return "https://zzap.test/a.txt"

    async def send_message(
        self,
        *,
        user_key: str,
        message: str,
        message_date: datetime,
        is_online: bool,
    ) -> None:
        self.calls.append("send")


class _FakeSettings:
    def __init__(self, *, integration_id: object) -> None:
        self.integration_id = integration_id
        self.successful_message_retention_days = 60
        self.failed_record_retention_days = 30
        self.webhook_delivery_retention_days = 30


class _FakeThread:
    def __init__(self, *, id: object, integration_id: object, user_key: str) -> None:
        self.id = id
        self.integration_id = integration_id
        self.user_key = user_key
        self.unread_count = 0


class _FailingZZapClient:
    async def list_messages(self, *args: object, **kwargs: object) -> list[object]:
        raise RuntimeError("network failed")


class _MutableClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class _ClockedZZapApiClient:
    def __init__(self, clock: _MutableClock) -> None:
        self.clock = clock
        self.calls: list[str] = []

    async def list_threads(self, *, page: int, page_size: int) -> list[object]:
        self.calls.append("list_threads")
        self.clock.value = 12.0
        return []

    async def list_messages(self, *args: object, **kwargs: object) -> list[object]:
        self.calls.append("list_messages")
        return []


async def _async_noop(*args: object, **kwargs: object) -> None:
    return None


class _FakeSessionScope:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *args: object) -> None:
        return None
