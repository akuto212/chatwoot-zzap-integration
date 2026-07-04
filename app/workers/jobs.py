from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.chatwoot import ChatwootApiError, ChatwootClient
from app.clients.zzap import ZZapApiError, ZZapClient, ZZapMessageDto, ZZapThreadDto
from app.db.models import (
    JobStatus,
    JobType,
    MessageDirection,
    MessageMapping,
    MessageStatus,
    ServiceState,
    SyncJob,
    ZZapThread,
)
from app.db.repositories import (
    claim_next_job,
    persist_inbound_message_job,
    reset_stale_processing_jobs,
    upsert_zzap_thread,
)
from app.db.session import create_engine, create_session_factory, session_scope
from app.services.fingerprinting import (
    build_zzap_fingerprint,
    normalize_message_text,
    parse_zzap_datetime,
    sha256_hex,
)
from app.services.inbound import InboundProcessor, should_import_zzap_message
from app.services.outbound import OutboundProcessor
from app.settings import Settings
from app.workers.cleanup import cleanup_old_records
from app.workers.locks import release_worker_advisory_lock, try_worker_advisory_lock
from app.workers.rate_limit import ZZapRateLimiter
from app.workers.zzap_scheduler import ZZapActionQueue, ZZapActionType

OUTBOUND_RETRY_DELAYS = [timedelta(minutes=1), timedelta(minutes=5), timedelta(minutes=15)]
INBOUND_RETRY_DELAYS = [
    timedelta(seconds=10),
    timedelta(seconds=30),
    timedelta(minutes=1),
    timedelta(minutes=5),
    timedelta(minutes=15),
]
MAX_OUTBOUND_ATTEMPTS = 3
MAX_INBOUND_ATTEMPTS = 5
CLEANUP_INTERVAL = timedelta(days=1)
OUTBOUND_ECHO_TOLERANCE = timedelta(minutes=2)


@dataclass(frozen=True)
class OutboundEchoGuard:
    message_hash: str
    zzap_message_date: datetime | None
    created_at: datetime | None


def retry_delay_for_attempt(*, direction: str, attempt_count: int) -> timedelta | None:
    delays = OUTBOUND_RETRY_DELAYS if direction == "outbound" else INBOUND_RETRY_DELAYS
    index = attempt_count - 1
    if index < 0 or index >= len(delays):
        return None
    return delays[index]


def _constant_datetime(value: datetime) -> Callable[[], datetime]:
    def _now() -> datetime:
        return value

    return _now


class RateLimitedZZapClient:
    def __init__(
        self,
        client: Any,
        *,
        limiter: ZZapRateLimiter,
        sleep: Callable[[float], Any] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._limiter = limiter
        self._sleep = sleep
        self._monotonic = monotonic

    async def upload_file(
        self,
        *,
        file_name: str,
        file_body_base64: str,
        upload_type: int = 1,
    ) -> str:
        await self._wait_for_slot()
        try:
            return await self._client.upload_file(
                file_name=file_name,
                file_body_base64=file_body_base64,
                upload_type=upload_type,
            )
        finally:
            self._mark_request_finished()

    async def send_message(
        self,
        *,
        user_key: str,
        message: str,
        message_date: datetime,
        is_online: bool,
    ) -> None:
        await self._wait_for_slot()
        try:
            await self._client.send_message(
                user_key=user_key,
                message=message,
                message_date=message_date,
                is_online=is_online,
            )
        finally:
            self._mark_request_finished()

    async def _wait_for_slot(self) -> None:
        now = self._monotonic()
        delay = self._limiter.delay_until_next(now=now)
        if delay > 0:
            await self._sleep(delay)

    def _mark_request_finished(self) -> None:
        self._limiter.mark_request_finished(now=self._monotonic())


async def run_worker_loop(settings: Settings) -> None:
    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    worker_id = f"worker-{uuid4()}"
    action_queue = ZZapActionQueue()
    rate_limiter = ZZapRateLimiter(interval_seconds=3.0)

    async with httpx.AsyncClient(timeout=settings.zzap_regular_timeout_seconds) as zzap_http:
        async with httpx.AsyncClient(
            timeout=settings.chatwoot_regular_timeout_seconds,
        ) as chatwoot_http:
            zzap_client = ZZapClient(
                base_url=settings.zzap_base_url,
                api_key=settings.zzap_api_key,
                http_client=zzap_http,
            )
            chatwoot_client = ChatwootClient(
                base_url=settings.chatwoot_base_url,
                account_id=settings.chatwoot_account_id,
                api_token=settings.chatwoot_api_token,
                http_client=chatwoot_http,
            )
            inbound_processor = InboundProcessor(
                chatwoot_client=chatwoot_client,
                inbox_id=settings.chatwoot_inbox_id,
                integration_id=settings.integration_id,
                max_attachment_bytes=settings.max_attachment_bytes,
            )
            outbound_processor = OutboundProcessor(
                chatwoot_client=chatwoot_client,
                zzap_client=RateLimitedZZapClient(zzap_client, limiter=rate_limiter),
                max_attachment_bytes=settings.max_attachment_bytes,
            )
            async with engine.connect() as lock_connection:
                lock_session = AsyncSession(bind=lock_connection)
                try:
                    if not await try_worker_advisory_lock(lock_session):
                        raise RuntimeError("another worker already holds the advisory lock")
                    last_cleanup_at = datetime.now(tz=UTC)
                    await _run_cleanup_once(
                        session_factory=session_factory,
                        settings=settings,
                        now=last_cleanup_at,
                    )
                    while True:
                        iteration_time = datetime.now(tz=UTC)
                        last_cleanup_at = await run_periodic_cleanup_if_due(
                            session_factory=session_factory,
                            settings=settings,
                            last_cleanup_at=last_cleanup_at,
                            now=iteration_time,
                        )
                        did_work = await run_worker_iteration(
                            session_factory=session_factory,
                            settings=settings,
                            worker_id=worker_id,
                            inbound_processor=inbound_processor,
                            outbound_processor=outbound_processor,
                            chatwoot_client=chatwoot_client,
                            zzap_client=zzap_client,
                            action_queue=action_queue,
                            rate_limiter=rate_limiter,
                            now=_constant_datetime(iteration_time),
                        )
                        if not did_work:
                            await asyncio.sleep(1.0)
                finally:
                    await release_worker_advisory_lock(lock_session)
                    await lock_session.close()
                    await engine.dispose()


async def run_worker_iteration(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    worker_id: str,
    inbound_processor: Any,
    outbound_processor: Any,
    chatwoot_client: Any,
    zzap_client: Any,
    action_queue: ZZapActionQueue,
    rate_limiter: ZZapRateLimiter,
    now: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
) -> bool:
    current_time = now() if now else datetime.now(tz=UTC)
    did_work = False
    claimed_job_id = None
    async with session_scope(session_factory) as session:
        reset_count = await reset_stale_processing_jobs(
            session,
            older_than=timedelta(minutes=10),
            now=current_time,
        )
        did_work = reset_count > 0
        job = await claim_next_job(session, worker_id=worker_id)
        if job is not None:
            claimed_job_id = job.id
            did_work = True

    if claimed_job_id is not None:
        async with session_scope(session_factory) as session:
            job = await session.get(SyncJob, claimed_job_id)
            if (
                job is not None
                and job.status == JobStatus.PROCESSING
                and job.locked_by == worker_id
            ):
                await process_claimed_job(
                    session,
                    job=job,
                    inbound_processor=inbound_processor,
                    outbound_processor=outbound_processor,
                    chatwoot_client=chatwoot_client,
                    now=current_time,
                )

    scheduled = action_queue.enqueue_summary_poll_if_due(
        now=(monotonic or time.monotonic)(),
        interval_seconds=3.0,
    )
    did_work = scheduled or did_work
    did_work = await process_next_zzap_action(
        session_factory=session_factory,
        settings=settings,
        zzap_client=zzap_client,
        action_queue=action_queue,
        rate_limiter=rate_limiter,
        monotonic=monotonic,
    ) or did_work

    async with session_scope(session_factory) as session:
        await _upsert_service_state(
            session,
            integration_id=settings.integration_id,
            key="worker_heartbeat",
            value={"at": current_time.isoformat()},
        )
    return did_work


async def process_next_zzap_action(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    zzap_client: Any,
    action_queue: ZZapActionQueue,
    rate_limiter: ZZapRateLimiter,
    monotonic: Callable[[], float] | None = None,
) -> bool:
    current_monotonic = (monotonic or time.monotonic)()
    if rate_limiter.delay_until_next(now=current_monotonic) > 0:
        return False
    action = action_queue.pop_next(now=current_monotonic)
    if action is None:
        return False

    try:
        if action.action_type == ZZapActionType.SUMMARY_POLL:
            try:
                threads = await zzap_client.list_threads(page=1, page_size=100)
            finally:
                rate_limiter.mark_request_finished(now=(monotonic or time.monotonic)())
            async with session_scope(session_factory) as session:
                await _set_auth_failure_state(
                    session,
                    integration_id=settings.integration_id,
                    key="zzap_auth_failed",
                    failed=False,
                )
                await _process_summary_threads(
                    session,
                    settings=settings,
                    threads=threads,
                    action_queue=action_queue,
                )
            return True

        if action.action_type == ZZapActionType.THREAD_FETCH and action.thread_user_key:
            async with session_scope(session_factory) as session:
                thread = await _get_thread_by_user_key(
                    session,
                    integration_id=settings.integration_id,
                    user_key=action.thread_user_key,
                )
            if thread is None:
                return True

            page_size = min(100, max(20, thread.unread_count + 5))
            try:
                messages = await zzap_client.list_messages(
                    user_key=thread.user_key,
                    page=1,
                    page_size=page_size,
                )
            finally:
                rate_limiter.mark_request_finished(now=(monotonic or time.monotonic)())
            async with session_scope(session_factory) as session:
                await _set_auth_failure_state(
                    session,
                    integration_id=settings.integration_id,
                    key="zzap_auth_failed",
                    failed=False,
                )
                fresh_thread = await session.get(ZZapThread, thread.id)
                if fresh_thread is None:
                    return True
                await _persist_thread_messages(
                    session,
                    settings=settings,
                    thread=fresh_thread,
                    messages=messages,
                )
            return True
    except Exception as exc:
        if action.action_type == ZZapActionType.THREAD_FETCH and action.thread_user_key:
            action_queue.enqueue_thread_fetch(action.thread_user_key)
        _delay_zzap_poll_after_error(action_queue, now=current_monotonic, exc=exc)
        async with session_scope(session_factory) as session:
            await _record_external_auth_failure(
                session,
                integration_id=settings.integration_id,
                exc=exc,
            )
        return True

    return False


async def process_claimed_job(
    session: AsyncSession,
    *,
    job: SyncJob,
    inbound_processor: Any,
    outbound_processor: Any,
    chatwoot_client: Any,
    now: datetime | None = None,
) -> None:
    current_time = now or datetime.now(tz=UTC)
    payload_before_dispatch = dict(job.payload)
    try:
        await _dispatch_job(
            session,
            job=job,
            inbound_processor=inbound_processor,
            outbound_processor=outbound_processor,
            chatwoot_client=chatwoot_client,
        )
    except Exception as exc:
        await _record_external_auth_failure(session, integration_id=job.integration_id, exc=exc)
        _mark_job_failed_or_pending(job, exc=exc, now=current_time)
        if _should_mark_inbound_mapping_failed(job):
            await _mark_inbound_mapping_failed(session, job)
        if _should_create_exhausted_outbound_note(job):
            session.add(
                SyncJob(
                    integration_id=job.integration_id,
                    job_type=JobType.CHATWOOT_PRIVATE_NOTE,
                    status=JobStatus.PENDING,
                    chatwoot_conversation_id=job.chatwoot_conversation_id,
                    payload={"content": _outbound_failure_note_content(exc)},
                ),
            )
        await session.flush()
        return

    if job.status == JobStatus.PROCESSING:
        job.status = JobStatus.SUCCEEDED
        job.payload = {}
    if job.status == JobStatus.SUCCEEDED:
        await _record_job_auth_success(
            session,
            job=job,
            payload_before_dispatch=payload_before_dispatch,
        )
    _clear_job_lock(job)
    job.next_attempt_at = None
    job.last_error = None
    await session.flush()


async def _dispatch_job(
    session: AsyncSession,
    *,
    job: SyncJob,
    inbound_processor: Any,
    outbound_processor: Any,
    chatwoot_client: Any,
) -> None:
    if job.job_type == JobType.INBOUND_ZZAP_MESSAGE_TO_CHATWOOT:
        await inbound_processor.process_job(session, job)
        return
    if job.job_type == JobType.OUTBOUND_CHATWOOT_MESSAGE_TO_ZZAP:
        await outbound_processor.process_job(session, job)
        return
    if job.job_type == JobType.CHATWOOT_PRIVATE_NOTE:
        if job.chatwoot_conversation_id is None:
            raise RuntimeError("private note job is missing chatwoot_conversation_id")
        await chatwoot_client.create_private_note(
            conversation_id=job.chatwoot_conversation_id,
            content=str(job.payload.get("content") or ""),
        )
        return
    raise RuntimeError(f"unsupported job type: {job.job_type}")


def _mark_job_failed_or_pending(job: SyncJob, *, exc: Exception, now: datetime) -> None:
    if job.attempt_count >= _max_attempts_for_job(job):
        job.status = JobStatus.FAILED
        job.next_attempt_at = None
    else:
        delay = retry_delay_for_attempt(
            direction=_retry_direction_for_job(job),
            attempt_count=job.attempt_count,
        )
        if delay is None:
            job.status = JobStatus.FAILED
            job.next_attempt_at = None
            job.last_error = str(exc)
            _clear_job_lock(job)
            return
        job.status = JobStatus.PENDING
        job.next_attempt_at = now + delay
    job.last_error = str(exc)
    _clear_job_lock(job)


def _retry_direction_for_job(job: SyncJob) -> str:
    if job.job_type == JobType.INBOUND_ZZAP_MESSAGE_TO_CHATWOOT:
        return "inbound"
    return "outbound"


def _max_attempts_for_job(job: SyncJob) -> int:
    if job.job_type == JobType.INBOUND_ZZAP_MESSAGE_TO_CHATWOOT:
        return MAX_INBOUND_ATTEMPTS
    return MAX_OUTBOUND_ATTEMPTS


def _should_mark_inbound_mapping_failed(job: SyncJob) -> bool:
    return (
        job.job_type == JobType.INBOUND_ZZAP_MESSAGE_TO_CHATWOOT
        and job.status == JobStatus.FAILED
        and job.message_mapping_id is not None
    )


async def _mark_inbound_mapping_failed(session: AsyncSession, job: SyncJob) -> None:
    if job.message_mapping_id is None:
        return
    mapping = await session.get(MessageMapping, job.message_mapping_id)
    if mapping is not None:
        mapping.status = MessageStatus.FAILED


def _should_create_exhausted_outbound_note(job: SyncJob) -> bool:
    return (
        job.job_type == JobType.OUTBOUND_CHATWOOT_MESSAGE_TO_ZZAP
        and job.status == JobStatus.FAILED
        and job.chatwoot_conversation_id is not None
    )


def _outbound_failure_note_content(exc: Exception) -> str:
    error = str(exc).strip() or exc.__class__.__name__
    return f"ZZap message was not sent after retries: {error}"


def _clear_job_lock(job: SyncJob) -> None:
    job.locked_at = None
    job.locked_by = None


async def _record_job_auth_success(
    session: AsyncSession,
    *,
    job: SyncJob,
    payload_before_dispatch: dict[str, Any],
) -> None:
    if job.job_type in {
        JobType.INBOUND_ZZAP_MESSAGE_TO_CHATWOOT,
        JobType.CHATWOOT_PRIVATE_NOTE,
    }:
        await _set_auth_failure_state(
            session,
            integration_id=job.integration_id,
            key="chatwoot_auth_failed",
            failed=False,
        )
    if job.job_type == JobType.OUTBOUND_CHATWOOT_MESSAGE_TO_ZZAP:
        await _set_auth_failure_state(
            session,
            integration_id=job.integration_id,
            key="zzap_auth_failed",
            failed=False,
        )
        if payload_before_dispatch.get("attachments"):
            await _set_auth_failure_state(
                session,
                integration_id=job.integration_id,
                key="chatwoot_auth_failed",
                failed=False,
            )


async def _run_cleanup_once(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    now: datetime | None = None,
) -> None:
    async with session_scope(session_factory) as session:
        await cleanup_old_records(
            session,
            successful_message_retention=timedelta(
                days=settings.successful_message_retention_days,
            ),
            failed_record_retention=timedelta(days=settings.failed_record_retention_days),
            webhook_delivery_retention=timedelta(days=settings.webhook_delivery_retention_days),
            now=now,
        )


async def run_periodic_cleanup_if_due(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    last_cleanup_at: datetime,
    now: datetime,
) -> datetime:
    if now - last_cleanup_at < CLEANUP_INTERVAL:
        return last_cleanup_at
    await _run_cleanup_once(session_factory=session_factory, settings=settings, now=now)
    return now


async def _process_summary_threads(
    session: AsyncSession,
    *,
    settings: Settings,
    threads: list[ZZapThreadDto],
    action_queue: ZZapActionQueue,
) -> None:
    for thread_dto in threads:
        message_last_date = (
            parse_zzap_datetime(thread_dto.message_last_date)
            if thread_dto.message_last_date
            else None
        )
        message_last_hash = (
            sha256_hex(normalize_message_text(thread_dto.message_last))
            if thread_dto.message_last is not None
            else None
        )
        existing_thread = await _get_thread_by_user_key(
            session,
            integration_id=settings.integration_id,
            user_key=thread_dto.user_key,
        )
        changed = _thread_summary_changed(
            existing_thread,
            message_last_date=message_last_date,
            message_last_hash=message_last_hash,
            unread_count=thread_dto.unread_count,
        )
        thread = await upsert_zzap_thread(
            session,
            integration_id=settings.integration_id,
            user_key=thread_dto.user_key,
            user_name=thread_dto.user_name,
            message_last_date=message_last_date,
            message_last_hash=message_last_hash,
            unread_count=thread_dto.unread_count,
            read_only=thread_dto.read_only,
            last_polled_at=datetime.now(tz=UTC),
        )
        if existing_thread is None and thread_dto.unread_count == 0:
            thread.cursor_message_date = message_last_date
            thread.cursor_guard_fingerprint = message_last_hash
        if thread_dto.unread_count > 0 or changed:
            action_queue.enqueue_thread_fetch(thread_dto.user_key)


async def _persist_thread_messages(
    session: AsyncSession,
    *,
    settings: Settings,
    thread: ZZapThread,
    messages: list[ZZapMessageDto],
) -> None:
    message_items = _message_items(settings=settings, thread=thread, messages=messages)
    known_fingerprints = await _known_fingerprints(
        session,
        integration_id=settings.integration_id,
        fingerprints={item.fingerprint.fingerprint for item in message_items},
    )
    outbound_echo_guards = await _known_outbound_echo_guards(
        session,
        integration_id=settings.integration_id,
        thread_id=thread.id,
        message_hashes={item.fingerprint.message_hash for item in message_items},
    )
    bootstrap_fallback_fingerprints = _bootstrap_fallback_fingerprints(
        thread=thread,
        message_items=message_items,
    )

    for item in message_items:
        if not should_import_zzap_message(
            message_date=item.message_date,
            cursor_message_date=thread.cursor_message_date,
            fingerprint=item.fingerprint.fingerprint,
            known_fingerprints=known_fingerprints,
            cursor_guard_fingerprint=thread.cursor_guard_fingerprint,
            message_hash=item.fingerprint.message_hash,
        ):
            continue
        if _is_outbound_echo(item, outbound_echo_guards):
            continue
        if not _should_import_bootstrap_item(
            thread=thread,
            item=item,
            bootstrap_fallback_fingerprints=bootstrap_fallback_fingerprints,
        ):
            continue
        await persist_inbound_message_job(
            session,
            integration_id=settings.integration_id,
            thread=thread,
            fingerprint=item.fingerprint.fingerprint,
            message_hash=item.fingerprint.message_hash,
            zzap_sender_user_key=item.sender_user_key,
            zzap_message_date=item.message_date,
            payload={
                "zzap_user_key": thread.user_key,
                "zzap_user_name": thread.user_name,
                "message": item.message_text,
            },
        )
        known_fingerprints.add(item.fingerprint.fingerprint)


def _bootstrap_fallback_fingerprints(
    *,
    thread: ZZapThread,
    message_items: list[_MessageItem],
) -> set[str]:
    if thread.cursor_message_date is not None or thread.unread_count <= 0:
        return set()
    if any(item.unread is True for item in message_items):
        return set()
    return {
        item.fingerprint.fingerprint
        for item in message_items[-thread.unread_count :]
    }


def _should_import_bootstrap_item(
    *,
    thread: ZZapThread,
    item: _MessageItem,
    bootstrap_fallback_fingerprints: set[str],
) -> bool:
    if thread.cursor_message_date is not None:
        return True
    if item.unread is True:
        return True
    return item.fingerprint.fingerprint in bootstrap_fallback_fingerprints


def _is_outbound_echo(item: _MessageItem, guards: list[OutboundEchoGuard]) -> bool:
    for guard in guards:
        if item.fingerprint.message_hash != guard.message_hash:
            continue
        if _within_outbound_echo_tolerance(item.message_date, guard.zzap_message_date):
            return True
        if _within_outbound_echo_tolerance(item.message_date, guard.created_at):
            return True
    return False


def _within_outbound_echo_tolerance(left: datetime, right: datetime | None) -> bool:
    if right is None:
        return False
    return abs(left - right) <= OUTBOUND_ECHO_TOLERANCE


async def _get_thread_by_user_key(
    session: AsyncSession,
    *,
    integration_id: Any,
    user_key: str,
) -> ZZapThread | None:
    result = await session.execute(
        select(ZZapThread).where(
            ZZapThread.integration_id == integration_id,
            ZZapThread.user_key == user_key,
        ),
    )
    return result.scalar_one_or_none()


def _thread_summary_changed(
    thread: ZZapThread | None,
    *,
    message_last_date: datetime | None,
    message_last_hash: str | None,
    unread_count: int,
) -> bool:
    if thread is None:
        return False
    return (
        thread.message_last_date != message_last_date
        or thread.message_last_hash != message_last_hash
        or thread.unread_count != unread_count
    )


class _MessageItem:
    def __init__(
        self,
        *,
        message_date: datetime,
        sender_user_key: str,
        message_text: str,
        unread: bool | None,
        fingerprint: Any,
        response_index: int,
    ) -> None:
        self.message_date = message_date
        self.sender_user_key = sender_user_key
        self.message_text = message_text
        self.unread = unread
        self.fingerprint = fingerprint
        self.response_index = response_index


def _message_items(
    *,
    settings: Settings,
    thread: ZZapThread,
    messages: list[ZZapMessageDto],
) -> list[_MessageItem]:
    items: list[_MessageItem] = []
    for index, message in enumerate(messages):
        if not message.message_date:
            continue
        message_date = parse_zzap_datetime(message.message_date)
        message_text = message.message or ""
        sender_user_key = message.user_key or thread.user_key
        fingerprint = build_zzap_fingerprint(
            integration_id=str(settings.integration_id),
            thread_user_key=thread.user_key,
            sender_user_key=sender_user_key,
            message_date=message_date,
            message_text=message_text,
        )
        items.append(
            _MessageItem(
                message_date=message_date,
                sender_user_key=sender_user_key,
                message_text=message_text,
                unread=message.unread,
                fingerprint=fingerprint,
                response_index=index,
            ),
        )
    return sorted(items, key=lambda item: (item.message_date, item.response_index))


async def _known_fingerprints(
    session: AsyncSession,
    *,
    integration_id: Any,
    fingerprints: set[str],
) -> set[str]:
    if not fingerprints:
        return set()
    result = await session.execute(
        select(MessageMapping.fingerprint).where(
            MessageMapping.integration_id == integration_id,
            MessageMapping.fingerprint.in_(fingerprints),
        ),
    )
    return {str(row[0]) for row in result.all()}


async def _known_outbound_echo_guards(
    session: AsyncSession,
    *,
    integration_id: Any,
    thread_id: Any,
    message_hashes: set[str],
) -> list[OutboundEchoGuard]:
    if not message_hashes:
        return []
    result = await session.execute(
        select(
            MessageMapping.message_hash,
            MessageMapping.zzap_message_date,
            MessageMapping.created_at,
        ).where(
            MessageMapping.integration_id == integration_id,
            MessageMapping.zzap_thread_id == thread_id,
            MessageMapping.direction == MessageDirection.OUTBOUND,
            MessageMapping.status == MessageStatus.SUCCEEDED,
            MessageMapping.message_hash.in_(message_hashes),
        ),
    )
    return [
        OutboundEchoGuard(
            message_hash=message_hash,
            zzap_message_date=zzap_message_date,
            created_at=created_at,
        )
        for message_hash, zzap_message_date, created_at in result.all()
    ]


async def _upsert_service_state(
    session: AsyncSession,
    *,
    integration_id: Any,
    key: str,
    value: dict[str, object],
) -> None:
    statement = (
        pg_insert(ServiceState)
        .values(integration_id=integration_id, key=key, value=value)
        .on_conflict_do_update(
            index_elements=["integration_id", "key"],
            set_={"value": value, "updated_at": datetime.now(tz=UTC)},
        )
    )
    await session.execute(statement)


async def _set_auth_failure_state(
    session: AsyncSession,
    *,
    integration_id: Any,
    key: str,
    failed: bool,
) -> None:
    await _upsert_service_state(
        session,
        integration_id=integration_id,
        key=key,
        value={"failed": failed},
    )


async def _record_external_auth_failure(
    session: AsyncSession,
    *,
    integration_id: Any,
    exc: Exception,
) -> None:
    if isinstance(exc, ZZapApiError) and exc.status_code == 401:
        await _set_auth_failure_state(
            session,
            integration_id=integration_id,
            key="zzap_auth_failed",
            failed=True,
        )
    if isinstance(exc, ChatwootApiError) and exc.status_code in {401, 403}:
        await _set_auth_failure_state(
            session,
            integration_id=integration_id,
            key="chatwoot_auth_failed",
            failed=True,
        )


def _delay_zzap_poll_after_error(
    action_queue: ZZapActionQueue,
    *,
    now: float,
    exc: Exception,
) -> None:
    action_queue.delay_summary_until(now=now, delay_seconds=_zzap_poll_backoff_seconds(exc))


def _zzap_poll_backoff_seconds(exc: Exception) -> float:
    if isinstance(exc, ZZapApiError):
        if exc.status_code == 401:
            return 300.0
        if exc.status_code == 429:
            return 60.0
    message = str(exc).lower()
    if "captcha" in message or "rate" in message:
        return 60.0
    return 30.0
