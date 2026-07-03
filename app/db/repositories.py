from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

from sqlalchemy import Select, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import (
    ChatwootContact,
    ChatwootConversation,
    JobStatus,
    JobType,
    MessageDirection,
    MessageMapping,
    MessageStatus,
    SyncJob,
    WebhookDelivery,
    ZZapThread,
)


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


async def upsert_zzap_thread(
    session: AsyncSession,
    *,
    integration_id: UUID,
    user_key: str,
    user_name: str | None,
    message_last_date: datetime | None,
    message_last_hash: str | None,
    unread_count: int,
    read_only: bool,
    last_polled_at: datetime | None = None,
) -> ZZapThread:
    values = {
        "integration_id": integration_id,
        "user_key": user_key,
        "user_name": user_name,
        "message_last_date": message_last_date,
        "message_last_hash": message_last_hash,
        "unread_count": unread_count,
        "read_only": read_only,
        "last_polled_at": last_polled_at,
    }
    statement = (
        pg_insert(ZZapThread)
        .values(**values)
        .on_conflict_do_update(
            index_elements=["integration_id", "user_key"],
            set_=values | {"updated_at": utcnow()},
        )
        .returning(ZZapThread.id)
    )
    result = await session.execute(statement)
    thread_id = result.scalar_one()
    thread = await session.get(ZZapThread, thread_id)
    if thread is None:
        raise RuntimeError("failed to load upserted ZZap thread")
    return thread


async def insert_inbound_message_mapping(
    session: AsyncSession,
    *,
    integration_id: UUID,
    fingerprint: str,
    message_hash: str,
    zzap_thread_id: UUID,
    zzap_sender_user_key: str | None,
    zzap_message_date: datetime,
    is_cursor_guard: bool = False,
) -> tuple[MessageMapping, bool]:
    statement = (
        pg_insert(MessageMapping)
        .values(
            integration_id=integration_id,
            direction=MessageDirection.INBOUND,
            status=MessageStatus.PENDING,
            fingerprint=fingerprint,
            message_hash=message_hash,
            zzap_thread_id=zzap_thread_id,
            zzap_sender_user_key=zzap_sender_user_key,
            zzap_message_date=zzap_message_date,
            is_cursor_guard=is_cursor_guard,
        )
        .on_conflict_do_nothing(index_elements=["integration_id", "fingerprint"])
        .returning(MessageMapping.id)
    )
    result = await session.execute(statement)
    mapping_id = result.scalar_one_or_none()
    created = mapping_id is not None
    if mapping_id is None:
        existing_result = await session.execute(
            select(MessageMapping).where(
                MessageMapping.integration_id == integration_id,
                MessageMapping.fingerprint == fingerprint,
            ),
        )
        mapping = existing_result.scalar_one()
    else:
        inserted_mapping = await session.get(MessageMapping, mapping_id)
        if inserted_mapping is None:
            raise RuntimeError("failed to load inserted message mapping")
        mapping = inserted_mapping
    return mapping, created


async def create_inbound_sync_job(
    session: AsyncSession,
    *,
    integration_id: UUID,
    zzap_thread_id: UUID,
    message_mapping_id: UUID,
    payload: dict[str, object],
) -> SyncJob:
    job = SyncJob(
        integration_id=integration_id,
        job_type=JobType.INBOUND_ZZAP_MESSAGE_TO_CHATWOOT,
        status=JobStatus.PENDING,
        zzap_thread_id=zzap_thread_id,
        message_mapping_id=message_mapping_id,
        payload=payload,
    )
    session.add(job)
    await session.flush()
    return job


async def persist_inbound_message_job(
    session: AsyncSession,
    *,
    integration_id: UUID,
    thread: ZZapThread,
    fingerprint: str,
    message_hash: str,
    zzap_sender_user_key: str | None,
    zzap_message_date: datetime,
    payload: dict[str, object],
    is_cursor_guard: bool = False,
) -> tuple[MessageMapping, SyncJob | None]:
    mapping, created = await insert_inbound_message_mapping(
        session,
        integration_id=integration_id,
        fingerprint=fingerprint,
        message_hash=message_hash,
        zzap_thread_id=thread.id,
        zzap_sender_user_key=zzap_sender_user_key,
        zzap_message_date=zzap_message_date,
        is_cursor_guard=is_cursor_guard,
    )
    if not created:
        return mapping, None

    job = await create_inbound_sync_job(
        session,
        integration_id=integration_id,
        zzap_thread_id=thread.id,
        message_mapping_id=mapping.id,
        payload=payload,
    )
    thread.cursor_message_date = zzap_message_date
    thread.cursor_guard_fingerprint = fingerprint
    await session.flush()
    return mapping, job


async def get_chatwoot_contact_by_zzap_user_key(
    session: AsyncSession,
    *,
    integration_id: UUID,
    zzap_user_key: str,
) -> ChatwootContact | None:
    result = await session.execute(
        select(ChatwootContact).where(
            ChatwootContact.integration_id == integration_id,
            ChatwootContact.zzap_user_key == zzap_user_key,
        ),
    )
    return result.scalar_one_or_none()


async def create_chatwoot_contact_mapping(
    session: AsyncSession,
    *,
    integration_id: UUID,
    zzap_user_key: str,
    chatwoot_contact_id: int,
    chatwoot_source_id: str,
) -> ChatwootContact:
    contact = ChatwootContact(
        integration_id=integration_id,
        zzap_user_key=zzap_user_key,
        chatwoot_contact_id=chatwoot_contact_id,
        chatwoot_source_id=chatwoot_source_id,
    )
    session.add(contact)
    await session.flush()
    return contact


async def get_chatwoot_conversation_by_thread_id(
    session: AsyncSession,
    *,
    integration_id: UUID,
    zzap_thread_id: UUID,
) -> ChatwootConversation | None:
    result = await session.execute(
        select(ChatwootConversation).where(
            ChatwootConversation.integration_id == integration_id,
            ChatwootConversation.zzap_thread_id == zzap_thread_id,
        ),
    )
    return result.scalar_one_or_none()


async def get_chatwoot_conversation_by_chatwoot_id(
    session: AsyncSession,
    *,
    integration_id: UUID,
    chatwoot_conversation_id: int,
) -> ChatwootConversation | None:
    result = await session.execute(
        select(ChatwootConversation).where(
            ChatwootConversation.integration_id == integration_id,
            ChatwootConversation.chatwoot_conversation_id == chatwoot_conversation_id,
        ),
    )
    return result.scalar_one_or_none()


async def create_chatwoot_conversation_mapping(
    session: AsyncSession,
    *,
    integration_id: UUID,
    zzap_thread_id: UUID,
    chatwoot_contact_id: UUID,
    chatwoot_conversation_id: int,
) -> ChatwootConversation:
    conversation = ChatwootConversation(
        integration_id=integration_id,
        zzap_thread_id=zzap_thread_id,
        chatwoot_contact_id=chatwoot_contact_id,
        chatwoot_conversation_id=chatwoot_conversation_id,
    )
    session.add(conversation)
    await session.flush()
    return conversation


async def get_message_mapping_by_id(
    session: AsyncSession,
    *,
    mapping_id: UUID,
) -> MessageMapping | None:
    return await session.get(MessageMapping, mapping_id)


def mark_message_mapping_delivered(
    mapping: MessageMapping,
    *,
    chatwoot_message_id: int,
    chatwoot_conversation_id: int,
) -> None:
    mapping.status = MessageStatus.SUCCEEDED
    mapping.chatwoot_message_id = chatwoot_message_id
    mapping.chatwoot_conversation_id = chatwoot_conversation_id


async def get_zzap_thread_by_id(
    session: AsyncSession,
    *,
    thread_id: UUID,
) -> ZZapThread | None:
    return await session.get(ZZapThread, thread_id)


async def has_chatwoot_message_mapping(
    session: AsyncSession,
    *,
    integration_id: UUID,
    chatwoot_message_id: int,
) -> bool:
    result = await session.execute(
        select(MessageMapping.id).where(
            MessageMapping.integration_id == integration_id,
            MessageMapping.chatwoot_message_id == chatwoot_message_id,
        ),
    )
    return result.scalar_one_or_none() is not None


async def has_outbound_sync_job(
    session: AsyncSession,
    *,
    integration_id: UUID,
    chatwoot_message_id: int,
) -> bool:
    result = await session.execute(
        select(SyncJob.id).where(
            SyncJob.integration_id == integration_id,
            SyncJob.chatwoot_message_id == chatwoot_message_id,
            SyncJob.job_type == JobType.OUTBOUND_CHATWOOT_MESSAGE_TO_ZZAP,
        ),
    )
    return result.scalar_one_or_none() is not None


async def record_webhook_delivery(
    session: AsyncSession,
    *,
    integration_id: UUID,
    delivery_id: str,
    event_name: str | None,
    chatwoot_message_id: int | None,
) -> bool:
    statement = (
        pg_insert(WebhookDelivery)
        .values(
            integration_id=integration_id,
            delivery_id=delivery_id,
            event_name=event_name,
            chatwoot_message_id=chatwoot_message_id,
        )
        .on_conflict_do_nothing(index_elements=["integration_id", "delivery_id"])
        .returning(WebhookDelivery.id)
    )
    result = await session.execute(statement)
    return result.scalar_one_or_none() is not None


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
