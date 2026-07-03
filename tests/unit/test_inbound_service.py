from __future__ import annotations

from datetime import datetime
from typing import cast
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
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
)
from app.services import inbound
from app.services.inbound import InboundProcessor, should_import_zzap_message


def test_should_import_message_newer_than_cursor() -> None:
    cursor = datetime(2025, 1, 1, 10, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))
    message_date = datetime(2025, 1, 1, 10, 0, 1, tzinfo=ZoneInfo("Europe/Moscow"))

    assert should_import_zzap_message(
        message_date=message_date,
        cursor_message_date=cursor,
        fingerprint="new",
        known_fingerprints={"old"},
        cursor_guard_fingerprint="old",
    )


def test_should_not_import_older_message_even_if_fingerprint_missing() -> None:
    cursor = datetime(2025, 1, 1, 10, 0, 1, tzinfo=ZoneInfo("Europe/Moscow"))
    message_date = datetime(2025, 1, 1, 10, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))

    assert not should_import_zzap_message(
        message_date=message_date,
        cursor_message_date=cursor,
        fingerprint="missing-after-retention",
        known_fingerprints=set(),
        cursor_guard_fingerprint="old",
    )


def test_should_not_import_cursor_guard_duplicate() -> None:
    cursor = datetime(2025, 1, 1, 10, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))

    assert not should_import_zzap_message(
        message_date=cursor,
        cursor_message_date=cursor,
        fingerprint="guard",
        known_fingerprints=set(),
        cursor_guard_fingerprint="guard",
    )


@pytest.mark.asyncio
async def test_inbound_processor_requires_mapping_before_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    integration_id = uuid4()
    thread_id = uuid4()
    job = SyncJob(
        integration_id=integration_id,
        job_type=JobType.INBOUND_ZZAP_MESSAGE_TO_CHATWOOT,
        status=JobStatus.PROCESSING,
        zzap_thread_id=thread_id,
        message_mapping_id=uuid4(),
        payload={
            "zzap_user_key": "zzap-user",
            "zzap_user_name": "Alice",
            "message": "hello",
        },
    )
    chatwoot = _FakeChatwootClient(message_id=30)

    async def fake_get_mapping(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(inbound, "get_message_mapping_by_id", fake_get_mapping)

    processor = InboundProcessor(
        chatwoot_client=chatwoot,
        inbox_id=2,
        integration_id=integration_id,
    )

    with pytest.raises(inbound.InboundProcessingError):
        await processor.process_job(cast(AsyncSession, _FakeSession()), job)

    assert chatwoot.incoming_messages == []


@pytest.mark.asyncio
async def test_inbound_processor_short_circuits_already_delivered_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    integration_id = uuid4()
    mapping_id = uuid4()
    mapping = MessageMapping(
        id=mapping_id,
        integration_id=integration_id,
        direction=MessageDirection.INBOUND,
        status=MessageStatus.SUCCEEDED,
        fingerprint="fingerprint",
        message_hash="hash",
        chatwoot_message_id=30,
        chatwoot_conversation_id=20,
    )
    job = SyncJob(
        integration_id=integration_id,
        job_type=JobType.INBOUND_ZZAP_MESSAGE_TO_CHATWOOT,
        status=JobStatus.PROCESSING,
        zzap_thread_id=uuid4(),
        message_mapping_id=mapping_id,
        payload={
            "zzap_user_key": "zzap-user",
            "zzap_user_name": "Alice",
            "message": "hello",
        },
    )
    chatwoot = _FakeChatwootClient(message_id=999)
    session = _FakeSession()

    async def fake_get_mapping(*args: object, **kwargs: object) -> MessageMapping:
        return mapping

    monkeypatch.setattr(inbound, "get_message_mapping_by_id", fake_get_mapping)

    processor = InboundProcessor(
        chatwoot_client=chatwoot,
        inbox_id=2,
        integration_id=integration_id,
    )

    await processor.process_job(cast(AsyncSession, session), job)

    assert chatwoot.incoming_messages == []
    assert job.chatwoot_message_id == 30
    assert job.chatwoot_conversation_id == 20
    assert job.payload == {}
    assert session.flushed is True


@pytest.mark.asyncio
async def test_inbound_processor_delivers_message_and_clears_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    integration_id = uuid4()
    thread_id = uuid4()
    contact = ChatwootContact(
        integration_id=integration_id,
        zzap_user_key="zzap-user",
        chatwoot_contact_id=10,
        chatwoot_source_id="source-1",
    )
    conversation = ChatwootConversation(
        integration_id=integration_id,
        zzap_thread_id=thread_id,
        chatwoot_contact_id=contact.id,
        chatwoot_conversation_id=20,
    )
    mapping_id = uuid4()
    mapping = MessageMapping(
        id=mapping_id,
        integration_id=integration_id,
        direction=MessageDirection.INBOUND,
        status=MessageStatus.PENDING,
        fingerprint="fingerprint",
        message_hash="hash",
    )
    job = SyncJob(
        integration_id=integration_id,
        job_type=JobType.INBOUND_ZZAP_MESSAGE_TO_CHATWOOT,
        status=JobStatus.PROCESSING,
        zzap_thread_id=thread_id,
        message_mapping_id=mapping_id,
        payload={
            "zzap_user_key": "zzap-user",
            "zzap_user_name": "Alice",
            "message": "hello",
        },
    )
    chatwoot = _FakeChatwootClient(message_id=30)
    session = _FakeSession()

    async def fake_get_contact(*args: object, **kwargs: object) -> ChatwootContact:
        return contact

    async def fake_get_conversation(*args: object, **kwargs: object) -> ChatwootConversation:
        return conversation

    async def fake_get_mapping(*args: object, **kwargs: object) -> MessageMapping:
        return mapping

    monkeypatch.setattr(inbound, "get_chatwoot_contact_by_zzap_user_key", fake_get_contact)
    monkeypatch.setattr(inbound, "get_chatwoot_conversation_by_thread_id", fake_get_conversation)
    monkeypatch.setattr(inbound, "get_message_mapping_by_id", fake_get_mapping)

    processor = InboundProcessor(
        chatwoot_client=chatwoot,
        inbox_id=2,
        integration_id=integration_id,
    )

    await processor.process_job(cast(AsyncSession, session), job)

    assert chatwoot.opened_conversation_id == 20
    assert chatwoot.incoming_messages == [(20, "hello")]
    assert mapping.status == MessageStatus.SUCCEEDED
    assert mapping.chatwoot_message_id == 30
    assert mapping.chatwoot_conversation_id == 20
    assert job.chatwoot_message_id == 30
    assert job.chatwoot_conversation_id == 20
    assert job.payload == {}
    assert session.flushed is True


class _FakeSession:
    def __init__(self) -> None:
        self.flushed = False

    async def flush(self) -> None:
        self.flushed = True


class _FakeChatwootClient:
    def __init__(self, *, message_id: int) -> None:
        self.message_id = message_id
        self.opened_conversation_id: int | None = None
        self.incoming_messages: list[tuple[int, str]] = []

    async def update_conversation_status(self, *, conversation_id: int, status: str) -> None:
        assert status == "open"
        self.opened_conversation_id = conversation_id

    async def create_incoming_message(self, *, conversation_id: int, content: str) -> int:
        self.incoming_messages.append((conversation_id, content))
        return self.message_id
