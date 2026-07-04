from __future__ import annotations

from datetime import datetime
from typing import cast
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.chatwoot import ChatwootContactDto
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
        message_hash="new-hash",
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
        message_hash="missing-hash",
        known_fingerprints=set(),
        cursor_guard_fingerprint="old",
    )


def test_should_not_import_cursor_guard_duplicate() -> None:
    cursor = datetime(2025, 1, 1, 10, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))

    assert not should_import_zzap_message(
        message_date=cursor,
        cursor_message_date=cursor,
        fingerprint="guard",
        message_hash="hash",
        known_fingerprints=set(),
        cursor_guard_fingerprint="guard",
    )


def test_should_not_import_cursor_guard_duplicate_when_guard_is_summary_hash() -> None:
    cursor = datetime(2025, 1, 1, 10, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))

    assert not should_import_zzap_message(
        message_date=cursor,
        cursor_message_date=cursor,
        fingerprint="full-fingerprint",
        message_hash="summary-hash",
        known_fingerprints=set(),
        cursor_guard_fingerprint="summary-hash",
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
    assert chatwoot.incoming_messages == [(20, "hello", None)]
    assert mapping.status == MessageStatus.SUCCEEDED
    assert mapping.chatwoot_message_id == 30
    assert mapping.chatwoot_conversation_id == 20
    assert job.chatwoot_message_id == 30
    assert job.chatwoot_conversation_id == 20
    assert job.payload == {}
    assert session.flushed is True


@pytest.mark.asyncio
async def test_inbound_processor_converts_zzap_image_tag_to_chatwoot_attachment(
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
    image_url = (
        "https://koj.blob.core.windows.net/zzap-upload/upload/messagefiles/"
        "67f36901dae479903df9ddcc04b3304e.webp"
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
            "message": f"[img]{image_url}[/img]",
        },
    )
    chatwoot = _FakeChatwootClient(
        message_id=30,
        downloads={image_url: b"webp-body"},
    )
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

    assert chatwoot.downloaded_urls == [image_url]
    assert chatwoot.incoming_messages == [
        (
            20,
            "",
            [
                {
                    "file_name": "67f36901dae479903df9ddcc04b3304e.webp",
                    "content": b"webp-body",
                    "content_type": "image/webp",
                },
            ],
        ),
    ]
    assert mapping.status == MessageStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_inbound_processor_creates_contact_with_source_attribute_and_fallback_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    integration_id = uuid4()
    thread_id = uuid4()
    mapping_id = uuid4()
    mapping = MessageMapping(
        id=mapping_id,
        integration_id=integration_id,
        direction=MessageDirection.INBOUND,
        status=MessageStatus.PENDING,
        fingerprint="fingerprint",
        message_hash="hash",
    )
    contact = ChatwootContact(
        integration_id=integration_id,
        zzap_user_key="abcdef123456",
        chatwoot_contact_id=10,
        chatwoot_source_id="source-1",
    )
    conversation = ChatwootConversation(
        integration_id=integration_id,
        zzap_thread_id=thread_id,
        chatwoot_contact_id=contact.id,
        chatwoot_conversation_id=20,
    )
    job = SyncJob(
        integration_id=integration_id,
        job_type=JobType.INBOUND_ZZAP_MESSAGE_TO_CHATWOOT,
        status=JobStatus.PROCESSING,
        zzap_thread_id=thread_id,
        message_mapping_id=mapping_id,
        payload={
            "zzap_user_key": "abcdef123456",
            "message": "hello",
        },
    )
    chatwoot = _FakeChatwootClient(message_id=30)

    async def fake_get_contact(*args: object, **kwargs: object) -> None:
        return None

    async def fake_create_contact_mapping(*args: object, **kwargs: object) -> ChatwootContact:
        return contact

    async def fake_get_conversation(*args: object, **kwargs: object) -> ChatwootConversation:
        return conversation

    async def fake_get_mapping(*args: object, **kwargs: object) -> MessageMapping:
        return mapping

    monkeypatch.setattr(inbound, "get_chatwoot_contact_by_zzap_user_key", fake_get_contact)
    monkeypatch.setattr(inbound, "create_chatwoot_contact_mapping", fake_create_contact_mapping)
    monkeypatch.setattr(inbound, "get_chatwoot_conversation_by_thread_id", fake_get_conversation)
    monkeypatch.setattr(inbound, "get_message_mapping_by_id", fake_get_mapping)

    processor = InboundProcessor(
        chatwoot_client=chatwoot,
        inbox_id=2,
        integration_id=integration_id,
    )

    await processor.process_job(cast(AsyncSession, _FakeSession()), job)

    assert chatwoot.created_contacts == [
        (
            2,
            "ZZap abcdef12",
            {"source": "zzap", "zzap_user_key": "abcdef123456"},
        ),
    ]


class _FakeSession:
    def __init__(self) -> None:
        self.flushed = False

    async def flush(self) -> None:
        self.flushed = True


class _FakeChatwootClient:
    def __init__(self, *, message_id: int, downloads: dict[str, bytes] | None = None) -> None:
        self.message_id = message_id
        self.downloads = downloads or {}
        self.downloaded_urls: list[str] = []
        self.opened_conversation_id: int | None = None
        self.incoming_messages: list[tuple[int, str, list[dict[str, object]] | None]] = []
        self.created_contacts: list[tuple[int, str, dict[str, str]]] = []

    async def create_contact(
        self,
        *,
        inbox_id: int,
        name: str,
        custom_attributes: dict[str, str],
    ) -> ChatwootContactDto:
        self.created_contacts.append((inbox_id, name, custom_attributes))
        return ChatwootContactDto(contact_id=10, source_id="source-1")

    async def update_conversation_status(self, *, conversation_id: int, status: str) -> None:
        assert status == "open"
        self.opened_conversation_id = conversation_id

    async def create_incoming_message(
        self,
        *,
        conversation_id: int,
        content: str,
        attachments: list[dict[str, object]] | None = None,
    ) -> int:
        self.incoming_messages.append((conversation_id, content, attachments))
        return self.message_id

    async def download_attachment(self, url: str, *, max_bytes: int) -> bytes:
        self.downloaded_urls.append(url)
        return self.downloads[url]
