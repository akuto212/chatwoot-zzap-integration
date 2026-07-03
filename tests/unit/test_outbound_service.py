from __future__ import annotations

from datetime import datetime
from typing import Any, cast
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import (
    ChatwootConversation,
    JobStatus,
    JobType,
    SyncJob,
    ZZapThread,
)
from app.services import outbound
from app.services.outbound import (
    OutboundPersistenceError,
    OutboundProcessor,
    build_zzap_outbound_message,
    persist_outbound_webhook_event,
)


def test_build_zzap_outbound_message_text_and_links() -> None:
    assert (
        build_zzap_outbound_message(
            content="hello",
            uploaded_file_urls=[
                "https://files.example/a.png",
                "https://files.example/b.pdf",
            ],
        )
        == "hello\n\nhttps://files.example/a.png\nhttps://files.example/b.pdf"
    )


def test_build_zzap_outbound_message_links_only() -> None:
    assert (
        build_zzap_outbound_message(
            content="",
            uploaded_file_urls=["https://files.example/a.png"],
        )
        == "https://files.example/a.png"
    )


@pytest.mark.asyncio
async def test_persist_outbound_webhook_event_creates_job(monkeypatch: pytest.MonkeyPatch) -> None:
    integration_id = uuid4()
    payload = {
        "event": "message_created",
        "id": 10,
        "content": "hello",
        "created_at": "2026-07-04T10:00:00+03:00",
        "conversation": {"id": 20, "inbox_id": 2},
        "attachments": [{"file_type": "image", "data_url": "https://chatwoot.test/a.png"}],
    }
    conversation = ChatwootConversation(
        integration_id=integration_id,
        zzap_thread_id=uuid4(),
        chatwoot_contact_id=uuid4(),
        chatwoot_conversation_id=20,
    )
    session = _FakeSession()

    async def fake_get_conversation(*args: object, **kwargs: object) -> ChatwootConversation:
        return conversation

    async def fake_record_delivery(*args: object, **kwargs: Any) -> bool:
        session.deliveries.append(kwargs["delivery_id"])
        return True

    async def fake_has_message_mapping(*args: object, **kwargs: object) -> bool:
        return False

    async def fake_has_outbound_job(*args: object, **kwargs: object) -> bool:
        return False

    monkeypatch.setattr(outbound, "record_webhook_delivery", fake_record_delivery)
    monkeypatch.setattr(outbound, "has_chatwoot_message_mapping", fake_has_message_mapping)
    monkeypatch.setattr(outbound, "has_outbound_sync_job", fake_has_outbound_job)
    monkeypatch.setattr(outbound, "get_chatwoot_conversation_by_chatwoot_id", fake_get_conversation)

    created = await persist_outbound_webhook_event(
        cast(AsyncSession, session),
        payload=payload,
        delivery_id="delivery-1",
        integration_id=integration_id,
    )

    assert created is True
    assert session.jobs[0].job_type == JobType.OUTBOUND_CHATWOOT_MESSAGE_TO_ZZAP
    assert session.jobs[0].chatwoot_message_id == 10
    assert session.jobs[0].chatwoot_conversation_id == 20
    assert session.jobs[0].zzap_thread_id == conversation.zzap_thread_id
    assert session.jobs[0].payload["content"] == "hello"
    assert session.jobs[0].payload["attachments"] == [
        {"data_url": "https://chatwoot.test/a.png", "file_name": "attachment-1"},
    ]
    assert session.deliveries == ["delivery-1"]


@pytest.mark.asyncio
async def test_persist_outbound_webhook_event_returns_false_for_duplicate_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    integration_id = uuid4()
    session = _FakeSession()

    async def fake_has_message_mapping(*args: object, **kwargs: object) -> bool:
        return True

    monkeypatch.setattr(outbound, "has_chatwoot_message_mapping", fake_has_message_mapping)

    created = await persist_outbound_webhook_event(
        cast(AsyncSession, session),
        payload={"id": 10, "conversation": {"id": 20}},
        delivery_id=None,
        integration_id=integration_id,
    )

    assert created is False
    assert session.jobs == []


@pytest.mark.asyncio
async def test_persist_outbound_webhook_event_raises_without_conversation_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    integration_id = uuid4()
    session = _FakeSession()

    async def fake_get_conversation(*args: object, **kwargs: object) -> None:
        return None

    async def fake_has_message_mapping(*args: object, **kwargs: object) -> bool:
        return False

    async def fake_has_outbound_job(*args: object, **kwargs: object) -> bool:
        return False

    monkeypatch.setattr(outbound, "has_chatwoot_message_mapping", fake_has_message_mapping)
    monkeypatch.setattr(outbound, "has_outbound_sync_job", fake_has_outbound_job)
    monkeypatch.setattr(outbound, "get_chatwoot_conversation_by_chatwoot_id", fake_get_conversation)

    with pytest.raises(OutboundPersistenceError):
        await persist_outbound_webhook_event(
            cast(AsyncSession, session),
            payload={"id": 10, "conversation": {"id": 20}},
            delivery_id=None,
            integration_id=integration_id,
        )


@pytest.mark.asyncio
async def test_outbound_processor_blocks_read_only_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    integration_id = uuid4()
    thread = ZZapThread(
        id=uuid4(),
        integration_id=integration_id,
        user_key="zzap-user",
        read_only=True,
    )
    job = SyncJob(
        integration_id=integration_id,
        job_type=JobType.OUTBOUND_CHATWOOT_MESSAGE_TO_ZZAP,
        status=JobStatus.PROCESSING,
        zzap_thread_id=thread.id,
        chatwoot_conversation_id=20,
        payload={"content": "hello"},
    )
    session = _FakeSession()

    async def fake_get_thread(*args: object, **kwargs: object) -> ZZapThread:
        return thread

    monkeypatch.setattr(outbound, "get_zzap_thread_by_id", fake_get_thread)

    processor = OutboundProcessor(
        chatwoot_client=_FakeChatwootClient(),
        zzap_client=_FakeZZapClient(),
        max_attachment_bytes=10,
    )

    await processor.process_job(cast(AsyncSession, session), job)

    assert job.status == JobStatus.BLOCKED
    assert session.jobs[0].job_type == JobType.CHATWOOT_PRIVATE_NOTE
    assert session.jobs[0].chatwoot_conversation_id == 20
    assert session.flushed is True


@pytest.mark.asyncio
async def test_outbound_processor_sends_message_and_clears_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    integration_id = uuid4()
    thread = ZZapThread(
        id=uuid4(),
        integration_id=integration_id,
        user_key="zzap-user",
        read_only=False,
    )
    job = SyncJob(
        integration_id=integration_id,
        job_type=JobType.OUTBOUND_CHATWOOT_MESSAGE_TO_ZZAP,
        status=JobStatus.PROCESSING,
        zzap_thread_id=thread.id,
        payload={
            "content": "hello",
            "created_at": "2026-07-04T10:00:00+03:00",
            "attachments": [
                {"file_name": "a.txt", "data_url": "https://chatwoot.test/a.txt"},
            ],
        },
    )
    chatwoot = _FakeChatwootClient(downloads={"https://chatwoot.test/a.txt": b"file"})
    zzap = _FakeZZapClient(upload_url="https://zzap.test/a.txt")
    session = _FakeSession()

    async def fake_get_thread(*args: object, **kwargs: object) -> ZZapThread:
        return thread

    monkeypatch.setattr(outbound, "get_zzap_thread_by_id", fake_get_thread)

    processor = OutboundProcessor(
        chatwoot_client=chatwoot,
        zzap_client=zzap,
        max_attachment_bytes=10,
    )

    await processor.process_job(cast(AsyncSession, session), job)

    assert zzap.sent_messages == [
            (
                "zzap-user",
                "hello\n\nhttps://zzap.test/a.txt",
                "2026-07-04T10:00:00+03:00",
                True,
            ),
    ]
    assert job.status == JobStatus.SUCCEEDED
    assert job.payload == {}
    assert session.flushed is True


class _FakeSession:
    def __init__(self, *, existing_chatwoot_message_ids: set[int] | None = None) -> None:
        self.existing_chatwoot_message_ids = existing_chatwoot_message_ids or set()
        self.jobs: list[SyncJob] = []
        self.deliveries: list[str] = []
        self.flushed = False

    def add(self, instance: object) -> None:
        if isinstance(instance, SyncJob):
            self.jobs.append(instance)

    async def flush(self) -> None:
        self.flushed = True


class _FakeChatwootClient:
    def __init__(self, *, downloads: dict[str, bytes] | None = None) -> None:
        self.downloads = downloads or {}

    async def download_attachment(self, url: str) -> bytes:
        return self.downloads[url]


class _FakeZZapClient:
    def __init__(self, *, upload_url: str = "https://zzap.test/file") -> None:
        self.upload_url = upload_url
        self.sent_messages: list[tuple[str, str, str, bool]] = []

    async def upload_file(
        self,
        *,
        file_name: str,
        file_body_base64: str,
        upload_type: int = 1,
    ) -> str:
        return self.upload_url

    async def send_message(
        self,
        *,
        user_key: str,
        message: str,
        message_date: datetime,
        is_online: bool,
    ) -> None:
        self.sent_messages.append((user_key, message, message_date.isoformat(), is_online))
