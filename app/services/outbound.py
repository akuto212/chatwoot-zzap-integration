from __future__ import annotations

from base64 import b64encode
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import JobStatus, JobType, SyncJob
from app.db.repositories import (
    get_chatwoot_conversation_by_chatwoot_id,
    get_zzap_thread_by_id,
    has_chatwoot_message_mapping,
    has_outbound_sync_job,
    record_webhook_delivery,
)
from app.services.attachments import ensure_attachment_size


class OutboundPersistenceError(RuntimeError):
    pass


class OutboundProcessingError(RuntimeError):
    pass


class ChatwootWebhookDecision(StrEnum):
    ACCEPT = "accept"
    IGNORE = "ignore"


def build_zzap_outbound_message(*, content: str, uploaded_file_urls: list[str]) -> str:
    stripped_content = content.strip()
    links = "\n".join(uploaded_file_urls)
    if stripped_content and links:
        return f"{stripped_content}\n\n{links}"
    if links:
        return links
    return stripped_content


async def persist_outbound_webhook_event(
    session: AsyncSession,
    *,
    payload: dict[str, Any],
    delivery_id: str | None,
    integration_id: UUID,
) -> bool:
    chatwoot_message_id = _required_int(payload, "id")
    conversation = payload.get("conversation")
    if not isinstance(conversation, dict):
        raise OutboundPersistenceError("outbound webhook is missing conversation")
    chatwoot_conversation_id = _required_int(conversation, "id")

    if delivery_id:
        delivery_created = await record_webhook_delivery(
            session,
            integration_id=integration_id,
            delivery_id=delivery_id,
            event_name=_optional_string(payload.get("event")),
            chatwoot_message_id=chatwoot_message_id,
        )
        if not delivery_created:
            return False

    if await has_chatwoot_message_mapping(
        session,
        integration_id=integration_id,
        chatwoot_message_id=chatwoot_message_id,
    ):
        return False
    if await has_outbound_sync_job(
        session,
        integration_id=integration_id,
        chatwoot_message_id=chatwoot_message_id,
    ):
        return False

    conversation_mapping = await get_chatwoot_conversation_by_chatwoot_id(
        session,
        integration_id=integration_id,
        chatwoot_conversation_id=chatwoot_conversation_id,
    )
    if conversation_mapping is None:
        raise OutboundPersistenceError("chatwoot conversation mapping was not found")

    job = SyncJob(
        integration_id=integration_id,
        job_type=JobType.OUTBOUND_CHATWOOT_MESSAGE_TO_ZZAP,
        status=JobStatus.PENDING,
        zzap_thread_id=conversation_mapping.zzap_thread_id,
        chatwoot_conversation_id=chatwoot_conversation_id,
        chatwoot_message_id=chatwoot_message_id,
        payload={
            "content": str(payload.get("content") or ""),
            "created_at": _optional_string(payload.get("created_at")),
            "attachments": _attachment_payloads(payload.get("attachments")),
        },
    )
    session.add(job)
    await session.flush()
    return True


@dataclass(frozen=True)
class OutboundProcessor:
    chatwoot_client: Any
    zzap_client: Any
    max_attachment_bytes: int

    async def process_job(self, session: AsyncSession, job: SyncJob) -> None:
        if job.status == JobStatus.SUCCEEDED:
            return
        if job.zzap_thread_id is None:
            raise OutboundProcessingError("outbound job is missing zzap_thread_id")
        thread = await get_zzap_thread_by_id(session, thread_id=job.zzap_thread_id)
        if thread is None:
            raise OutboundProcessingError("outbound job thread was not found")
        if thread.read_only:
            self._block_read_only_job(session, job)
            await session.flush()
            return

        payload = job.payload
        uploaded_file_urls = list(_uploaded_file_urls(payload))
        attachments = _attachment_payloads(payload.get("attachments"))
        for attachment in attachments[len(uploaded_file_urls) :]:
            body = await self.chatwoot_client.download_attachment(attachment["data_url"])
            ensure_attachment_size(len(body), self.max_attachment_bytes)
            file_url = await self.zzap_client.upload_file(
                file_name=attachment["file_name"],
                file_body_base64=b64encode(body).decode("ascii"),
            )
            uploaded_file_urls.append(file_url)
            payload["uploaded_file_urls"] = uploaded_file_urls
            await session.flush()

        content = str(payload.get("content") or "")
        await self.zzap_client.send_message(
            user_key=thread.user_key,
            message=build_zzap_outbound_message(
                content=content,
                uploaded_file_urls=uploaded_file_urls,
            ),
            message_date=_message_date(payload.get("created_at")),
            is_online=True,
        )
        job.status = JobStatus.SUCCEEDED
        job.payload = {}
        await session.flush()

    def _block_read_only_job(self, session: AsyncSession, job: SyncJob) -> None:
        job.status = JobStatus.BLOCKED
        session.add(
            SyncJob(
                integration_id=job.integration_id,
                job_type=JobType.CHATWOOT_PRIVATE_NOTE,
                status=JobStatus.PENDING,
                chatwoot_conversation_id=job.chatwoot_conversation_id,
                payload={"content": "ZZap thread is read-only; message was not sent."},
            ),
        )


def classify_chatwoot_message_created(
    payload: dict[str, Any],
    expected_inbox_id: int,
) -> ChatwootWebhookDecision:
    if payload.get("event") != "message_created":
        return ChatwootWebhookDecision.IGNORE
    if payload.get("message_type") != "outgoing":
        return ChatwootWebhookDecision.IGNORE
    if payload.get("private") is True:
        return ChatwootWebhookDecision.IGNORE

    conversation = payload.get("conversation")
    if not isinstance(conversation, dict):
        return ChatwootWebhookDecision.IGNORE

    try:
        inbox_id = int(conversation.get("inbox_id") or 0)
    except (TypeError, ValueError):
        return ChatwootWebhookDecision.IGNORE

    if inbox_id != expected_inbox_id:
        return ChatwootWebhookDecision.IGNORE

    sender = payload.get("sender")
    if not isinstance(sender, dict):
        return ChatwootWebhookDecision.IGNORE
    if sender.get("type") != "user":
        return ChatwootWebhookDecision.IGNORE

    return ChatwootWebhookDecision.ACCEPT


def _attachment_payloads(value: object) -> list[dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise OutboundPersistenceError("attachments payload must be a list")

    attachments: list[dict[str, str]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise OutboundPersistenceError("attachment item must be an object")
        url = item.get("data_url") or item.get("download_url")
        if not isinstance(url, str) or not url:
            raise OutboundPersistenceError("attachment item is missing data_url")
        file_name = item.get("file_name") or item.get("filename") or f"attachment-{index + 1}"
        attachments.append({"data_url": url, "file_name": str(file_name)})
    return attachments


def _uploaded_file_urls(payload: dict[str, Any]) -> list[str]:
    value = payload.get("uploaded_file_urls") or []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise OutboundProcessingError("uploaded_file_urls payload must be a list of strings")
    return value


def _message_date(value: object) -> datetime:
    if isinstance(value, str) and value:
        return datetime.fromisoformat(value)
    return datetime.now().astimezone()


def _required_int(payload: dict[str, Any], key: str) -> int:
    try:
        return int(payload[key])
    except (KeyError, TypeError, ValueError) as exc:
        raise OutboundPersistenceError(f"outbound webhook is missing integer {key}") from exc


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None
