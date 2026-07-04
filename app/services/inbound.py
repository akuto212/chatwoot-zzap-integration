from __future__ import annotations

import mimetypes
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import unquote, urlparse
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import (
    ChatwootContact,
    ChatwootConversation,
    MessageDirection,
    MessageMapping,
    MessageStatus,
    SyncJob,
)
from app.db.repositories import (
    create_chatwoot_contact_mapping,
    create_chatwoot_conversation_mapping,
    get_chatwoot_contact_by_zzap_user_key,
    get_chatwoot_conversation_by_thread_id,
    get_message_mapping_by_id,
    mark_message_mapping_delivered,
)


class InboundProcessingError(RuntimeError):
    pass


ZZAP_IMAGE_TAG_RE = re.compile(r"\[img\](https?://[^\[]+?)\[/img\]", re.IGNORECASE)
DEFAULT_MAX_ATTACHMENT_BYTES = 10 * 1024 * 1024


@dataclass(frozen=True)
class InboundMessageContent:
    content: str
    attachments: list[dict[str, object]]


def should_import_zzap_message(
    *,
    message_date: datetime,
    cursor_message_date: datetime | None,
    fingerprint: str,
    message_hash: str,
    known_fingerprints: set[str],
    cursor_guard_fingerprint: str | None,
) -> bool:
    if fingerprint in known_fingerprints:
        return False
    is_cursor_guard = cursor_guard_fingerprint in {fingerprint, message_hash}
    if cursor_message_date is None:
        return not is_cursor_guard
    if message_date > cursor_message_date:
        return True
    if message_date == cursor_message_date:
        return not is_cursor_guard
    return False


@dataclass(frozen=True)
class InboundProcessor:
    chatwoot_client: Any
    inbox_id: int
    integration_id: UUID
    max_attachment_bytes: int = DEFAULT_MAX_ATTACHMENT_BYTES

    async def process_job(self, session: AsyncSession, job: SyncJob) -> None:
        if job.zzap_thread_id is None:
            raise InboundProcessingError("inbound job is missing zzap_thread_id")
        mapping = await self._load_mapping(session, job)
        if mapping.status == MessageStatus.SUCCEEDED:
            self._copy_delivered_mapping_to_job(job, mapping)
            await session.flush()
            return

        payload = job.payload
        zzap_user_key = _required_payload_string(payload, "zzap_user_key")
        content = await self._message_content(_required_payload_string(payload, "message"))
        user_name = _display_name(payload.get("zzap_user_name"), zzap_user_key)

        contact = await self._get_or_create_contact(session, zzap_user_key, user_name)
        conversation = await self._get_or_create_conversation(session, job, contact)
        await self.chatwoot_client.update_conversation_status(
            conversation_id=conversation.chatwoot_conversation_id,
            status="open",
        )
        chatwoot_message_id = await self.chatwoot_client.create_incoming_message(
            conversation_id=conversation.chatwoot_conversation_id,
            content=content.content,
            attachments=content.attachments or None,
        )

        mark_message_mapping_delivered(
            mapping,
            chatwoot_message_id=chatwoot_message_id,
            chatwoot_conversation_id=conversation.chatwoot_conversation_id,
        )

        job.chatwoot_conversation_id = conversation.chatwoot_conversation_id
        job.chatwoot_message_id = chatwoot_message_id
        job.payload = {}
        await session.flush()

    async def _message_content(self, raw_content: str) -> InboundMessageContent:
        image_urls = _zzap_image_urls(raw_content)
        if not image_urls:
            return InboundMessageContent(content=raw_content, attachments=[])

        attachments: list[dict[str, object]] = []
        for index, image_url in enumerate(image_urls):
            file_name = _file_name_from_url(image_url, index=index)
            attachments.append(
                {
                    "file_name": file_name,
                    "content": await self.chatwoot_client.download_attachment(
                        image_url,
                        max_bytes=self.max_attachment_bytes,
                    ),
                    "content_type": _content_type(file_name),
                },
            )
        return InboundMessageContent(
            content=_strip_zzap_image_tags(raw_content).strip(),
            attachments=attachments,
        )

    async def _load_mapping(self, session: AsyncSession, job: SyncJob) -> MessageMapping:
        if job.message_mapping_id is None:
            raise InboundProcessingError("inbound job is missing message_mapping_id")
        mapping = await get_message_mapping_by_id(session, mapping_id=job.message_mapping_id)
        if mapping is None:
            raise InboundProcessingError("inbound job message mapping was not found")
        if mapping.integration_id != self.integration_id:
            raise InboundProcessingError("inbound job message mapping integration mismatch")
        if mapping.direction != MessageDirection.INBOUND:
            raise InboundProcessingError("inbound job message mapping direction mismatch")
        return mapping

    def _copy_delivered_mapping_to_job(self, job: SyncJob, mapping: MessageMapping) -> None:
        if mapping.chatwoot_message_id is None or mapping.chatwoot_conversation_id is None:
            raise InboundProcessingError("delivered mapping is missing Chatwoot identifiers")
        job.chatwoot_message_id = mapping.chatwoot_message_id
        job.chatwoot_conversation_id = mapping.chatwoot_conversation_id
        job.payload = {}

    async def _get_or_create_contact(
        self,
        session: AsyncSession,
        zzap_user_key: str,
        user_name: str,
    ) -> ChatwootContact:
        contact = await get_chatwoot_contact_by_zzap_user_key(
            session,
            integration_id=self.integration_id,
            zzap_user_key=zzap_user_key,
        )
        if contact is not None:
            return contact

        chatwoot_contact = await self.chatwoot_client.create_contact(
            inbox_id=self.inbox_id,
            name=user_name,
            custom_attributes={"source": "zzap", "zzap_user_key": zzap_user_key},
        )
        return await create_chatwoot_contact_mapping(
            session,
            integration_id=self.integration_id,
            zzap_user_key=zzap_user_key,
            chatwoot_contact_id=chatwoot_contact.contact_id,
            chatwoot_source_id=chatwoot_contact.source_id,
        )

    async def _get_or_create_conversation(
        self,
        session: AsyncSession,
        job: SyncJob,
        contact: ChatwootContact,
    ) -> ChatwootConversation:
        if job.zzap_thread_id is None:
            raise InboundProcessingError("inbound job is missing zzap_thread_id")

        conversation = await get_chatwoot_conversation_by_thread_id(
            session,
            integration_id=self.integration_id,
            zzap_thread_id=job.zzap_thread_id,
        )
        if conversation is not None:
            return conversation
        if not contact.chatwoot_source_id:
            raise InboundProcessingError("chatwoot contact mapping is missing source_id")

        chatwoot_conversation_id = await self.chatwoot_client.create_conversation(
            inbox_id=self.inbox_id,
            contact_id=contact.chatwoot_contact_id,
            source_id=contact.chatwoot_source_id,
        )
        return await create_chatwoot_conversation_mapping(
            session,
            integration_id=self.integration_id,
            zzap_thread_id=job.zzap_thread_id,
            chatwoot_contact_id=contact.id,
            chatwoot_conversation_id=chatwoot_conversation_id,
        )


def _required_payload_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise InboundProcessingError(f"inbound job payload is missing {key}")
    return value


def _zzap_image_urls(content: str) -> list[str]:
    return [match.group(1).strip() for match in ZZAP_IMAGE_TAG_RE.finditer(content)]


def _strip_zzap_image_tags(content: str) -> str:
    return ZZAP_IMAGE_TAG_RE.sub("", content)


def _file_name_from_url(url: str, *, index: int) -> str:
    file_name = unquote(urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]).strip()
    if _has_file_extension(file_name):
        return file_name
    return f"zzap-image-{index + 1}"


def _has_file_extension(file_name: str) -> bool:
    if "." not in file_name:
        return False
    stem, extension = file_name.rsplit(".", 1)
    return bool(stem and extension)


def _content_type(file_name: str) -> str:
    guessed_type, _ = mimetypes.guess_type(file_name)
    return guessed_type or "application/octet-stream"


def _display_name(value: object, zzap_user_key: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return f"ZZap {zzap_user_key[:8]}"
