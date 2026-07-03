from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class MessageDirection(StrEnum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"


class MessageStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    IGNORED = "ignored"
    BLOCKED = "blocked"


class JobType(StrEnum):
    INBOUND_ZZAP_MESSAGE_TO_CHATWOOT = "inbound_zzap_message_to_chatwoot"
    OUTBOUND_CHATWOOT_MESSAGE_TO_ZZAP = "outbound_chatwoot_message_to_zzap"
    CHATWOOT_PRIVATE_NOTE = "chatwoot_private_note"


class JobStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    IGNORED = "ignored"
    BLOCKED = "blocked"


def _build_str_enum_type(enum_class: type[StrEnum], *, name: str) -> Enum:
    return Enum(
        enum_class,
        name=name,
        values_callable=lambda enum_type: [member.value for member in enum_type],
    )


MessageDirectionType = _build_str_enum_type(MessageDirection, name="message_direction")
MessageStatusType = _build_str_enum_type(MessageStatus, name="message_status")
JobTypeType = _build_str_enum_type(JobType, name="job_type")
JobStatusType = _build_str_enum_type(JobStatus, name="job_status")


class ZZapThread(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "zzap_threads"
    __table_args__ = (
        UniqueConstraint("integration_id", "user_key", name="uq_zzap_threads_integration_user_key"),
        Index(
            "ix_zzap_threads_integration_changed",
            "integration_id",
            "message_last_date",
            "message_last_hash",
            "unread_count",
        ),
    )

    integration_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    user_key: Mapped[str] = mapped_column(String(512), nullable=False)
    user_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    message_last_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    message_last_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    unread_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    read_only: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    cursor_message_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    cursor_guard_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_polled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ChatwootContact(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "chatwoot_contacts"
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "zzap_user_key",
            name="uq_chatwoot_contacts_integration_zzap_user_key",
        ),
        UniqueConstraint(
            "integration_id",
            "chatwoot_contact_id",
            name="uq_chatwoot_contacts_integration_chatwoot_contact_id",
        ),
    )

    integration_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    zzap_user_key: Mapped[str] = mapped_column(String(512), nullable=False)
    chatwoot_contact_id: Mapped[int] = mapped_column(Integer, nullable=False)


class ChatwootConversation(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "chatwoot_conversations"
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "zzap_thread_id",
            name="uq_chatwoot_conversations_integration_thread",
        ),
        UniqueConstraint(
            "integration_id",
            "chatwoot_conversation_id",
            name="uq_chatwoot_conversations_integration_conversation",
        ),
    )

    integration_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    zzap_thread_id: Mapped[UUID] = mapped_column(
        ForeignKey("zzap_threads.id", ondelete="CASCADE"),
        nullable=False,
    )
    chatwoot_contact_id: Mapped[UUID] = mapped_column(
        ForeignKey("chatwoot_contacts.id", ondelete="CASCADE"),
        nullable=False,
    )
    chatwoot_conversation_id: Mapped[int] = mapped_column(Integer, nullable=False)


class MessageMapping(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "message_mappings"
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "fingerprint",
            name="uq_message_mappings_integration_fingerprint",
        ),
        UniqueConstraint(
            "integration_id",
            "chatwoot_message_id",
            name="uq_message_mappings_integration_chatwoot_message_id",
        ),
        Index("ix_message_mappings_cleanup", "status", "created_at"),
        Index(
            "ix_message_mappings_chatwoot_conversation",
            "integration_id",
            "chatwoot_conversation_id",
        ),
    )

    integration_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    direction: Mapped[MessageDirection] = mapped_column(MessageDirectionType, nullable=False)
    status: Mapped[MessageStatus] = mapped_column(MessageStatusType, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    message_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    zzap_thread_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("zzap_threads.id", ondelete="SET NULL"),
        nullable=True,
    )
    zzap_sender_user_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    zzap_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    zzap_message_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    chatwoot_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    chatwoot_conversation_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_cursor_guard: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )


class SyncJob(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "sync_jobs"
    __table_args__ = (
        Index("ix_sync_jobs_claim", "status", "next_attempt_at", "created_at"),
        Index(
            "ix_sync_jobs_chatwoot_message",
            "integration_id",
            "chatwoot_message_id",
            "job_type",
        ),
    )

    integration_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    job_type: Mapped[JobType] = mapped_column(JobTypeType, nullable=False)
    status: Mapped[JobStatus] = mapped_column(JobStatusType, nullable=False)
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    locked_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    zzap_thread_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("zzap_threads.id", ondelete="SET NULL"),
        nullable=True,
    )
    chatwoot_conversation_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    chatwoot_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    message_mapping_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("message_mappings.id", ondelete="SET NULL"),
        nullable=True,
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )


class WebhookDelivery(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "delivery_id",
            name="uq_webhook_deliveries_integration_delivery",
        ),
        Index("ix_webhook_deliveries_cleanup", "created_at"),
    )

    integration_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    delivery_id: Mapped[str] = mapped_column(String(512), nullable=False)
    event_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    chatwoot_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)


class ServiceState(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "service_state"
    __table_args__ = (
        UniqueConstraint("integration_id", "key", name="uq_service_state_integration_key"),
    )

    integration_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    key: Mapped[str] = mapped_column(String(128), nullable=False)
    value: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
