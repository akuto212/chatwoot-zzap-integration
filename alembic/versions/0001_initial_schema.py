"""initial schema

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-07-03 00:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None

message_direction = sa.Enum("inbound", "outbound", name="message_direction")
message_status = sa.Enum(
    "pending",
    "succeeded",
    "failed",
    "ignored",
    "blocked",
    name="message_status",
)
job_type = sa.Enum(
    "inbound_zzap_message_to_chatwoot",
    "outbound_chatwoot_message_to_zzap",
    "chatwoot_private_note",
    name="job_type",
)
job_status = sa.Enum(
    "pending",
    "processing",
    "succeeded",
    "failed",
    "ignored",
    "blocked",
    name="job_status",
)


def upgrade() -> None:
    bind = op.get_bind()
    message_direction.create(bind, checkfirst=True)
    message_status.create(bind, checkfirst=True)
    job_type.create(bind, checkfirst=True)
    job_status.create(bind, checkfirst=True)

    op.create_table(
        "zzap_threads",
        sa.Column("integration_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_key", sa.String(length=512), nullable=False),
        sa.Column("user_name", sa.String(length=512), nullable=True),
        sa.Column("message_last_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("message_last_hash", sa.String(length=64), nullable=True),
        sa.Column("unread_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("read_only", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("cursor_message_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cursor_guard_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("last_polled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_zzap_threads"),
        sa.UniqueConstraint(
            "integration_id",
            "user_key",
            name="uq_zzap_threads_integration_user_key",
        ),
    )
    op.create_index(
        "ix_zzap_threads_integration_changed",
        "zzap_threads",
        ["integration_id", "message_last_date", "message_last_hash", "unread_count"],
        unique=False,
    )

    op.create_table(
        "chatwoot_contacts",
        sa.Column("integration_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("zzap_user_key", sa.String(length=512), nullable=False),
        sa.Column("chatwoot_contact_id", sa.Integer(), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_chatwoot_contacts"),
        sa.UniqueConstraint(
            "integration_id",
            "chatwoot_contact_id",
            name="uq_chatwoot_contacts_integration_chatwoot_contact_id",
        ),
        sa.UniqueConstraint(
            "integration_id",
            "zzap_user_key",
            name="uq_chatwoot_contacts_integration_zzap_user_key",
        ),
    )

    op.create_table(
        "chatwoot_conversations",
        sa.Column("integration_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("zzap_thread_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chatwoot_contact_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chatwoot_conversation_id", sa.Integer(), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["chatwoot_contact_id"],
            ["chatwoot_contacts.id"],
            name="fk_chatwoot_conversations_chatwoot_contact_id_chatwoot_contacts",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["zzap_thread_id"],
            ["zzap_threads.id"],
            name="fk_chatwoot_conversations_zzap_thread_id_zzap_threads",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_chatwoot_conversations"),
        sa.UniqueConstraint(
            "integration_id",
            "chatwoot_conversation_id",
            name="uq_chatwoot_conversations_integration_conversation",
        ),
        sa.UniqueConstraint(
            "integration_id",
            "zzap_thread_id",
            name="uq_chatwoot_conversations_integration_thread",
        ),
    )

    op.create_table(
        "message_mappings",
        sa.Column("integration_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("direction", message_direction, nullable=False),
        sa.Column("status", message_status, nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("message_hash", sa.String(length=64), nullable=False),
        sa.Column("zzap_thread_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("zzap_sender_user_key", sa.String(length=512), nullable=True),
        sa.Column("zzap_message_id", sa.String(length=128), nullable=True),
        sa.Column("zzap_message_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("chatwoot_message_id", sa.Integer(), nullable=True),
        sa.Column("chatwoot_conversation_id", sa.Integer(), nullable=True),
        sa.Column("is_cursor_guard", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["zzap_thread_id"],
            ["zzap_threads.id"],
            name="fk_message_mappings_zzap_thread_id_zzap_threads",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_message_mappings"),
        sa.UniqueConstraint(
            "integration_id",
            "chatwoot_message_id",
            name="uq_message_mappings_integration_chatwoot_message_id",
        ),
        sa.UniqueConstraint(
            "integration_id",
            "fingerprint",
            name="uq_message_mappings_integration_fingerprint",
        ),
    )
    op.create_index(
        "ix_message_mappings_chatwoot_conversation",
        "message_mappings",
        ["integration_id", "chatwoot_conversation_id"],
        unique=False,
    )
    op.create_index(
        "ix_message_mappings_cleanup",
        "message_mappings",
        ["status", "created_at"],
        unique=False,
    )

    op.create_table(
        "sync_jobs",
        sa.Column("integration_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_type", job_type, nullable=False),
        sa.Column("status", job_status, nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_by", sa.String(length=128), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("zzap_thread_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("chatwoot_conversation_id", sa.Integer(), nullable=True),
        sa.Column("chatwoot_message_id", sa.Integer(), nullable=True),
        sa.Column("message_mapping_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["message_mapping_id"],
            ["message_mappings.id"],
            name="fk_sync_jobs_message_mapping_id_message_mappings",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["zzap_thread_id"],
            ["zzap_threads.id"],
            name="fk_sync_jobs_zzap_thread_id_zzap_threads",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_sync_jobs"),
    )
    op.create_index(
        "ix_sync_jobs_chatwoot_message",
        "sync_jobs",
        ["integration_id", "chatwoot_message_id", "job_type"],
        unique=False,
    )
    op.create_index(
        "ix_sync_jobs_claim",
        "sync_jobs",
        ["integration_id", "status", "next_attempt_at", "created_at"],
        unique=False,
    )

    op.create_table(
        "webhook_deliveries",
        sa.Column("integration_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("delivery_id", sa.String(length=512), nullable=False),
        sa.Column("event_name", sa.String(length=128), nullable=True),
        sa.Column("chatwoot_message_id", sa.Integer(), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_webhook_deliveries"),
        sa.UniqueConstraint(
            "integration_id",
            "delivery_id",
            name="uq_webhook_deliveries_integration_delivery",
        ),
    )
    op.create_index(
        "ix_webhook_deliveries_cleanup",
        "webhook_deliveries",
        ["created_at"],
        unique=False,
    )

    op.create_table(
        "service_state",
        sa.Column("integration_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("key", sa.String(length=128), nullable=False),
        sa.Column(
            "value",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_service_state"),
        sa.UniqueConstraint(
            "integration_id",
            "key",
            name="uq_service_state_integration_key",
        ),
    )


def downgrade() -> None:
    op.drop_table("service_state")
    op.drop_index("ix_webhook_deliveries_cleanup", table_name="webhook_deliveries")
    op.drop_table("webhook_deliveries")
    op.drop_index("ix_sync_jobs_claim", table_name="sync_jobs")
    op.drop_index("ix_sync_jobs_chatwoot_message", table_name="sync_jobs")
    op.drop_table("sync_jobs")
    op.drop_index("ix_message_mappings_cleanup", table_name="message_mappings")
    op.drop_index("ix_message_mappings_chatwoot_conversation", table_name="message_mappings")
    op.drop_table("message_mappings")
    op.drop_table("chatwoot_conversations")
    op.drop_table("chatwoot_contacts")
    op.drop_index("ix_zzap_threads_integration_changed", table_name="zzap_threads")
    op.drop_table("zzap_threads")

    bind = op.get_bind()
    job_status.drop(bind, checkfirst=True)
    job_type.drop(bind, checkfirst=True)
    message_status.drop(bind, checkfirst=True)
    message_direction.drop(bind, checkfirst=True)
