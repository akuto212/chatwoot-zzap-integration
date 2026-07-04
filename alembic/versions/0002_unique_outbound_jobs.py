"""make outbound sync jobs idempotent

Revision ID: 0002_unique_outbound_jobs
Revises: 0001_initial_schema
Create Date: 2026-07-04 00:00:00
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "0002_unique_outbound_jobs"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_sync_jobs_chatwoot_message", table_name="sync_jobs")
    op.create_index(
        "uq_sync_jobs_chatwoot_message_job_type",
        "sync_jobs",
        ["integration_id", "chatwoot_message_id", "job_type"],
        unique=True,
        postgresql_where=sa.text("chatwoot_message_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_sync_jobs_chatwoot_message_job_type", table_name="sync_jobs")
    op.create_index(
        "ix_sync_jobs_chatwoot_message",
        "sync_jobs",
        ["integration_id", "chatwoot_message_id", "job_type"],
        unique=False,
    )
