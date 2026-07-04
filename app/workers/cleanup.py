from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import JobStatus, MessageMapping, MessageStatus, SyncJob, WebhookDelivery


@dataclass(frozen=True)
class CleanupResult:
    successful_message_mappings_deleted: int
    failed_message_mappings_deleted: int
    failed_jobs_deleted: int
    webhook_deliveries_deleted: int


async def cleanup_old_records(
    session: AsyncSession,
    *,
    successful_message_retention: timedelta,
    failed_record_retention: timedelta,
    webhook_delivery_retention: timedelta,
    now: datetime | None = None,
) -> CleanupResult:
    current_time = now or datetime.now(tz=UTC)
    successful_message_cutoff = current_time - successful_message_retention
    failed_record_cutoff = current_time - failed_record_retention
    webhook_delivery_cutoff = current_time - webhook_delivery_retention

    successful_message_mappings = await session.execute(
        delete(MessageMapping).where(
            MessageMapping.status == MessageStatus.SUCCEEDED,
            MessageMapping.is_cursor_guard.is_(False),
            MessageMapping.created_at < successful_message_cutoff,
        ),
    )
    failed_message_mappings = await session.execute(
        delete(MessageMapping).where(
            MessageMapping.status == MessageStatus.FAILED,
            MessageMapping.created_at < failed_record_cutoff,
        ),
    )
    failed_jobs = await session.execute(
        delete(SyncJob).where(
            SyncJob.status == JobStatus.FAILED,
            SyncJob.created_at < failed_record_cutoff,
        ),
    )
    webhook_deliveries = await session.execute(
        delete(WebhookDelivery).where(WebhookDelivery.created_at < webhook_delivery_cutoff),
    )

    return CleanupResult(
        successful_message_mappings_deleted=_rowcount(successful_message_mappings),
        failed_message_mappings_deleted=_rowcount(failed_message_mappings),
        failed_jobs_deleted=_rowcount(failed_jobs),
        webhook_deliveries_deleted=_rowcount(webhook_deliveries),
    )


class _RowcountResult(Protocol):
    rowcount: int


def _rowcount(result: object) -> int:
    return cast(_RowcountResult, result).rowcount
