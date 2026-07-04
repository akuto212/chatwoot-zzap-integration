from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

ADVISORY_LOCK_KEY = 721_202_607_003


async def try_worker_advisory_lock(session: AsyncSession) -> bool:
    result = await session.execute(
        text("SELECT pg_try_advisory_lock(:key)"),
        {"key": ADVISORY_LOCK_KEY},
    )
    return bool(result.scalar_one())


async def release_worker_advisory_lock(session: AsyncSession) -> None:
    await session.execute(
        text("SELECT pg_advisory_unlock(:key)"),
        {"key": ADVISORY_LOCK_KEY},
    )
