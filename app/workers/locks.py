from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

ADVISORY_LOCK_KEY = 721_202_607_003


async def try_worker_advisory_lock(session: AsyncSession) -> bool:
    result = await session.execute(
        text("SELECT pg_try_advisory_lock(:key)"),
        {"key": ADVISORY_LOCK_KEY},
    )
    acquired = bool(result.scalar_one())
    # This is a session-level lock on the worker's dedicated connection.
    # End the snapshot/transaction without releasing the lock: otherwise its
    # xmin prevents VACUUM from reclaiming versions for the worker's lifetime.
    await session.commit()
    return acquired


async def release_worker_advisory_lock(session: AsyncSession) -> None:
    await session.execute(
        text("SELECT pg_advisory_unlock(:key)"),
        {"key": ADVISORY_LOCK_KEY},
    )
    await session.commit()
