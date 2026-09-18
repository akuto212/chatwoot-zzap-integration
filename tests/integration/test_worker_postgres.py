"""Run against a disposable PostgreSQL using TEST_POSTGRES_URL (asyncpg URL)."""

import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.clients.zzap import ZZapThreadDto
from app.db.models import ZZapThread
from app.workers.jobs import _process_summary_threads
from app.workers.locks import release_worker_advisory_lock, try_worker_advisory_lock
from app.workers.zzap_scheduler import ZZapActionQueue

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRES_URL"),
    reason="TEST_POSTGRES_URL is not configured",
)


async def test_lock_does_not_pin_snapshot_and_remains_exclusive():
    engine = create_async_engine(os.environ["TEST_POSTGRES_URL"])
    try:
        async with engine.connect() as owner, engine.connect() as contender:
            async with AsyncSession(bind=owner) as session, AsyncSession(bind=contender) as other:
                pid = await session.scalar(text("SELECT pg_backend_pid()"))
                assert await try_worker_advisory_lock(session)
                assert not session.in_transaction()
                assert not owner.in_transaction()
                assert not await try_worker_advisory_lock(other)
                assert not contender.in_transaction()
                # Observe from a separate connection: querying on the owner
                # would itself start a transaction and take a new snapshot.
                state = (
                    await other.execute(
                        text(
                            "SELECT state, backend_xmin, xact_start "
                            "FROM pg_stat_activity WHERE pid=:pid"
                        ),
                        {"pid": pid},
                    )
                ).one()
                assert state == ("idle", None, None)
                await other.commit()
                await release_worker_advisory_lock(session)
                assert not owner.in_transaction()
                assert await try_worker_advisory_lock(other)
                await release_worker_advisory_lock(other)
    finally:
        await engine.dispose()


async def test_unchanged_polls_do_not_create_row_versions():
    engine = create_async_engine(os.environ["TEST_POSTGRES_URL"])
    try:
        async with engine.connect() as connection:
            # A private schema prevents interference with any existing tables.
            schema = "test_churn_" + uuid4().hex
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.execute(text(f'SET search_path TO "{schema}"'))
            await connection.run_sync(ZZapThread.__table__.create)
            await connection.commit()
            try:
                async with AsyncSession(bind=connection, expire_on_commit=False) as session:
                    settings = SimpleNamespace(integration_id=uuid4())
                    dto = ZZapThreadDto("customer", "Name", 0, None, "hello", False)

                    async def poll():
                        await _process_summary_threads(
                            session,
                            settings=settings,
                            threads=[dto],
                            action_queue=ZZapActionQueue(),
                        )
                        await session.commit()

                    await poll()
                    original = (
                        await session.execute(
                            text("SELECT xmin::text, ctid::text, last_polled_at FROM zzap_threads")
                        )
                    ).one()
                    await session.commit()
                    for _ in range(30):
                        await poll()
                    current = (
                        await session.execute(
                            text("SELECT xmin::text, ctid::text, last_polled_at FROM zzap_threads")
                        )
                    ).one()
                    assert current == original
                    dto = ZZapThreadDto("customer", "Renamed", 1, None, "new", True)
                    await poll()
                    session.expire_all()
                    thread = (await session.execute(select(ZZapThread))).scalar_one()
                    assert thread.user_name == "Renamed"
                    assert thread.read_only is True
                    assert thread.unread_count == 1
            finally:
                await connection.rollback()
                await connection.execute(text("SET search_path TO public"))
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
                await connection.commit()
    finally:
        await engine.dispose()
