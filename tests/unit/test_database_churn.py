from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.clients.zzap import ZZapThreadDto
from app.db.models import ZZapThread
from app.services.fingerprinting import sha256_hex
from app.workers import jobs
from app.workers.locks import release_worker_advisory_lock, try_worker_advisory_lock
from app.workers.zzap_scheduler import ZZapActionQueue


@pytest.mark.parametrize("acquired", [True, False])
async def test_lock_acquisition_ends_transaction(acquired):
    session = AsyncMock()
    session.execute.return_value = SimpleNamespace(scalar_one=lambda: acquired)
    assert await try_worker_advisory_lock(session) is acquired
    session.commit.assert_awaited_once()


async def test_unlock_ends_transaction():
    session = AsyncMock()
    await release_worker_advisory_lock(session)
    session.commit.assert_awaited_once()


@pytest.mark.parametrize("unread", [0, 2])
async def test_unchanged_summary_does_not_write_but_still_fetches_unread(monkeypatch, unread):
    thread, dto = summary(unread)
    monkeypatch.setattr(jobs, "_get_thread_by_user_key", AsyncMock(return_value=thread))
    upsert = AsyncMock(return_value=thread)
    monkeypatch.setattr(jobs, "upsert_zzap_thread", upsert)
    queue = ZZapActionQueue()
    await jobs._process_summary_threads(
        AsyncMock(),
        settings=SimpleNamespace(integration_id=thread.integration_id),
        threads=[dto],
        action_queue=queue,
    )
    upsert.assert_not_awaited()
    assert queue.size() == (1 if unread else 0)


@pytest.mark.parametrize(
    "field,value",
    [
        ("user_name", "Renamed"),
        ("read_only", True),
        ("unread_count", 3),
        ("message_last", "new message"),
        ("message_last_date", "2026-09-19T12:00:00+00:00"),
    ],
)
async def test_changed_summary_is_persisted(monkeypatch, field, value):
    from dataclasses import replace

    thread, dto = summary(0)
    dto = replace(dto, **{field: value})
    monkeypatch.setattr(jobs, "_get_thread_by_user_key", AsyncMock(return_value=thread))
    upsert = AsyncMock(return_value=thread)
    monkeypatch.setattr(jobs, "upsert_zzap_thread", upsert)
    await jobs._process_summary_threads(
        AsyncMock(),
        settings=SimpleNamespace(integration_id=thread.integration_id),
        threads=[dto],
        action_queue=ZZapActionQueue(),
    )
    upsert.assert_awaited_once()


def summary(unread):
    date = datetime(2026, 9, 18, 12, tzinfo=UTC)
    thread = ZZapThread(
        id=uuid4(),
        integration_id=uuid4(),
        user_key="customer",
        user_name="Name",
        message_last_date=date,
        message_last_hash=sha256_hex("hello"),
        unread_count=unread,
        read_only=False,
    )
    dto = ZZapThreadDto("customer", "Name", unread, date.isoformat(), "hello", False)
    return thread, dto


@pytest.mark.parametrize("unlock_fails", [False, True])
async def test_worker_closes_lock_connection_even_when_cleanup_fails(monkeypatch, unlock_fails):
    from unittest.mock import MagicMock

    connection = AsyncMock()
    connection.__aenter__.return_value = connection
    engine = MagicMock()
    engine.connect.return_value = connection
    engine.dispose = AsyncMock()
    session = AsyncMock()
    monkeypatch.setattr(jobs, "create_engine", lambda _: engine)
    monkeypatch.setattr(jobs, "create_session_factory", lambda _: object())
    monkeypatch.setattr(jobs, "AsyncSession", lambda **_: session)
    for name in ("ZZapClient", "ChatwootClient", "InboundProcessor", "OutboundProcessor"):
        monkeypatch.setattr(jobs, name, MagicMock())
    monkeypatch.setattr(jobs, "try_worker_advisory_lock", AsyncMock(return_value=True))
    monkeypatch.setattr(jobs, "_run_cleanup_once", AsyncMock(side_effect=RuntimeError("cleanup")))
    monkeypatch.setattr(
        jobs,
        "release_worker_advisory_lock",
        AsyncMock(
            side_effect=RuntimeError("unlock") if unlock_fails else None,
        ),
    )
    settings = SimpleNamespace(
        database_url="unused",
        zzap_regular_timeout_seconds=1,
        chatwoot_regular_timeout_seconds=1,
        zzap_base_url="https://example.test",
        zzap_api_key="unused",
        chatwoot_base_url="https://example.test",
        chatwoot_account_id=1,
        chatwoot_api_token="unused",
        chatwoot_inbox_id=1,
        integration_id=uuid4(),
        max_attachment_bytes=100,
    )
    with pytest.raises(RuntimeError, match="unlock" if unlock_fails else "cleanup"):
        await jobs.run_worker_loop(settings)
    connection.invalidate.assert_awaited_once()
    session.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()
