from __future__ import annotations

import pytest

from app.db.alembic_config import get_alembic_database_url


def test_alembic_database_url_preserves_asyncpg_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://user:pass@db:5432/chatwoot_zzap",
    )

    assert get_alembic_database_url() == (
        "postgresql+asyncpg://user:pass@db:5432/chatwoot_zzap"
    )
