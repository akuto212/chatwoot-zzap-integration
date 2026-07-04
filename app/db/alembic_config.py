from __future__ import annotations

import os


def get_alembic_database_url() -> str:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        msg = "DATABASE_URL environment variable is required for Alembic migrations"
        raise RuntimeError(msg)

    return database_url
