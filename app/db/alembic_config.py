from __future__ import annotations

import os

from sqlalchemy.engine import make_url


def get_alembic_database_url() -> str:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        msg = "DATABASE_URL environment variable is required for Alembic migrations"
        raise RuntimeError(msg)

    url = make_url(database_url)
    if url.drivername == "postgresql+asyncpg":
        # Alembic runs with a synchronous engine, so asyncpg must be rewritten.
        url = url.set(drivername="postgresql")
    return url.render_as_string(hide_password=False)
