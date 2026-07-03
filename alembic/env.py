from __future__ import annotations

from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import make_url

from alembic import context
from app.db import models  # noqa: F401
from app.db.base import Base
from app.settings import Settings

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    url = make_url(Settings().database_url)
    if url.drivername == "postgresql+asyncpg":
        # Alembic's synchronous engine cannot use the asyncpg driver directly.
        url = url.set(drivername="postgresql")
    return url.render_as_string(hide_password=False)


def run_migrations_offline() -> None:
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
