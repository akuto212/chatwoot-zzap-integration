# ZZap Chatwoot Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Dockerized Litestar microservice that synchronizes messages bidirectionally between ZZap and a self-hosted Chatwoot instance.

**Architecture:** One Python project runs in `web`, `worker`, or `all` mode. Litestar handles HTTP endpoints and Chatwoot webhooks; a PostgreSQL-backed worker handles ZZap polling, durable jobs, retry, cleanup, and readiness state. PostgreSQL is the only stateful dependency and stores mappings, jobs, idempotency records, and service state.

**Tech Stack:** Python 3.14, uv, Litestar, SQLAlchemy async, asyncpg, Alembic, httpx, pydantic-settings, structlog/logging, pytest, pytest-asyncio, ruff, mypy, PostgreSQL, Docker Compose.

---

## Source Spec

Implement from:

- `docs/superpowers/specs/2026-07-03-zzap-chatwoot-integration-design.md`

Do not change the product decisions in the spec while executing this plan. If implementation discovers a conflict with ZZap or Chatwoot API behavior, stop and document the concrete conflict before changing scope.

## Scope Check

This plan covers one integrated service. The work is broad but not decomposed into separate sub-projects because the main behaviors share the same database schema, idempotency model, and worker lifecycle.

## File Structure Map

Create this structure:

```text
app/
  __init__.py
  asgi.py
  cli.py
  settings.py
  api/
    __init__.py
    health.py
    webhooks.py
  clients/
    __init__.py
    chatwoot.py
    zzap.py
  db/
    __init__.py
    base.py
    models.py
    repositories.py
    session.py
  services/
    __init__.py
    attachments.py
    fingerprinting.py
    inbound.py
    outbound.py
    readiness.py
    webhooks.py
  workers/
    __init__.py
    cleanup.py
    jobs.py
    locks.py
    rate_limit.py
    zzap_scheduler.py
tests/
  conftest.py
  unit/
    test_attachments.py
    test_fingerprinting.py
    test_job_claiming.py
    test_rate_limit.py
    test_settings.py
    test_webhook_security.py
    test_webhook_service.py
    test_zzap_scheduler.py
alembic/
  env.py
  script.py.mako
  versions/
scripts/
  docker-entrypoint.sh
```

Keep files focused. If a file grows large while implementing, split along the boundaries above rather than adding a general utility module.

## Implementation Tasks

### Task 1: Project Tooling And Package Scaffold

**Files:**

- Modify: `pyproject.toml`
- Create: `app/__init__.py`
- Create: `app/api/__init__.py`
- Create: `app/clients/__init__.py`
- Create: `app/db/__init__.py`
- Create: `app/services/__init__.py`
- Create: `app/workers/__init__.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Add runtime and dev dependencies**

Run:

```bash
rtk uv add litestar uvicorn sqlalchemy asyncpg alembic httpx pydantic-settings structlog
rtk uv add --dev pytest pytest-asyncio ruff mypy
```

Expected: `pyproject.toml` and `uv.lock` update successfully.

- [ ] **Step 2: Configure pytest, ruff, and mypy**

Modify `pyproject.toml` so it contains these tool sections:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.ruff]
line-length = 100
target-version = "py314"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]

[tool.mypy]
python_version = "3.14"
ignore_missing_imports = true
warn_unused_configs = true
```

- [ ] **Step 3: Create package directories**

Create the package `__init__.py` files listed in this task. Each file can be empty.

- [ ] **Step 4: Add a minimal test bootstrap**

Create `tests/conftest.py`:

```python
from __future__ import annotations
```

- [ ] **Step 5: Verify tooling starts**

Run:

```bash
rtk uv run pytest -q
rtk uv run ruff check .
rtk uv run mypy app
```

Expected:

- `pytest`: exits 0 with no tests collected or all existing tests passing.
- `ruff`: exits 0.
- `mypy`: exits 0.

- [ ] **Step 6: Commit**

```bash
rtk git add pyproject.toml uv.lock app tests
rtk git commit -m "chore: scaffold project tooling"
```

### Task 2: Typed Settings

**Files:**

- Create: `app/settings.py`
- Test: `tests/unit/test_settings.py`

- [ ] **Step 1: Write failing settings tests**

Create `tests/unit/test_settings.py`:

```python
from __future__ import annotations

from uuid import UUID

import pytest

from app.settings import AppMode, Settings


def test_settings_load_required_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_MODE", "worker")
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@db:5432/app")
    monkeypatch.setenv("INTEGRATION_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("ZZAP_BASE_URL", "https://b52-api.zzap.pro")
    monkeypatch.setenv("ZZAP_API_KEY", "zzap-secret")
    monkeypatch.setenv("CHATWOOT_BASE_URL", "https://chatwoot.example.test")
    monkeypatch.setenv("CHATWOOT_ACCOUNT_ID", "1")
    monkeypatch.setenv("CHATWOOT_INBOX_ID", "2")
    monkeypatch.setenv("CHATWOOT_API_TOKEN", "chatwoot-secret")
    monkeypatch.setenv("CHATWOOT_WEBHOOK_SECRET", "webhook-secret")

    settings = Settings()

    assert settings.app_mode == AppMode.WORKER
    assert settings.database_url == "postgresql+asyncpg://user:pass@db:5432/app"
    assert settings.integration_id == UUID("11111111-1111-4111-8111-111111111111")
    assert settings.max_attachment_bytes == 10 * 1024 * 1024
    assert settings.successful_message_retention_days == 60
    assert settings.failed_record_retention_days == 30
    assert settings.webhook_delivery_retention_days == 30


def test_settings_reject_invalid_app_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_MODE", "invalid")
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@db:5432/app")
    monkeypatch.setenv("INTEGRATION_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("ZZAP_BASE_URL", "https://b52-api.zzap.pro")
    monkeypatch.setenv("ZZAP_API_KEY", "zzap-secret")
    monkeypatch.setenv("CHATWOOT_BASE_URL", "https://chatwoot.example.test")
    monkeypatch.setenv("CHATWOOT_ACCOUNT_ID", "1")
    monkeypatch.setenv("CHATWOOT_INBOX_ID", "2")
    monkeypatch.setenv("CHATWOOT_API_TOKEN", "chatwoot-secret")
    monkeypatch.setenv("CHATWOOT_WEBHOOK_SECRET", "webhook-secret")

    with pytest.raises(ValueError):
        Settings()
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
rtk uv run pytest tests/unit/test_settings.py -q
```

Expected: fail because `app.settings` does not exist.

- [ ] **Step 3: Implement settings**

Create `app/settings.py`:

```python
from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from uuid import UUID

from pydantic import AnyHttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppMode(StrEnum):
    WEB = "web"
    WORKER = "worker"
    ALL = "all"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    app_mode: AppMode = Field(default=AppMode.WEB, alias="APP_MODE")
    database_url: str = Field(alias="DATABASE_URL")
    integration_id: UUID = Field(alias="INTEGRATION_ID")

    zzap_base_url: AnyHttpUrl = Field(alias="ZZAP_BASE_URL")
    zzap_api_key: str = Field(alias="ZZAP_API_KEY")

    chatwoot_base_url: AnyHttpUrl = Field(alias="CHATWOOT_BASE_URL")
    chatwoot_account_id: int = Field(alias="CHATWOOT_ACCOUNT_ID")
    chatwoot_inbox_id: int = Field(alias="CHATWOOT_INBOX_ID")
    chatwoot_api_token: str = Field(alias="CHATWOOT_API_TOKEN")
    chatwoot_webhook_secret: str = Field(alias="CHATWOOT_WEBHOOK_SECRET")

    max_attachment_bytes: int = Field(default=10 * 1024 * 1024, alias="MAX_ATTACHMENT_BYTES")
    successful_message_retention_days: int = Field(
        default=60,
        alias="SUCCESSFUL_MESSAGE_RETENTION_DAYS",
    )
    failed_record_retention_days: int = Field(default=30, alias="FAILED_RECORD_RETENTION_DAYS")
    webhook_delivery_retention_days: int = Field(
        default=30,
        alias="WEBHOOK_DELIVERY_RETENTION_DAYS",
    )

    zzap_regular_timeout_seconds: float = Field(default=30.0, alias="ZZAP_TIMEOUT_SECONDS")
    chatwoot_regular_timeout_seconds: float = Field(default=30.0, alias="CHATWOOT_TIMEOUT_SECONDS")
    attachment_timeout_seconds: float = Field(default=60.0, alias="ATTACHMENT_TIMEOUT_SECONDS")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 4: Run settings tests**

Run:

```bash
rtk uv run pytest tests/unit/test_settings.py -q
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
rtk git add app/settings.py tests/unit/test_settings.py
rtk git commit -m "feat: add typed settings"
```

### Task 3: Database Models And Alembic

**Files:**

- Create: `app/db/base.py`
- Create: `app/db/models.py`
- Create: `app/db/session.py`
- Create: `alembic.ini`
- Create: `alembic/env.py`
- Create: `alembic/script.py.mako`
- Create: `alembic/versions/0001_initial_schema.py`

- [ ] **Step 1: Create SQLAlchemy base and enums**

Create `app/db/base.py`:

```python
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, MetaData, func
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class UUIDPrimaryKeyMixin:
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
```

- [ ] **Step 2: Create ORM models**

Create `app/db/models.py` with these model classes and enums:

```python
from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class MessageDirection(StrEnum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"


class MessageStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    IGNORED = "ignored"
    BLOCKED = "blocked"


class JobType(StrEnum):
    INBOUND_ZZAP_MESSAGE_TO_CHATWOOT = "inbound_zzap_message_to_chatwoot"
    OUTBOUND_CHATWOOT_MESSAGE_TO_ZZAP = "outbound_chatwoot_message_to_zzap"
    CHATWOOT_PRIVATE_NOTE = "chatwoot_private_note"


class JobStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    IGNORED = "ignored"
    BLOCKED = "blocked"


class ZZapThread(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "zzap_threads"
    __table_args__ = (
        UniqueConstraint("integration_id", "user_key", name="uq_zzap_threads_integration_user_key"),
        Index("ix_zzap_threads_integration_changed", "integration_id", "message_last_date"),
    )

    integration_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    user_key: Mapped[str] = mapped_column(String(512), nullable=False)
    user_name: Mapped[str | None] = mapped_column(String(512))
    message_last_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    message_last_hash: Mapped[str | None] = mapped_column(String(64))
    unread_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    read_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    cursor_message_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cursor_guard_fingerprint: Mapped[str | None] = mapped_column(String(64))
    last_polled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ChatwootContact(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "chatwoot_contacts"
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "zzap_user_key",
            name="uq_chatwoot_contacts_integration_zzap_user_key",
        ),
    )

    integration_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    zzap_user_key: Mapped[str] = mapped_column(String(512), nullable=False)
    chatwoot_contact_id: Mapped[int] = mapped_column(Integer, nullable=False)


class ChatwootConversation(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "chatwoot_conversations"
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "zzap_thread_id",
            name="uq_chatwoot_conversations_integration_thread",
        ),
        UniqueConstraint(
            "integration_id",
            "chatwoot_conversation_id",
            name="uq_chatwoot_conversations_integration_conversation",
        ),
    )

    integration_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    zzap_thread_id: Mapped[UUID] = mapped_column(ForeignKey("zzap_threads.id"), nullable=False)
    chatwoot_contact_id: Mapped[UUID] = mapped_column(ForeignKey("chatwoot_contacts.id"), nullable=False)
    chatwoot_conversation_id: Mapped[int] = mapped_column(Integer, nullable=False)


class MessageMapping(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "message_mappings"
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "fingerprint",
            name="uq_message_mappings_integration_fingerprint",
        ),
        Index("ix_message_mappings_cleanup", "status", "created_at"),
        Index("ix_message_mappings_chatwoot_message", "integration_id", "chatwoot_message_id"),
    )

    integration_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    direction: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    message_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    zzap_thread_id: Mapped[UUID | None] = mapped_column(ForeignKey("zzap_threads.id"))
    zzap_sender_user_key: Mapped[str | None] = mapped_column(String(512))
    zzap_message_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    chatwoot_message_id: Mapped[int | None] = mapped_column(Integer)
    chatwoot_conversation_id: Mapped[int | None] = mapped_column(Integer)
    is_cursor_guard: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class SyncJob(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "sync_jobs"
    __table_args__ = (
        Index("ix_sync_jobs_claim", "status", "next_attempt_at", "created_at"),
        Index("ix_sync_jobs_chatwoot_message", "integration_id", "chatwoot_message_id", "job_type"),
    )

    integration_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    job_type: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    locked_by: Mapped[str | None] = mapped_column(String(128))
    last_error: Mapped[str | None] = mapped_column(Text)
    zzap_thread_id: Mapped[UUID | None] = mapped_column(ForeignKey("zzap_threads.id"))
    chatwoot_conversation_id: Mapped[int | None] = mapped_column(Integer)
    chatwoot_message_id: Mapped[int | None] = mapped_column(Integer)
    message_mapping_id: Mapped[UUID | None] = mapped_column(ForeignKey("message_mappings.id"))
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)


class WebhookDelivery(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "delivery_id",
            name="uq_webhook_deliveries_integration_delivery",
        ),
        Index("ix_webhook_deliveries_cleanup", "created_at"),
    )

    integration_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    delivery_id: Mapped[str] = mapped_column(String(512), nullable=False)
    event_name: Mapped[str | None] = mapped_column(String(128))
    chatwoot_message_id: Mapped[int | None] = mapped_column(Integer)


class ServiceState(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "service_state"
    __table_args__ = (
        UniqueConstraint("integration_id", "key", name="uq_service_state_integration_key"),
    )

    integration_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    key: Mapped[str] = mapped_column(String(128), nullable=False)
    value: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
```

- [ ] **Step 3: Create async session module**

Create `app/db/session.py`:

```python
from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


def create_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def session_scope(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        async with session.begin():
            yield session
```

- [ ] **Step 4: Create Alembic files**

Create `alembic.ini`:

```ini
[alembic]
script_location = alembic
prepend_sys_path = .

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
```

Create `alembic/env.py`:

```python
from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.db.base import Base
from app.db import models
from app.settings import Settings

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    return Settings().database_url.replace("+asyncpg", "")


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
    connectable = engine_from_config(configuration, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

Create `alembic/script.py.mako` from Alembic's default template or run `rtk uv run alembic init alembic` before applying this task, then replace `env.py`.

- [ ] **Step 5: Create initial migration**

Create `alembic/versions/0001_initial_schema.py` by running:

```bash
rtk uv run alembic revision --autogenerate -m "initial schema"
```

Then inspect the generated migration and ensure it creates exactly the seven tables from the spec:

- `zzap_threads`
- `chatwoot_contacts`
- `chatwoot_conversations`
- `message_mappings`
- `sync_jobs`
- `webhook_deliveries`
- `service_state`

- [ ] **Step 6: Verify import and metadata**

Run:

```bash
rtk uv run python -c "from app.db.base import Base; from app.db import models; print(sorted(Base.metadata.tables))"
```

Expected output includes all seven table names.

- [ ] **Step 7: Commit**

```bash
rtk git add app/db alembic.ini alembic
rtk git commit -m "feat: add database schema"
```

### Task 4: Fingerprinting And Time Handling

**Files:**

- Create: `app/services/fingerprinting.py`
- Test: `tests/unit/test_fingerprinting.py`

- [ ] **Step 1: Write failing fingerprint tests**

Create `tests/unit/test_fingerprinting.py`:

```python
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.services.fingerprinting import (
    build_zzap_fingerprint,
    normalize_message_text,
    parse_zzap_datetime,
    sha256_hex,
)


def test_normalize_message_text_preserves_outer_whitespace() -> None:
    assert normalize_message_text("  hello\r\nworld  ") == "  hello\nworld  "


def test_normalize_message_text_uses_unicode_nfc() -> None:
    assert normalize_message_text("e\u0301") == "\u00e9"


def test_parse_zzap_datetime_assigns_moscow_timezone() -> None:
    parsed = parse_zzap_datetime("2025-04-29T21:06:45")
    assert parsed == datetime(2025, 4, 29, 21, 6, 45, tzinfo=ZoneInfo("Europe/Moscow"))


def test_build_zzap_fingerprint_is_stable() -> None:
    message_date = datetime(2025, 4, 29, 21, 6, 45, tzinfo=ZoneInfo("Europe/Moscow"))
    fingerprint = build_zzap_fingerprint(
        integration_id="11111111-1111-4111-8111-111111111111",
        thread_user_key="thread-key",
        sender_user_key="sender-key",
        message_date=message_date,
        message_text="hello\r\nworld",
    )

    assert len(fingerprint.message_hash) == 64
    assert len(fingerprint.fingerprint) == 64
    assert fingerprint.message_hash == sha256_hex("hello\nworld")
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
rtk uv run pytest tests/unit/test_fingerprinting.py -q
```

Expected: fail because `app.services.fingerprinting` does not exist.

- [ ] **Step 3: Implement fingerprinting**

Create `app/services/fingerprinting.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from unicodedata import normalize
from zoneinfo import ZoneInfo

MOSCOW_TZ = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True)
class MessageFingerprint:
    message_hash: str
    fingerprint: str


def normalize_message_text(value: str) -> str:
    return normalize("NFC", value.replace("\r\n", "\n").replace("\r", "\n"))


def sha256_hex(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def parse_zzap_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=MOSCOW_TZ)
    return parsed.astimezone(MOSCOW_TZ)


def build_zzap_fingerprint(
    *,
    integration_id: str,
    thread_user_key: str,
    sender_user_key: str,
    message_date: datetime,
    message_text: str,
) -> MessageFingerprint:
    normalized_text = normalize_message_text(message_text)
    message_hash = sha256_hex(normalized_text)
    fingerprint_source = "|".join(
        [
            integration_id,
            thread_user_key,
            sender_user_key,
            message_date.isoformat(),
            message_hash,
        ],
    )
    return MessageFingerprint(message_hash=message_hash, fingerprint=sha256_hex(fingerprint_source))
```

- [ ] **Step 4: Run tests**

Run:

```bash
rtk uv run pytest tests/unit/test_fingerprinting.py -q
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
rtk git add app/services/fingerprinting.py tests/unit/test_fingerprinting.py
rtk git commit -m "feat: add message fingerprinting"
```

### Task 5: Webhook HMAC Verification

**Files:**

- Create: `app/services/webhooks.py`
- Test: `tests/unit/test_webhook_security.py`

- [ ] **Step 1: Write failing HMAC tests**

Create `tests/unit/test_webhook_security.py`:

```python
from __future__ import annotations

import hmac
from hashlib import sha256

import pytest

from app.services.webhooks import WebhookSignatureError, verify_chatwoot_signature


def _signature(secret: str, timestamp: str, body: bytes) -> str:
    digest = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, sha256).hexdigest()
    return f"sha256={digest}"


def test_verify_chatwoot_signature_accepts_valid_signature() -> None:
    body = b'{"event":"message_created"}'
    timestamp = "1000"
    secret = "secret"

    verify_chatwoot_signature(
        raw_body=body,
        timestamp=timestamp,
        signature=_signature(secret, timestamp, body),
        secret=secret,
        now_seconds=1100,
        tolerance_seconds=300,
    )


def test_verify_chatwoot_signature_rejects_invalid_signature() -> None:
    with pytest.raises(WebhookSignatureError):
        verify_chatwoot_signature(
            raw_body=b"{}",
            timestamp="1000",
            signature="sha256=bad",
            secret="secret",
            now_seconds=1100,
            tolerance_seconds=300,
        )


def test_verify_chatwoot_signature_rejects_old_timestamp() -> None:
    body = b"{}"
    timestamp = "1000"
    secret = "secret"

    with pytest.raises(WebhookSignatureError):
        verify_chatwoot_signature(
            raw_body=body,
            timestamp=timestamp,
            signature=_signature(secret, timestamp, body),
            secret=secret,
            now_seconds=2000,
            tolerance_seconds=300,
        )
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
rtk uv run pytest tests/unit/test_webhook_security.py -q
```

Expected: fail because `app.services.webhooks` does not exist.

- [ ] **Step 3: Implement HMAC verification**

Create `app/services/webhooks.py`:

```python
from __future__ import annotations

import hmac
from hashlib import sha256


class WebhookSignatureError(ValueError):
    pass


def verify_chatwoot_signature(
    *,
    raw_body: bytes,
    timestamp: str | None,
    signature: str | None,
    secret: str,
    now_seconds: int,
    tolerance_seconds: int = 300,
) -> None:
    if not timestamp or not signature:
        raise WebhookSignatureError("missing signature headers")

    try:
        timestamp_int = int(timestamp)
    except ValueError as exc:
        raise WebhookSignatureError("invalid timestamp") from exc

    if abs(now_seconds - timestamp_int) > tolerance_seconds:
        raise WebhookSignatureError("timestamp outside tolerance")

    expected_digest = hmac.new(
        secret.encode("utf-8"),
        f"{timestamp}.".encode("utf-8") + raw_body,
        sha256,
    ).hexdigest()
    expected = f"sha256={expected_digest}"

    if not hmac.compare_digest(expected, signature):
        raise WebhookSignatureError("invalid signature")
```

- [ ] **Step 4: Run tests**

Run:

```bash
rtk uv run pytest tests/unit/test_webhook_security.py -q
```

Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
rtk git add app/services/webhooks.py tests/unit/test_webhook_security.py
rtk git commit -m "feat: verify chatwoot webhooks"
```

### Task 6: Job Repository And Atomic Claim

**Files:**

- Create: `app/db/repositories.py`
- Test: `tests/unit/test_job_claiming.py`

- [ ] **Step 1: Write repository unit tests around SQL shape**

Create `tests/unit/test_job_claiming.py`:

```python
from __future__ import annotations

from app.db.repositories import build_claim_job_statement


def test_claim_job_statement_uses_skip_locked() -> None:
    statement = build_claim_job_statement(worker_id="worker-1")
    compiled = str(statement.compile(compile_kwargs={"literal_binds": True}))

    assert "FOR UPDATE" in compiled
    assert "SKIP LOCKED" in compiled
    assert "sync_jobs" in compiled
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
rtk uv run pytest tests/unit/test_job_claiming.py -q
```

Expected: fail because `build_claim_job_statement` does not exist.

- [ ] **Step 3: Implement repository SQL helpers**

Create `app/db/repositories.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import JobStatus, SyncJob


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


def build_claim_job_statement(*, worker_id: str) -> Select[tuple[SyncJob]]:
    return (
        select(SyncJob)
        .where(SyncJob.status == JobStatus.PENDING.value)
        .where((SyncJob.next_attempt_at.is_(None)) | (SyncJob.next_attempt_at <= utcnow()))
        .order_by(SyncJob.next_attempt_at.asc().nullsfirst(), SyncJob.created_at.asc())
        .limit(1)
        .with_for_update(skip_locked=True)
    )


async def claim_next_job(session: AsyncSession, *, worker_id: str) -> SyncJob | None:
    result = await session.execute(build_claim_job_statement(worker_id=worker_id))
    job = result.scalar_one_or_none()
    if job is None:
        return None
    job.status = JobStatus.PROCESSING.value
    job.locked_at = utcnow()
    job.locked_by = worker_id
    job.attempt_count += 1
    return job


async def reset_stale_processing_jobs(
    session: AsyncSession,
    *,
    older_than: timedelta,
    now: datetime | None = None,
) -> int:
    current_time = now or utcnow()
    cutoff = current_time - older_than
    result = await session.execute(
        select(SyncJob).where(
            SyncJob.status == JobStatus.PROCESSING.value,
            SyncJob.locked_at < cutoff,
        ),
    )
    jobs = list(result.scalars())
    for job in jobs:
        job.status = JobStatus.PENDING.value
        job.locked_at = None
        job.locked_by = None
    return len(jobs)
```

- [ ] **Step 4: Run tests**

Run:

```bash
rtk uv run pytest tests/unit/test_job_claiming.py -q
```

Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
rtk git add app/db/repositories.py tests/unit/test_job_claiming.py
rtk git commit -m "feat: add atomic job claiming"
```

### Task 7: Attachment Rules

**Files:**

- Create: `app/services/attachments.py`
- Test: `tests/unit/test_attachments.py`

- [ ] **Step 1: Write failing attachment tests**

Create `tests/unit/test_attachments.py`:

```python
from __future__ import annotations

import pytest

from app.services.attachments import AttachmentTooLargeError, ensure_attachment_size


def test_ensure_attachment_size_accepts_limit_boundary() -> None:
    ensure_attachment_size(size_bytes=10, max_bytes=10)


def test_ensure_attachment_size_rejects_large_file() -> None:
    with pytest.raises(AttachmentTooLargeError):
        ensure_attachment_size(size_bytes=11, max_bytes=10)
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
rtk uv run pytest tests/unit/test_attachments.py -q
```

Expected: fail because `app.services.attachments` does not exist.

- [ ] **Step 3: Implement attachment checks**

Create `app/services/attachments.py`:

```python
from __future__ import annotations


class AttachmentTooLargeError(ValueError):
    pass


def ensure_attachment_size(*, size_bytes: int, max_bytes: int) -> None:
    if size_bytes > max_bytes:
        raise AttachmentTooLargeError(
            f"attachment is {size_bytes} bytes, max allowed is {max_bytes} bytes",
        )
```

- [ ] **Step 4: Run tests**

Run:

```bash
rtk uv run pytest tests/unit/test_attachments.py -q
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
rtk git add app/services/attachments.py tests/unit/test_attachments.py
rtk git commit -m "feat: add attachment validation"
```

### Task 8: ZZap And Chatwoot HTTP Clients

**Files:**

- Create: `app/clients/zzap.py`
- Create: `app/clients/chatwoot.py`
- Test: `tests/unit/test_zzap_client.py`
- Test: `tests/unit/test_chatwoot_client.py`

- [ ] **Step 1: Write ZZap client tests with httpx MockTransport**

Create `tests/unit/test_zzap_client.py`:

```python
from __future__ import annotations

import httpx
import pytest

from app.clients.zzap import ZZapClient


@pytest.mark.asyncio
async def test_zzap_client_sends_api_key() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["zzap-api-key"] == "secret"
        return httpx.Response(200, json={"success": True, "result": {"data": []}, "result_info": {}})

    client = ZZapClient(
        base_url="https://zzap.example.test",
        api_key="secret",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    result = await client.list_threads(page=1, page_size=100)

    assert result == []
```

- [ ] **Step 2: Write Chatwoot client tests with httpx MockTransport**

Create `tests/unit/test_chatwoot_client.py`:

```python
from __future__ import annotations

import httpx
import pytest

from app.clients.chatwoot import ChatwootClient


@pytest.mark.asyncio
async def test_chatwoot_client_creates_private_note() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["api_access_token"] == "token"
        assert request.url.path == "/api/v1/accounts/1/conversations/2/messages"
        payload = await request.aread()
        assert b"private" in payload
        return httpx.Response(200, json={"id": 10})

    client = ChatwootClient(
        base_url="https://chatwoot.example.test",
        account_id=1,
        api_token="token",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    message_id = await client.create_private_note(conversation_id=2, content="failed")

    assert message_id == 10
```

- [ ] **Step 3: Run tests and verify they fail**

Run:

```bash
rtk uv run pytest tests/unit/test_zzap_client.py tests/unit/test_chatwoot_client.py -q
```

Expected: fail because client modules do not exist.

- [ ] **Step 4: Implement clients**

Create `app/clients/zzap.py` with methods:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx


@dataclass(frozen=True)
class ZZapThreadDto:
    user_key: str
    user_name: str | None
    unread_count: int
    message_last_date: str | None
    message_last: str | None
    read_only: bool


@dataclass(frozen=True)
class ZZapMessageDto:
    user_key: str | None
    user_name: str | None
    message_date: str | None
    message: str | None
    unread: bool | None


class ZZapApiError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


class ZZapClient:
    def __init__(self, *, base_url: str, api_key: str, http_client: httpx.AsyncClient) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._http = http_client

    async def list_threads(self, *, page: int, page_size: int) -> list[ZZapThreadDto]:
        payload = await self._request_json(
            "GET",
            "/api/client/v1/messages",
            params={"page": page, "page_size": page_size},
        )
        return [
            ZZapThreadDto(
                user_key=item.get("user_key") or "",
                user_name=item.get("user_name"),
                unread_count=item.get("unread_count") or 0,
                message_last_date=item.get("message_last_date"),
                message_last=item.get("message_last"),
                read_only=bool(item.get("read_only")),
            )
            for item in payload.get("result", {}).get("data") or []
            if item.get("user_key")
        ]

    async def list_messages(
        self,
        *,
        user_key: str,
        page: int,
        page_size: int,
    ) -> list[ZZapMessageDto]:
        payload = await self._request_json(
            "GET",
            f"/api/client/v1/messages/{user_key}",
            params={"page": page, "page_size": page_size},
        )
        return [
            ZZapMessageDto(
                user_key=item.get("user_key"),
                user_name=item.get("user_name"),
                message_date=item.get("message_date"),
                message=item.get("message"),
                unread=item.get("unread"),
            )
            for item in payload.get("result", {}).get("data") or []
        ]

    async def upload_file(self, *, file_name: str, file_body_base64: str, upload_type: int = 1) -> str:
        payload = await self._request_json(
            "POST",
            "/api/client/v1/upload",
            json={"file_name": file_name, "file_body": file_body_base64, "upload_type": upload_type},
        )
        file_url = payload.get("result", {}).get("file_url")
        if not file_url:
            raise ZZapApiError(200, "ZZap upload response did not include file_url")
        return str(file_url)

    async def send_message(
        self,
        *,
        user_key: str,
        message: str,
        message_date: datetime,
        is_online: bool,
    ) -> None:
        await self._request_json(
            "POST",
            "/api/client/v1/messages",
            json={
                "user_key": user_key,
                "message": message,
                "message_date": message_date.isoformat(),
                "is_online": is_online,
            },
        )

    async def _request_json(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = await self._http.request(
            method,
            f"{self._base_url}{path}",
            headers={"zzap-api-key": self._api_key},
            **kwargs,
        )
        if response.status_code >= 400:
            raise ZZapApiError(response.status_code, response.text)
        payload = response.json()
        if payload.get("success") is False:
            raise ZZapApiError(int(payload.get("code") or response.status_code), str(payload.get("errors")))
        return payload
```

Create `app/clients/chatwoot.py` with methods for contact, conversation, messages, private notes, and attachment download:

```python
from __future__ import annotations

from typing import Any

import httpx


class ChatwootApiError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


class ChatwootClient:
    def __init__(self, *, base_url: str, account_id: int, api_token: str, http_client: httpx.AsyncClient) -> None:
        self._base_url = base_url.rstrip("/")
        self._account_id = account_id
        self._api_token = api_token
        self._http = http_client

    async def create_contact(self, *, inbox_id: int, name: str, custom_attributes: dict[str, str]) -> int:
        payload = await self._request_json(
            "POST",
            "/contacts",
            json={"inbox_id": inbox_id, "name": name, "custom_attributes": custom_attributes},
        )
        return int(payload["payload"]["contact"]["id"] if "payload" in payload else payload["id"])

    async def create_conversation(self, *, inbox_id: int, contact_id: int, status: str = "open") -> int:
        payload = await self._request_json(
            "POST",
            "/conversations",
            json={"inbox_id": inbox_id, "contact_id": contact_id, "status": status},
        )
        return int(payload["id"])

    async def update_conversation_status(self, *, conversation_id: int, status: str) -> None:
        await self._request_json("POST", f"/conversations/{conversation_id}/toggle_status", json={"status": status})

    async def create_incoming_message(self, *, conversation_id: int, content: str) -> int:
        payload = await self._request_json(
            "POST",
            f"/conversations/{conversation_id}/messages",
            json={"content": content, "message_type": "incoming"},
        )
        return int(payload["id"])

    async def create_private_note(self, *, conversation_id: int, content: str) -> int:
        payload = await self._request_json(
            "POST",
            f"/conversations/{conversation_id}/messages",
            json={"content": content, "private": True},
        )
        return int(payload["id"])

    async def download_attachment(self, url: str) -> bytes:
        response = await self._http.get(url, headers=self._headers())
        if response.status_code >= 400:
            raise ChatwootApiError(response.status_code, response.text)
        return response.content

    async def _request_json(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = await self._http.request(
            method,
            f"{self._base_url}/api/v1/accounts/{self._account_id}{path}",
            headers=self._headers(),
            **kwargs,
        )
        if response.status_code >= 400:
            raise ChatwootApiError(response.status_code, response.text)
        return response.json()

    def _headers(self) -> dict[str, str]:
        return {"api_access_token": self._api_token}
```

- [ ] **Step 5: Run client tests**

Run:

```bash
rtk uv run pytest tests/unit/test_zzap_client.py tests/unit/test_chatwoot_client.py -q
```

Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
rtk git add app/clients tests/unit/test_zzap_client.py tests/unit/test_chatwoot_client.py
rtk git commit -m "feat: add external api clients"
```

### Task 9: Chatwoot Webhook Classification Service

**Files:**

- Create: `app/services/outbound.py`
- Test: `tests/unit/test_webhook_service.py`

- [ ] **Step 1: Write webhook service tests**

Create `tests/unit/test_webhook_service.py`:

```python
from __future__ import annotations

from app.services.outbound import ChatwootWebhookDecision, classify_chatwoot_message_created


def test_classify_ignores_wrong_event() -> None:
    decision = classify_chatwoot_message_created(
        payload={"event": "conversation_updated"},
        expected_inbox_id=2,
    )
    assert decision == ChatwootWebhookDecision.IGNORE


def test_classify_accepts_public_outgoing_operator_message() -> None:
    decision = classify_chatwoot_message_created(
        payload={
            "event": "message_created",
            "id": 10,
            "message_type": "outgoing",
            "private": False,
            "conversation": {"id": 20, "inbox_id": 2},
            "sender": {"type": "user"},
        },
        expected_inbox_id=2,
    )
    assert decision == ChatwootWebhookDecision.ACCEPT


def test_classify_ignores_private_note() -> None:
    decision = classify_chatwoot_message_created(
        payload={
            "event": "message_created",
            "id": 10,
            "message_type": "outgoing",
            "private": True,
            "conversation": {"id": 20, "inbox_id": 2},
            "sender": {"type": "user"},
        },
        expected_inbox_id=2,
    )
    assert decision == ChatwootWebhookDecision.IGNORE
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
rtk uv run pytest tests/unit/test_webhook_service.py -q
```

Expected: fail because outbound service does not exist.

- [ ] **Step 3: Implement webhook classification**

Create `app/services/outbound.py` with the classification function:

```python
from __future__ import annotations

from enum import StrEnum
from typing import Any


class ChatwootWebhookDecision(StrEnum):
    ACCEPT = "accept"
    IGNORE = "ignore"


def classify_chatwoot_message_created(
    *,
    payload: dict[str, Any],
    expected_inbox_id: int,
) -> ChatwootWebhookDecision:
    if payload.get("event") != "message_created":
        return ChatwootWebhookDecision.IGNORE
    if payload.get("message_type") != "outgoing":
        return ChatwootWebhookDecision.IGNORE
    if payload.get("private") is True:
        return ChatwootWebhookDecision.IGNORE

    conversation = payload.get("conversation") or {}
    if int(conversation.get("inbox_id") or 0) != expected_inbox_id:
        return ChatwootWebhookDecision.IGNORE

    sender = payload.get("sender") or {}
    if sender.get("type") not in {"user", None}:
        return ChatwootWebhookDecision.IGNORE

    return ChatwootWebhookDecision.ACCEPT
```

- [ ] **Step 4: Run webhook service tests**

Run:

```bash
rtk uv run pytest tests/unit/test_webhook_security.py tests/unit/test_webhook_service.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
rtk git add app/services/outbound.py tests/unit/test_webhook_service.py
rtk git commit -m "feat: classify chatwoot webhooks"
```

### Task 10: Litestar App Factory, Health, And Readiness

**Files:**

- Create: `app/api/health.py`
- Create: `app/asgi.py`
- Test: `tests/unit/test_health.py`

- [ ] **Step 1: Write health tests**

Create `tests/unit/test_health.py`:

```python
from __future__ import annotations

from litestar.testing import TestClient

from app.asgi import create_app


def test_health_returns_ok() -> None:
    with TestClient(create_app()) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
rtk uv run pytest tests/unit/test_health.py -q
```

Expected: fail because `app.asgi` does not exist.

- [ ] **Step 3: Implement health and app factory**

Create `app/api/health.py`:

```python
from __future__ import annotations

from litestar import get


@get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@get("/ready")
async def ready() -> dict[str, str]:
    return {"status": "ready"}
```

Create `app/asgi.py`:

```python
from __future__ import annotations

from litestar import Litestar

from app.api.health import health, ready
from app.settings import Settings, get_settings


def create_app() -> Litestar:
    settings = get_settings()
    return Litestar(
        route_handlers=[health, ready],
        dependencies={"settings": lambda: settings},
    )


app = create_app()
```

- [ ] **Step 4: Run health test**

Run:

```bash
rtk uv run pytest tests/unit/test_health.py -q
```

Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
rtk git add app/api/health.py app/asgi.py tests/unit/test_health.py
rtk git commit -m "feat: add litestar app factory"
```

### Task 11: ZZap Rate Limiter And Polling Coalescing

**Files:**

- Create: `app/workers/rate_limit.py`
- Create: `app/workers/zzap_scheduler.py`
- Test: `tests/unit/test_rate_limit.py`
- Test: `tests/unit/test_zzap_scheduler.py`

- [ ] **Step 1: Write rate limiter tests**

Create `tests/unit/test_rate_limit.py`:

```python
from __future__ import annotations

from app.workers.rate_limit import ZZapRateLimiter


def test_rate_limiter_first_request_is_ready() -> None:
    limiter = ZZapRateLimiter(interval_seconds=3.0)
    assert limiter.delay_until_next(now=10.0) == 0.0


def test_rate_limiter_waits_between_requests() -> None:
    limiter = ZZapRateLimiter(interval_seconds=3.0)
    limiter.mark_request_started(now=10.0)
    assert limiter.delay_until_next(now=11.0) == 2.0
```

- [ ] **Step 2: Write scheduler coalescing tests**

Create `tests/unit/test_zzap_scheduler.py`:

```python
from __future__ import annotations

from app.workers.zzap_scheduler import ZZapActionQueue


def test_summary_poll_is_coalesced() -> None:
    queue = ZZapActionQueue()
    queue.enqueue_summary_poll()
    queue.enqueue_summary_poll()

    assert queue.size() == 1


def test_thread_fetch_is_coalesced_per_thread() -> None:
    queue = ZZapActionQueue()
    queue.enqueue_thread_fetch("a")
    queue.enqueue_thread_fetch("a")
    queue.enqueue_thread_fetch("b")

    assert queue.size() == 2
```

- [ ] **Step 3: Run tests and verify they fail**

Run:

```bash
rtk uv run pytest tests/unit/test_rate_limit.py tests/unit/test_zzap_scheduler.py -q
```

Expected: fail because worker modules do not exist.

- [ ] **Step 4: Implement limiter and queue**

Create `app/workers/rate_limit.py`:

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ZZapRateLimiter:
    interval_seconds: float
    last_request_started_at: float | None = None

    def delay_until_next(self, *, now: float) -> float:
        if self.last_request_started_at is None:
            return 0.0
        elapsed = now - self.last_request_started_at
        return max(0.0, self.interval_seconds - elapsed)

    def mark_request_started(self, *, now: float) -> None:
        self.last_request_started_at = now
```

Create `app/workers/zzap_scheduler.py`:

```python
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import StrEnum


class ZZapActionType(StrEnum):
    SUMMARY_POLL = "summary_poll"
    THREAD_FETCH = "thread_fetch"


@dataclass(frozen=True)
class ZZapAction:
    action_type: ZZapActionType
    thread_user_key: str | None = None


class ZZapActionQueue:
    def __init__(self) -> None:
        self._queue: deque[ZZapAction] = deque()
        self._pending_summary = False
        self._pending_thread_fetches: set[str] = set()

    def enqueue_summary_poll(self) -> None:
        if self._pending_summary:
            return
        self._pending_summary = True
        self._queue.append(ZZapAction(ZZapActionType.SUMMARY_POLL))

    def enqueue_thread_fetch(self, thread_user_key: str) -> None:
        if thread_user_key in self._pending_thread_fetches:
            return
        self._pending_thread_fetches.add(thread_user_key)
        self._queue.append(ZZapAction(ZZapActionType.THREAD_FETCH, thread_user_key))

    def pop_next(self) -> ZZapAction | None:
        if not self._queue:
            return None
        action = self._queue.popleft()
        if action.action_type == ZZapActionType.SUMMARY_POLL:
            self._pending_summary = False
        if action.action_type == ZZapActionType.THREAD_FETCH and action.thread_user_key:
            self._pending_thread_fetches.discard(action.thread_user_key)
        return action

    def size(self) -> int:
        return len(self._queue)
```

- [ ] **Step 5: Run scheduler tests**

Run:

```bash
rtk uv run pytest tests/unit/test_rate_limit.py tests/unit/test_zzap_scheduler.py -q
```

Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
rtk git add app/workers/rate_limit.py app/workers/zzap_scheduler.py tests/unit/test_rate_limit.py tests/unit/test_zzap_scheduler.py
rtk git commit -m "feat: add zzap scheduling primitives"
```

### Task 12: Inbound ZZap To Chatwoot Pipeline

**Files:**

- Create: `app/services/inbound.py`
- Modify: `app/db/repositories.py`
- Test: `tests/unit/test_inbound_service.py`

- [ ] **Step 1: Write inbound candidate tests**

Create `tests/unit/test_inbound_service.py`:

```python
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.services.inbound import should_import_zzap_message


def test_should_import_message_newer_than_cursor() -> None:
    cursor = datetime(2025, 1, 1, 10, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))
    message_date = datetime(2025, 1, 1, 10, 0, 1, tzinfo=ZoneInfo("Europe/Moscow"))

    assert should_import_zzap_message(
        message_date=message_date,
        cursor_message_date=cursor,
        fingerprint="new",
        known_fingerprints={"old"},
        cursor_guard_fingerprint="old",
    )


def test_should_not_import_older_message_even_if_fingerprint_missing() -> None:
    cursor = datetime(2025, 1, 1, 10, 0, 1, tzinfo=ZoneInfo("Europe/Moscow"))
    message_date = datetime(2025, 1, 1, 10, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))

    assert not should_import_zzap_message(
        message_date=message_date,
        cursor_message_date=cursor,
        fingerprint="missing-after-retention",
        known_fingerprints=set(),
        cursor_guard_fingerprint="old",
    )


def test_should_not_import_cursor_guard_duplicate() -> None:
    cursor = datetime(2025, 1, 1, 10, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))

    assert not should_import_zzap_message(
        message_date=cursor,
        cursor_message_date=cursor,
        fingerprint="guard",
        known_fingerprints=set(),
        cursor_guard_fingerprint="guard",
    )
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
rtk uv run pytest tests/unit/test_inbound_service.py -q
```

Expected: fail because `app.services.inbound` does not exist.

- [ ] **Step 3: Implement inbound cursor decision**

Create `app/services/inbound.py`:

```python
from __future__ import annotations

from datetime import datetime


def should_import_zzap_message(
    *,
    message_date: datetime,
    cursor_message_date: datetime | None,
    fingerprint: str,
    known_fingerprints: set[str],
    cursor_guard_fingerprint: str | None,
) -> bool:
    if fingerprint in known_fingerprints:
        return False
    if cursor_message_date is None:
        return fingerprint != cursor_guard_fingerprint
    if message_date > cursor_message_date:
        return True
    if message_date == cursor_message_date:
        return fingerprint != cursor_guard_fingerprint
    return False
```

- [ ] **Step 4: Add inbound repository operations**

Extend `app/db/repositories.py` with functions that:

- upsert `zzap_threads` by `(integration_id, user_key)`;
- insert `message_mappings` idempotently by `(integration_id, fingerprint)`;
- create `sync_jobs` for `inbound_zzap_message_to_chatwoot`;
- advance `zzap_threads.cursor_message_date` and `cursor_guard_fingerprint` only after mappings/jobs are persisted.

Use SQLAlchemy transactions from the caller; do not commit inside repository functions.

- [ ] **Step 5: Implement inbound job processor**

Extend `app/services/inbound.py` with `InboundProcessor` that:

- gets or creates a Chatwoot contact mapping;
- gets or creates a Chatwoot conversation mapping;
- reopens conversation as `open`;
- creates a Chatwoot incoming message;
- updates `message_mappings.chatwoot_message_id`;
- clears temporary job payload after success.

- [ ] **Step 6: Run inbound tests**

Run:

```bash
rtk uv run pytest tests/unit/test_inbound_service.py -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
rtk git add app/services/inbound.py app/db/repositories.py tests/unit/test_inbound_service.py
rtk git commit -m "feat: add inbound sync pipeline"
```

### Task 13: Outbound Chatwoot To ZZap Pipeline

**Files:**

- Create: `app/api/webhooks.py`
- Modify: `app/asgi.py`
- Modify: `app/services/outbound.py`
- Modify: `app/db/repositories.py`
- Test: `tests/unit/test_outbound_service.py`

- [ ] **Step 1: Write outbound formatting tests**

Create `tests/unit/test_outbound_service.py`:

```python
from __future__ import annotations

from app.services.outbound import build_zzap_outbound_message


def test_build_zzap_outbound_message_text_and_links() -> None:
    assert build_zzap_outbound_message(
        content="hello",
        uploaded_file_urls=["https://files.example/a.png", "https://files.example/b.pdf"],
    ) == "hello\n\nhttps://files.example/a.png\nhttps://files.example/b.pdf"


def test_build_zzap_outbound_message_links_only() -> None:
    assert build_zzap_outbound_message(
        content="",
        uploaded_file_urls=["https://files.example/a.png"],
    ) == "https://files.example/a.png"
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
rtk uv run pytest tests/unit/test_outbound_service.py -q
```

Expected: fail because `build_zzap_outbound_message` does not exist.

- [ ] **Step 3: Implement outbound formatting**

Add to `app/services/outbound.py`:

```python
def build_zzap_outbound_message(*, content: str, uploaded_file_urls: list[str]) -> str:
    stripped_content = content.strip()
    links = "\n".join(uploaded_file_urls)
    if stripped_content and links:
        return f"{stripped_content}\n\n{links}"
    if links:
        return links
    return stripped_content
```

- [ ] **Step 4: Implement outbound job persistence from webhook**

Create `app/api/webhooks.py`:

```python
from __future__ import annotations

import time

from litestar import Controller, Request, Response, post
from litestar.exceptions import HTTPException
from litestar.status_codes import HTTP_200_OK, HTTP_403_FORBIDDEN, HTTP_500_INTERNAL_SERVER_ERROR

from app.services.outbound import (
    ChatwootWebhookDecision,
    classify_chatwoot_message_created,
    persist_outbound_webhook_event,
)
from app.services.webhooks import WebhookSignatureError, verify_chatwoot_signature
from app.settings import Settings


class ChatwootWebhookController(Controller):
    path = "/webhooks/chatwoot"

    @post()
    async def receive(self, request: Request, settings: Settings) -> Response[dict[str, str]]:
        raw_body = await request.body()
        try:
            verify_chatwoot_signature(
                raw_body=raw_body,
                timestamp=request.headers.get("X-Chatwoot-Timestamp"),
                signature=request.headers.get("X-Chatwoot-Signature"),
                secret=settings.chatwoot_webhook_secret,
                now_seconds=int(time.time()),
            )
        except WebhookSignatureError as exc:
            raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="invalid signature") from exc

        payload = await request.json()
        decision = classify_chatwoot_message_created(
            payload=payload,
            expected_inbox_id=settings.chatwoot_inbox_id,
        )
        if decision == ChatwootWebhookDecision.IGNORE:
            return Response({"status": "ignored"}, status_code=HTTP_200_OK)

        try:
            created = await persist_outbound_webhook_event(
                payload=payload,
                delivery_id=request.headers.get("X-Chatwoot-Delivery"),
                settings=settings,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                detail="failed to persist outbound job",
            ) from exc

        return Response({"status": "accepted" if created else "duplicate"}, status_code=HTTP_200_OK)
```

Add `persist_outbound_webhook_event` to `app/services/outbound.py`. It must:

- records `X-Chatwoot-Delivery` in `webhook_deliveries` if present;
- deduplicates by `chatwoot_message_id`;
- checks the Chatwoot conversation mapping exists;
- creates one `outbound_chatwoot_message_to_zzap` job;
- returns `200 OK` for duplicates or ignored events;
- returns `500` if a relevant event cannot be persisted.

Modify `app/asgi.py` to register `ChatwootWebhookController`:

```python
from app.api.webhooks import ChatwootWebhookController
```

and include it:

```python
route_handlers=[health, ready, ChatwootWebhookController]
```

- [ ] **Step 5: Implement outbound job processor**

Extend `app/services/outbound.py` with `OutboundProcessor` that:

- loads the mapped ZZap thread;
- if `read_only=true`, marks job `blocked` and creates a `chatwoot_private_note` job;
- downloads Chatwoot attachments;
- validates each attachment size against `MAX_ATTACHMENT_BYTES`;
- base64 encodes the file body;
- uploads files to ZZap through the shared ZZap client path;
- stores successful upload URLs in job payload for retry reuse;
- sends one ZZap message with `is_online=true`;
- uses Chatwoot message creation time as ZZap `message_date`;
- clears temporary payload on success.

- [ ] **Step 6: Run outbound tests**

Run:

```bash
rtk uv run pytest tests/unit/test_webhook_service.py tests/unit/test_outbound_service.py -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
rtk git add app/services/outbound.py app/api/webhooks.py app/asgi.py app/db/repositories.py tests/unit/test_outbound_service.py
rtk git commit -m "feat: add outbound sync pipeline"
```

### Task 14: Job Runner, Retry, Private Notes, Cleanup, And Advisory Lock

**Files:**

- Create: `app/workers/jobs.py`
- Create: `app/workers/locks.py`
- Create: `app/workers/cleanup.py`
- Test: `tests/unit/test_job_retry.py`

- [ ] **Step 1: Write retry schedule tests**

Create `tests/unit/test_job_retry.py`:

```python
from __future__ import annotations

from datetime import timedelta

from app.workers.jobs import retry_delay_for_attempt


def test_outbound_retry_schedule() -> None:
    assert retry_delay_for_attempt(direction="outbound", attempt_count=1) == timedelta(minutes=1)
    assert retry_delay_for_attempt(direction="outbound", attempt_count=2) == timedelta(minutes=5)
    assert retry_delay_for_attempt(direction="outbound", attempt_count=3) == timedelta(minutes=15)


def test_inbound_retry_schedule() -> None:
    assert retry_delay_for_attempt(direction="inbound", attempt_count=1) == timedelta(seconds=10)
    assert retry_delay_for_attempt(direction="inbound", attempt_count=2) == timedelta(seconds=30)
    assert retry_delay_for_attempt(direction="inbound", attempt_count=3) == timedelta(minutes=1)
    assert retry_delay_for_attempt(direction="inbound", attempt_count=4) == timedelta(minutes=5)
    assert retry_delay_for_attempt(direction="inbound", attempt_count=5) == timedelta(minutes=15)
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
rtk uv run pytest tests/unit/test_job_retry.py -q
```

Expected: fail because `app.workers.jobs` does not exist.

- [ ] **Step 3: Implement retry helpers**

Create `app/workers/jobs.py` with retry helpers:

```python
from __future__ import annotations

from datetime import timedelta

OUTBOUND_RETRY_DELAYS = [timedelta(minutes=1), timedelta(minutes=5), timedelta(minutes=15)]
INBOUND_RETRY_DELAYS = [
    timedelta(seconds=10),
    timedelta(seconds=30),
    timedelta(minutes=1),
    timedelta(minutes=5),
    timedelta(minutes=15),
]


def retry_delay_for_attempt(*, direction: str, attempt_count: int) -> timedelta | None:
    delays = OUTBOUND_RETRY_DELAYS if direction == "outbound" else INBOUND_RETRY_DELAYS
    index = attempt_count - 1
    if index < 0 or index >= len(delays):
        return None
    return delays[index]
```

- [ ] **Step 4: Implement advisory lock helper**

Create `app/workers/locks.py` with:

```python
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


ADVISORY_LOCK_KEY = 721_202_607_003


async def try_worker_advisory_lock(session: AsyncSession) -> bool:
    result = await session.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": ADVISORY_LOCK_KEY})
    return bool(result.scalar_one())


async def release_worker_advisory_lock(session: AsyncSession) -> None:
    await session.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": ADVISORY_LOCK_KEY})
```

- [ ] **Step 5: Implement cleanup worker function**

Create `app/workers/cleanup.py` with functions that delete:

- successful non-cursor-guard `message_mappings` older than retention;
- failed jobs/message records older than retention;
- `webhook_deliveries` older than retention.

Do not delete `message_mappings.is_cursor_guard=true`.

- [ ] **Step 6: Run retry tests**

Run:

```bash
rtk uv run pytest tests/unit/test_job_retry.py -q
```

Expected: 2 passed.

- [ ] **Step 7: Commit**

```bash
rtk git add app/workers tests/unit/test_job_retry.py
rtk git commit -m "feat: add worker retry primitives"
```

### Task 15: Wire Worker Loop And Readiness State

**Files:**

- Create: `app/cli.py`
- Create: `app/services/readiness.py`
- Modify: `app/workers/jobs.py`
- Modify: `app/workers/zzap_scheduler.py`
- Modify: `app/api/health.py`

- [ ] **Step 1: Implement readiness service**

Create `app/services/readiness.py`:

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReadinessResult:
    ready: bool
    reason: str


def evaluate_readiness(*, database_ok: bool, zzap_auth_failed: bool, chatwoot_auth_failed: bool) -> ReadinessResult:
    if not database_ok:
        return ReadinessResult(ready=False, reason="database_unavailable")
    if zzap_auth_failed:
        return ReadinessResult(ready=False, reason="zzap_auth_failed")
    if chatwoot_auth_failed:
        return ReadinessResult(ready=False, reason="chatwoot_auth_failed")
    return ReadinessResult(ready=True, reason="ready")
```

- [ ] **Step 2: Wire `/ready` to database and service_state**

Modify `app/api/health.py` so `/ready`:

- checks PostgreSQL with `SELECT 1`;
- reads `service_state` for current auth failures;
- returns 200 when ready;
- returns 503 with `{"status": "not_ready", "reason": "<reason>"}` when not ready.

- [ ] **Step 3: Wire worker loop**

Create `app/cli.py`:

```python
from __future__ import annotations

import asyncio

import uvicorn

from app.asgi import app
from app.settings import AppMode, get_settings
from app.workers.jobs import run_worker_loop


async def run_all_mode() -> None:
    settings = get_settings()
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8000))
    await asyncio.gather(server.serve(), run_worker_loop(settings=settings))


def main() -> None:
    settings = get_settings()
    if settings.app_mode == AppMode.WORKER:
        asyncio.run(run_worker_loop(settings=settings))
        return
    if settings.app_mode == AppMode.ALL:
        asyncio.run(run_all_mode())
        return
    raise RuntimeError("web mode is served by the ASGI server")


if __name__ == "__main__":
    main()
```

Modify worker modules so `worker` mode:

- creates engine/session factory;
- acquires advisory lock;
- runs cleanup once;
- loops over:
  - stale job reset;
  - one due `sync_jobs` claim/process cycle;
  - due ZZap summary scheduling;
  - one ZZap action under rate limiter;
  - service heartbeat update;
- sleeps briefly when no work is available.

- [ ] **Step 4: Wire job dispatcher**

In `app/workers/jobs.py`, dispatch by `job_type`:

- `inbound_zzap_message_to_chatwoot` -> `InboundProcessor`;
- `outbound_chatwoot_message_to_zzap` -> `OutboundProcessor`;
- `chatwoot_private_note` -> Chatwoot client private note method.

On success, set `status=succeeded`, clear payload, clear lock fields. On retryable failure, set `status=pending`, set `next_attempt_at`, clear lock fields. On exhausted failure, set `status=failed`, clear lock fields.

- [ ] **Step 5: Run unit tests**

Run:

```bash
rtk uv run pytest tests/unit -q
```

Expected: all unit tests pass.

- [ ] **Step 6: Commit**

```bash
rtk git add app/api/health.py app/services/readiness.py app/workers app/cli.py
rtk git commit -m "feat: wire worker loop and readiness"
```

### Task 16: Docker, Compose, And Environment Example

**Files:**

- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `docker-compose.prod.example.yml`
- Create: `scripts/docker-entrypoint.sh`
- Create: `.env.example`

- [ ] **Step 1: Create Docker entrypoint**

Create `scripts/docker-entrypoint.sh`:

```sh
#!/bin/sh
set -eu

uv run alembic upgrade head

if [ "${APP_MODE:-web}" = "web" ]; then
  exec uv run uvicorn app.asgi:app --host 0.0.0.0 --port 8000
fi

if [ "${APP_MODE:-web}" = "worker" ]; then
  exec uv run python -m app.cli
fi

if [ "${APP_MODE:-web}" = "all" ]; then
  exec uv run python -m app.cli
fi

echo "Unsupported APP_MODE=${APP_MODE:-web}" >&2
exit 1
```

Make it executable:

```bash
rtk chmod +x scripts/docker-entrypoint.sh
```

- [ ] **Step 2: Create Dockerfile**

Create `Dockerfile`:

```dockerfile
FROM python:3.14-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY . .

ENTRYPOINT ["./scripts/docker-entrypoint.sh"]
```

- [ ] **Step 3: Create local compose**

Create `docker-compose.yml`:

```yaml
services:
  db:
    image: postgres:17
    environment:
      POSTGRES_DB: chatwoot_zzap
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d chatwoot_zzap"]
      interval: 5s
      timeout: 3s
      retries: 10

  app:
    build: .
    env_file:
      - .env
    environment:
      APP_MODE: all
      DATABASE_URL: postgresql+asyncpg://postgres:postgres@db:5432/chatwoot_zzap
    ports:
      - "8000:8000"
    depends_on:
      db:
        condition: service_healthy

volumes:
  postgres_data:
```

- [ ] **Step 4: Create production compose example**

Create `docker-compose.prod.example.yml`:

```yaml
services:
  web:
    image: chatwoot-zzap-integration:latest
    env_file:
      - .env
    environment:
      APP_MODE: web
    ports:
      - "8000:8000"
    restart: unless-stopped

  worker:
    image: chatwoot-zzap-integration:latest
    env_file:
      - .env
    environment:
      APP_MODE: worker
    restart: unless-stopped
```

- [ ] **Step 5: Create `.env.example`**

Create `.env.example` with non-secret examples:

```dotenv
APP_MODE=all
DATABASE_URL=postgresql+asyncpg://postgres:postgres@db:5432/chatwoot_zzap
INTEGRATION_ID=11111111-1111-4111-8111-111111111111
ZZAP_BASE_URL=https://b52-api.zzap.pro
ZZAP_API_KEY=replace-me
CHATWOOT_BASE_URL=https://chatwoot.example.com
CHATWOOT_ACCOUNT_ID=1
CHATWOOT_INBOX_ID=1
CHATWOOT_API_TOKEN=replace-me
CHATWOOT_WEBHOOK_SECRET=replace-me
MAX_ATTACHMENT_BYTES=10485760
SUCCESSFUL_MESSAGE_RETENTION_DAYS=60
FAILED_RECORD_RETENTION_DAYS=30
WEBHOOK_DELIVERY_RETENTION_DAYS=30
```

- [ ] **Step 6: Run static verification**

Run:

```bash
rtk uv run ruff check .
rtk uv run mypy app
rtk uv run pytest -q
```

Expected: all commands exit 0.

- [ ] **Step 7: Commit**

```bash
rtk git add Dockerfile docker-compose.yml docker-compose.prod.example.yml scripts/docker-entrypoint.sh .env.example
rtk git commit -m "chore: add docker deployment files"
```

### Task 17: Documentation And Final Verification

**Files:**

- Modify: `README.md`

- [ ] **Step 1: Write README**

Update `README.md` with:

- what the service does;
- required env vars;
- Docker Compose local start;
- Chatwoot webhook URL and HMAC secret requirement;
- ZZap rate limit behavior;
- known limitations from the spec;
- test commands.

- [ ] **Step 2: Run final verification**

Run:

```bash
rtk uv run ruff check .
rtk uv run mypy app
rtk uv run pytest -q
```

Expected: all commands exit 0.

- [ ] **Step 3: Inspect git diff**

Run:

```bash
rtk git status --short
rtk git diff --stat
```

Expected: only intended implementation and README changes are present.

- [ ] **Step 4: Commit**

```bash
rtk git add README.md
rtk git commit -m "docs: document integration service"
```

## Self-Review Checklist

- Spec coverage:
  - Bidirectional sync: Tasks 8, 12, 13, 15.
  - PostgreSQL schema and migrations: Task 3.
  - Job queue and atomic claim: Tasks 6, 14, 15.
  - ZZap rate limit and coalescing: Task 11.
  - Cursor plus overlap and retention guard: Tasks 4, 12.
  - Chatwoot HMAC webhook: Tasks 5, 9.
  - Attachments: Tasks 7, 8, 13.
  - Docker and migration entrypoint: Task 16.
  - Tests/tooling: Tasks 1 and 17.
- Placeholder scan:
  - No unresolved marker sections remain.
  - Webhook route creation is delayed until outbound job persistence exists.
  - CLI creation is delayed until the worker loop exists.
- Type consistency:
  - Job type and status values match `app/db/models.py`.
  - `integration_id` is a UUID setting and appears on all main tables.
  - Fingerprint functions return `message_hash` and `fingerprint`, matching the schema.

## Execution Options

After this plan is approved:

1. **Subagent-Driven (recommended)** - dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** - execute tasks in this session using executing-plans, batch execution with checkpoints.
