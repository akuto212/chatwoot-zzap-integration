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
    assert isinstance(settings.zzap_base_url, str)
    assert isinstance(settings.chatwoot_base_url, str)
    assert settings.max_attachment_bytes == 10 * 1024 * 1024
    assert settings.successful_message_retention_days == 60
    assert settings.failed_record_retention_days == 30
    assert settings.webhook_delivery_retention_days == 30
    rendered = repr(settings)
    assert "postgresql+asyncpg://user:pass@db:5432/app" not in rendered
    assert "zzap-secret" not in rendered
    assert "chatwoot-secret" not in rendered
    assert "webhook-secret" not in rendered


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
