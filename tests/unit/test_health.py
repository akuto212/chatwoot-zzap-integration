from __future__ import annotations

from collections.abc import Iterator

import pytest
from litestar.testing import TestClient

import app.api.health as health_api
from app.asgi import create_app
from app.services.readiness import ReadinessResult
from app.settings import get_settings


def test_health_returns_ok() -> None:
    with TestClient(create_app()) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_returns_ready() -> None:
    with TestClient(create_app()) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_ready_returns_not_ready_reason(
    monkeypatch: pytest.MonkeyPatch,
    settings_env: None,
) -> None:
    async def fake_check_readiness(*args: object, **kwargs: object) -> ReadinessResult:
        return ReadinessResult(ready=False, reason="zzap_auth_failed")

    monkeypatch.setattr(health_api, "check_readiness", fake_check_readiness)

    with TestClient(create_app()) as client:
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready", "reason": "zzap_auth_failed"}


@pytest.fixture
def settings_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@db:5432/app")
    monkeypatch.setenv("INTEGRATION_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("ZZAP_BASE_URL", "https://b52-api.zzap.pro")
    monkeypatch.setenv("ZZAP_API_KEY", "zzap-secret")
    monkeypatch.setenv("CHATWOOT_BASE_URL", "https://chatwoot.example.test")
    monkeypatch.setenv("CHATWOOT_ACCOUNT_ID", "1")
    monkeypatch.setenv("CHATWOOT_INBOX_ID", "2")
    monkeypatch.setenv("CHATWOOT_API_TOKEN", "chatwoot-secret")
    monkeypatch.setenv("CHATWOOT_WEBHOOK_SECRET", "webhook-secret")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
def ready_check(monkeypatch: pytest.MonkeyPatch, settings_env: None) -> None:
    async def fake_check_readiness(*args: object, **kwargs: object) -> ReadinessResult:
        return ReadinessResult(ready=True, reason="ready")

    monkeypatch.setattr(health_api, "check_readiness", fake_check_readiness)
