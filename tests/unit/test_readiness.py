from __future__ import annotations

from app.services.readiness import evaluate_readiness


def test_readiness_requires_database() -> None:
    result = evaluate_readiness(
        database_ok=False,
        zzap_auth_failed=False,
        chatwoot_auth_failed=False,
    )

    assert result.ready is False
    assert result.reason == "database_unavailable"


def test_readiness_reports_zzap_auth_failure() -> None:
    result = evaluate_readiness(
        database_ok=True,
        zzap_auth_failed=True,
        chatwoot_auth_failed=False,
    )

    assert result.ready is False
    assert result.reason == "zzap_auth_failed"


def test_readiness_reports_chatwoot_auth_failure() -> None:
    result = evaluate_readiness(
        database_ok=True,
        zzap_auth_failed=False,
        chatwoot_auth_failed=True,
    )

    assert result.ready is False
    assert result.reason == "chatwoot_auth_failed"


def test_readiness_ready_when_dependencies_ok() -> None:
    result = evaluate_readiness(
        database_ok=True,
        zzap_auth_failed=False,
        chatwoot_auth_failed=False,
    )

    assert result.ready is True
    assert result.reason == "ready"
