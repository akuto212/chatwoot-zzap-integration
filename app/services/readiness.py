from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReadinessResult:
    ready: bool
    reason: str


def evaluate_readiness(
    *,
    database_ok: bool,
    zzap_auth_failed: bool,
    chatwoot_auth_failed: bool,
) -> ReadinessResult:
    if not database_ok:
        return ReadinessResult(ready=False, reason="database_unavailable")
    if zzap_auth_failed:
        return ReadinessResult(ready=False, reason="zzap_auth_failed")
    if chatwoot_auth_failed:
        return ReadinessResult(ready=False, reason="chatwoot_auth_failed")
    return ReadinessResult(ready=True, reason="ready")
