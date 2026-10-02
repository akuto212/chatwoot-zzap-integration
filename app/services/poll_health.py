"""Durable health of summary polling only, independent of message processing."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.db.models import ServiceState
from app.db.session import session_scope
from app.services.diagnostics import emit

POLL_KEY = "zzap_poll_health"


def timestamp(value: Any) -> float:
    try:
        return datetime.fromisoformat(value).timestamp() if value else 0.0
    except TypeError, ValueError:
        return 0.0


def degraded(state: dict[str, Any], now: float, threshold: float) -> bool:
    basis = timestamp(state.get("last_success_at")) or timestamp(state.get("initialized_at"))
    return bool(basis and now - basis >= threshold)


def failure_duration(state: dict[str, Any], now: float) -> float:
    start = timestamp(state.get("incident_started_at")) or timestamp(state.get("failure_since"))
    if not start and state.get("degraded"):
        start = timestamp(state.get("last_success_at")) or timestamp(state.get("initialized_at"))
    return max(0.0, now - start) if start else 0.0


class PollHealth:
    def __init__(self, factory: Any, integration_id: Any, threshold: float) -> None:
        self.factory = factory
        self.integration_id = integration_id
        self.threshold = threshold
        self.state: dict[str, Any] = {}
        self.lock = asyncio.Lock()
        self.dirty = False
        self.retry_after = 0.0

    async def record_attempt(self) -> None:
        try:
            async with asyncio.timeout(5.0):
                await self.attempt()
        except Exception:
            emit("zzap_poll_health_persistence_failed", warning=True, category="database")

    async def record_result(self, exc: Exception | None) -> None:
        try:
            async with asyncio.timeout(5.0):
                await self.result(exc)
        except Exception:
            emit("zzap_poll_health_persistence_failed", warning=True, category="database")

    async def initialize(self) -> None:
        async with session_scope(self.factory) as session:
            result = await session.execute(
                select(ServiceState.value).where(
                    ServiceState.integration_id == self.integration_id, ServiceState.key == POLL_KEY
                )
            )
            self.state = dict(result.scalar_one_or_none() or {})
        if not self.state:
            self.state = {
                "initialized_at": datetime.now(UTC).isoformat(),
                "consecutive_failures": 0,
                "degraded": False,
            }
        self.state["threshold_seconds"] = self.threshold
        await self.save()
        await self.observe()

    async def save(self) -> None:
        self.dirty = True
        async with session_scope(self.factory) as session:
            await session.execute(
                insert(ServiceState)
                .values(
                    integration_id=self.integration_id,
                    key=POLL_KEY,
                    value=dict(self.state),
                )
                .on_conflict_do_update(
                    index_elements=["integration_id", "key"],
                    set_={"value": dict(self.state), "updated_at": datetime.now(UTC)},
                )
            )

        self.dirty = False

    async def attempt(self, now: datetime | None = None) -> None:
        async with self.lock:
            self.state["last_attempt_at"] = (now or datetime.now(UTC)).isoformat()
            self.state["last_attempt_status"] = "in_progress"
            await self.save()

    async def result(self, exc: Exception | None, now: datetime | None = None) -> None:
        async with self.lock:
            current = now or datetime.now(UTC)
            if exc is None:
                count = self.state.get("consecutive_failures", 0)
                if count or self.state.get("degraded"):
                    self.state["last_recovery_at"] = current.isoformat()
                    self.state["last_incident"] = {
                        "started_at": self.state.get("incident_started_at")
                        or self.state.get("failure_since")
                        or self.state.get("last_success_at")
                        or self.state.get("initialized_at"),
                        "ended_at": current.isoformat(),
                        "errors": count,
                        "duration_seconds": failure_duration(self.state, current.timestamp()),
                    }
                    emit("zzap_poll_recovered", **self.state["last_incident"])
                self.state.update(
                    last_success_at=current.isoformat(),
                    last_attempt_status="success",
                    consecutive_failures=0,
                    failure_since=None,
                    incident_started_at=None,
                    degraded=False,
                    last_error_category=None,
                    last_http_status=200,
                )
            else:
                self.state["failure_since"] = self.state.get("failure_since") or current.isoformat()
                self.state["incident_started_at"] = (
                    self.state.get("incident_started_at") or self.state["failure_since"]
                )
                self.state["consecutive_failures"] = self.state.get("consecutive_failures", 0) + 1
                self.state.update(
                    last_attempt_status="failure",
                    last_error_category=getattr(exc, "category", "unknown"),
                    last_http_status=getattr(exc, "actual_http_status", None)
                    or getattr(exc, "status_code", 0),
                )
            await self._transition(current)
            await self.save()

    async def _transition(self, now: datetime) -> bool:
        if degraded(self.state, now.timestamp(), self.threshold) and not self.state.get("degraded"):
            self.state["incident_started_at"] = (
                self.state.get("incident_started_at")
                or self.state.get("failure_since")
                or self.state.get("last_success_at")
                or self.state.get("initialized_at")
            )
            self.state["degraded"] = True
            self.state["degraded_at"] = now.isoformat()
            emit(
                "zzap_poll_degraded",
                warning=True,
                incident_started_at=self.state["incident_started_at"],
                consecutive_failures=self.state.get("consecutive_failures", 0),
                failure_duration_seconds=failure_duration(self.state, now.timestamp()),
            )
            return True
        return False

    async def observe(self, now: datetime | None = None) -> None:
        async with self.lock:
            changed = await self._transition(now or datetime.now(UTC))
            if changed or self.dirty:
                await self.save()

    async def monitor(self) -> None:
        while True:
            basis = timestamp(self.state.get("last_success_at")) or timestamp(
                self.state.get("initialized_at")
            )
            remaining = basis + self.threshold - datetime.now(UTC).timestamp()
            await asyncio.sleep(min(0.5, remaining) if remaining > 0 else 0.5)
            if asyncio.get_running_loop().time() < self.retry_after:
                continue
            try:
                async with asyncio.timeout(5.0):
                    await self.observe()
            except Exception:
                self.retry_after = asyncio.get_running_loop().time() + 5.0
                emit("zzap_poll_health_persistence_failed", warning=True, category="database")
