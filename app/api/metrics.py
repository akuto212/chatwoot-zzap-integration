"""Small PostgreSQL-backed exporter, shared by web/worker deployments."""

from __future__ import annotations

import asyncio
import hmac
import time
from typing import Any

from litestar import Request, Response, get
from litestar.di import NamedDependency
from sqlalchemy import select

from app.db.models import ServiceState
from app.db.session import create_engine, create_session_factory, session_scope
from app.services.poll_health import POLL_KEY, degraded, failure_duration, timestamp
from app.settings import Settings


class MetricsExporter:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.engine = create_engine(settings.database_url)
        self.factory = create_session_factory(self.engine)
        self.snapshot: dict[str, Any] = {}
        self.expires = 0.0
        self.lock = asyncio.Lock()

    async def render(self) -> str:
        async with self.lock:
            if time.monotonic() >= self.expires:
                async with asyncio.timeout(self.settings.metrics_db_timeout_seconds):
                    async with session_scope(self.factory) as session:
                        result = await session.execute(
                            select(ServiceState.key, ServiceState.value).where(
                                ServiceState.integration_id == self.settings.integration_id,
                                ServiceState.key.in_([POLL_KEY, "worker_heartbeat"]),
                            )
                        )
                        self.snapshot = {str(key): value for key, value in result.all()}
                self.expires = time.monotonic() + self.settings.metrics_cache_seconds
        return render_metrics(self.snapshot, time.time(), self.settings.zzap_poll_degraded_seconds)


def render_metrics(snapshot: dict[str, Any], now: float, threshold: float) -> str:
    state = snapshot.get(POLL_KEY, {})
    threshold = state.get("threshold_seconds", threshold)
    is_degraded = degraded(state, now, threshold)
    values = {
        "zzap_poll_initialized_timestamp_seconds": timestamp(state.get("initialized_at")),
        "zzap_last_success_timestamp_seconds": timestamp(state.get("last_success_at")),
        "zzap_last_attempt_timestamp_seconds": timestamp(state.get("last_attempt_at")),
        "zzap_poll_consecutive_failures": state.get("consecutive_failures", 0),
        "zzap_poll_degraded": int(is_degraded),
        "zzap_poll_failure_duration_seconds": failure_duration(
            {**state, "degraded": is_degraded}, now
        ),
        "zzap_poll_last_http_status": state.get("last_http_status", 0),
        "zzap_worker_last_heartbeat_timestamp_seconds": timestamp(
            snapshot.get("worker_heartbeat", {}).get("at")
        ),
    }
    return "".join(
        f"# HELP {name} ZZap polling health: {name}.\n# TYPE {name} gauge\n{name} {value}\n"
        for name, value in values.items()
    )


@get("/metrics")
async def metrics(request: Request, settings: NamedDependency[Settings]) -> Response[str]:
    if not settings.metrics_token:
        return Response("Metrics disabled", status_code=404, media_type="text/plain")
    supplied = request.headers.get("authorization", "")
    if not hmac.compare_digest(supplied.encode(), f"Bearer {settings.metrics_token}".encode()):
        return Response("Unauthorized", status_code=401, media_type="text/plain")
    exporter = getattr(request.app.state, "zzap_metrics_exporter", None)
    if exporter is None:
        exporter = MetricsExporter(settings)
        request.app.state.zzap_metrics_exporter = exporter
    try:
        async with asyncio.timeout(settings.metrics_db_timeout_seconds + 0.1):
            rendered = await exporter.render()
        return Response(
            rendered,
            media_type="text/plain",
            headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
        )
    except Exception:
        return Response("Metrics database unavailable", status_code=503, media_type="text/plain")


async def close_metrics(app: Any) -> None:
    exporter = getattr(app.state, "zzap_metrics_exporter", None)
    if exporter is not None:
        await exporter.engine.dispose()
