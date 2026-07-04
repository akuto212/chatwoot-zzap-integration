from __future__ import annotations

from typing import Any

from litestar import Response, get
from litestar.di import NamedDependency
from litestar.status_codes import HTTP_200_OK, HTTP_503_SERVICE_UNAVAILABLE
from sqlalchemy import select, text

from app.db.models import ServiceState
from app.db.session import create_engine, create_session_factory, session_scope
from app.services.readiness import ReadinessResult, evaluate_readiness
from app.settings import Settings


@get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@get("/ready")
async def ready(settings: NamedDependency[Settings]) -> Response[dict[str, str]]:
    result = await check_readiness(settings)
    if result.ready:
        return Response({"status": "ready"}, status_code=HTTP_200_OK)
    return Response(
        {"status": "not_ready", "reason": result.reason},
        status_code=HTTP_503_SERVICE_UNAVAILABLE,
    )


async def check_readiness(settings: Settings) -> ReadinessResult:
    engine = None
    try:
        engine = create_engine(settings.database_url)
        session_factory = create_session_factory(engine)
        async with session_scope(session_factory) as session:
            await session.execute(text("SELECT 1"))
            state_result = await session.execute(
                select(ServiceState.key, ServiceState.value).where(
                    ServiceState.integration_id == settings.integration_id,
                    ServiceState.key.in_(["zzap_auth_failed", "chatwoot_auth_failed"]),
                ),
            )
            states = {str(key): value for key, value in state_result.all()}
        return evaluate_readiness(
            database_ok=True,
            zzap_auth_failed=_state_auth_failed(states.get("zzap_auth_failed")),
            chatwoot_auth_failed=_state_auth_failed(states.get("chatwoot_auth_failed")),
        )
    except Exception:
        return evaluate_readiness(
            database_ok=False,
            zzap_auth_failed=False,
            chatwoot_auth_failed=False,
        )
    finally:
        if engine is not None:
            await engine.dispose()


def _state_auth_failed(value: Any) -> bool:
    return isinstance(value, dict) and value.get("failed") is True
