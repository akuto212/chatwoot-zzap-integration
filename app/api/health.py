from __future__ import annotations

from litestar import get


@get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@get("/ready")
async def ready() -> dict[str, str]:
    return {"status": "ready"}
