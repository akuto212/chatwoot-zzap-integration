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
