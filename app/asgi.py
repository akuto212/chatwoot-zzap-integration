from __future__ import annotations

from litestar import Litestar
from litestar.di import Provide

from app.api.health import health, ready
from app.settings import Settings, get_settings


def create_app() -> Litestar:
    return Litestar(
        route_handlers=[health, ready],
        dependencies={"settings": Provide(_settings_dependency, sync_to_thread=False)},
    )


def _settings_dependency() -> Settings:
    return get_settings()


app = create_app()
