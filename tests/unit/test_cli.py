from __future__ import annotations

import pytest

import app.cli as cli
from app.settings import AppMode


def test_cli_runs_worker_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(cli, "get_settings", lambda: _Settings(AppMode.WORKER))
    monkeypatch.setattr(cli.asyncio, "run", lambda coroutine: calls.append(str(coroutine)))
    monkeypatch.setattr(cli, "run_worker_loop", lambda settings: "worker-loop")

    cli.main()

    assert calls == ["worker-loop"]


def test_cli_runs_all_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(cli, "get_settings", lambda: _Settings(AppMode.ALL))
    monkeypatch.setattr(cli.asyncio, "run", lambda coroutine: calls.append(str(coroutine)))
    monkeypatch.setattr(cli, "run_all_mode", lambda: "all-mode")

    cli.main()

    assert calls == ["all-mode"]


def test_cli_rejects_web_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "get_settings", lambda: _Settings(AppMode.WEB))

    with pytest.raises(RuntimeError, match="web mode"):
        cli.main()


class _Settings:
    def __init__(self, app_mode: AppMode) -> None:
        self.app_mode = app_mode
