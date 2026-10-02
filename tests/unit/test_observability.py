from __future__ import annotations

import asyncio
import copy
import json
import socket
import ssl
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import httpx
import pytest
from litestar.testing import TestClient

from app.api.metrics import MetricsExporter, render_metrics
from app.asgi import create_app
from app.clients.zzap import ZZapApiError, ZZapClient
from app.services.diagnostics import network_category, response_diagnostics
from app.services.poll_health import POLL_KEY, PollHealth
from app.settings import Settings
from app.workers import jobs
from app.workers.rate_limit import ZZapRateLimiter
from app.workers.zzap_scheduler import ZZapActionQueue

START = datetime(2026, 10, 2, 12, tzinfo=UTC)


@pytest.mark.parametrize("status", [200, 401, 403, 429, 500])
async def test_http_diagnostics_safe_and_no_extra_requests(status: int, caplog: Any) -> None:
    calls: list[Any] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if status == 200:
            return httpx.Response(200, json={"success": True, "result": {"data": []}})
        return httpx.Response(
            status,
            json={
                "errors": "API_SECRET PRIVATE_MESSAGE PERSON_EMAIL USER_KEY",
                "token": "API_SECRET",
            },
            headers={
                "server": "cloudflare",
                "cf-ray": "0123456789abcdef-FRA",
                "authorization": "API_SECRET",
                "set-cookie": "API_SECRET",
                "x-request-id": "API_SECRET",
                "retry-after": "60",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ZZapClient(base_url="https://zzap.test", api_key="API_SECRET", http_client=http)
        if status == 200:
            assert await client.list_messages(user_key="USER_KEY", page=1, page_size=100) == []
        else:
            with pytest.raises(ZZapApiError) as raised:
                await client.list_messages(user_key="USER_KEY", page=1, page_size=100)
            assert "PRIVATE_MESSAGE" not in str(raised.value)
            data = json.loads(caplog.records[-1].message)
            assert data["http_status"] == status
            assert data["endpoint"] == "/api/client/v1/messages/{user_key}"
            assert data["query"] == {"page": 1, "page_size": 100}
            assert data["duration_seconds"] >= 0
            assert data["headers"]["retry-after"] == "60"
            for secret in ["API_SECRET", "PRIVATE_MESSAGE", "PERSON_EMAIL", "USER_KEY"]:
                assert secret not in caplog.text
    assert len(calls) == 1


@pytest.mark.parametrize(
    "exception,category",
    [
        (httpx.ReadTimeout("SECRET"), "timeout"),
        (httpx.ReadError("SECRET"), "network_unknown"),
        (httpx.ConnectError("SECRET"), "connection"),
    ],
)
async def test_network_safe(exception: httpx.RequestError, category: str, caplog: Any) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise exception

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ZZapClient(base_url="https://zzap.test", api_key="SECRET", http_client=http)
        with pytest.raises(type(exception)) as raised:
            await client.list_threads(page=1, page_size=100)
    assert category in str(raised.value)
    assert "SECRET" not in caplog.text + str(raised.value)
    assert json.loads(caplog.records[-1].message)["category"] == category


@pytest.mark.parametrize(
    "cause,category",
    [
        (socket.gaierror(), "dns"),
        (ssl.SSLError(), "tls"),
        (ConnectionResetError(), "connection_reset"),
    ],
)
def test_network_cause_chain(cause: Exception, category: str) -> None:
    exc = httpx.ConnectError("secret")
    exc.__cause__ = cause
    assert network_category(exc) == category


def test_html_and_unknown_source_and_closed_stream() -> None:
    class Closed:
        def get_extra_info(self, name: str) -> Any:
            raise RuntimeError("secret")

    response = httpx.Response(
        403,
        text="<html>PRIVATE</html>",
        headers={"content-type": "text/html", "server": "cloudflare"},
        extensions={"network_stream": Closed()},
    )
    data = response_diagnostics(response)
    assert data["source"] == "cdn_waf_suspected"
    assert "PRIVATE" not in str(data)
    assert (
        response_diagnostics(httpx.Response(403, json={"error": "private"}))["source"] == "unknown"
    )


class MemoryHealth(PollHealth):
    def __init__(self, state: dict[str, Any] | None = None) -> None:
        super().__init__(None, "integration", 120)
        self.state = copy.deepcopy(
            state
            or {"initialized_at": START.isoformat(), "consecutive_failures": 0, "degraded": False}
        )
        self.saved: list[dict[str, Any]] = []

    async def save(self) -> None:
        self.saved.append(copy.deepcopy(self.state))


async def test_incident_first_failure_restart_recovery_and_new_incident(caplog: Any) -> None:
    health = MemoryHealth()
    await health.attempt(START)
    error = ZZapApiError(403, "safe")
    error.category = "forbidden"
    await health.result(error, START)
    assert health.state["consecutive_failures"] == 1
    assert health.state["failure_since"] == START.isoformat()
    await health.result(error, START + timedelta(seconds=30))
    await health.observe(START + timedelta(seconds=119))
    assert health.state["degraded"] is False
    restarted = MemoryHealth(health.saved[-1])
    await restarted.observe(START + timedelta(seconds=120))
    assert restarted.state["degraded"] is True
    await restarted.observe(START + timedelta(seconds=121))
    assert sum('"event": "zzap_poll_degraded"' in record.message for record in caplog.records) == 1
    await restarted.result(None, START + timedelta(seconds=3600))
    assert restarted.state["last_incident"]["duration_seconds"] == 3600
    assert restarted.state["last_incident"]["errors"] == 2
    assert restarted.state["degraded"] is False
    assert restarted.state["consecutive_failures"] == 0
    await restarted.result(error, START + timedelta(seconds=3610))
    assert restarted.state["failure_since"] == (START + timedelta(seconds=3610)).isoformat()
    await restarted.observe(START + timedelta(seconds=3720))
    assert restarted.state["degraded"] is True


async def test_first_start_without_attempt_or_success_and_stalled_request() -> None:
    health = MemoryHealth()
    await health.attempt(START)
    await health.observe(START + timedelta(seconds=120))
    assert health.state["degraded"] is True
    assert health.state["last_attempt_status"] == "in_progress"
    assert health.state["consecutive_failures"] == 0
    await health.result(None, START + timedelta(seconds=121))
    assert health.state["last_incident"]["duration_seconds"] == 121


async def test_summary_success_not_processing_error_and_post_does_not_reset(
    monkeypatch: Any,
) -> None:
    health = MemoryHealth()
    queue = ZZapActionQueue()
    queue.enqueue_summary_poll_if_due(now=10, interval_seconds=3)
    limiter = ZZapRateLimiter(interval_seconds=3)
    calls: list[Any] = []

    class Client:
        async def list_threads(self, **kwargs: Any) -> list[Any]:
            calls.append("get")
            return []

        async def send_message(self, **kwargs: Any) -> None:
            calls.append("post")

    @asynccontextmanager
    async def scope(factory: Any) -> Any:
        yield None

    async def no_op(*args: Any, **kwargs: Any) -> None:
        pass

    async def broken(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("database processing")

    monkeypatch.setattr(jobs, "session_scope", scope)
    monkeypatch.setattr(jobs, "_set_auth_failure_state", no_op)
    monkeypatch.setattr(jobs, "_record_external_auth_failure", no_op)
    monkeypatch.setattr(jobs, "_process_summary_threads", broken)
    await jobs.process_next_zzap_action(
        session_factory=cast(Any, None),
        settings=cast(Any, SimpleNamespace(integration_id="id")),
        zzap_client=Client(),
        action_queue=queue,
        rate_limiter=limiter,
        monotonic=lambda: 10,
        poll_health=health,
    )
    assert health.state["last_attempt_status"] == "success"
    await health.result(ZZapApiError(403, "safe"), START)
    snapshot = copy.deepcopy(health.state)

    async def sleep(delay: float) -> None:
        pass

    await jobs.RateLimitedZZapClient(
        Client(), limiter=limiter, sleep=sleep, monotonic=lambda: 20
    ).send_message(user_key="private", message="private", message_date=START, is_online=True)
    assert health.state == snapshot
    assert calls == ["get", "post"]


def test_metrics_from_worker_snapshot_in_web_without_labels() -> None:
    state = {
        "initialized_at": START.isoformat(),
        "last_attempt_at": START.isoformat(),
        "failure_since": START.isoformat(),
        "consecutive_failures": 4,
        "last_http_status": 403,
    }
    snapshot = {POLL_KEY: state, "worker_heartbeat": {"at": START.isoformat()}}
    early = render_metrics(snapshot, START.timestamp() + 119, 120)
    late = render_metrics(snapshot, START.timestamp() + 120, 120)
    assert "zzap_poll_degraded 0" in early
    assert "zzap_poll_degraded 1" in late
    assert "zzap_poll_failure_duration_seconds 120.0" in late
    assert "zzap_last_success_timestamp_seconds 0.0" in late
    assert "zzap_poll_last_http_status 403" in late
    assert "{" not in late
    assert "zzap_poll_degraded 0" in render_metrics({}, START.timestamp(), 120)


def settings(token: str = "") -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://user:pass@db/app",
        INTEGRATION_ID=UUID("11111111-1111-4111-8111-111111111111"),
        ZZAP_BASE_URL="https://zzap.test",
        ZZAP_API_KEY="secret",
        CHATWOOT_BASE_URL="https://cw.test",
        CHATWOOT_ACCOUNT_ID=1,
        CHATWOOT_INBOX_ID=1,
        CHATWOOT_API_TOKEN="secret",
        CHATWOOT_WEBHOOK_SECRET="secret",
        METRICS_TOKEN=token,
    )


@pytest.mark.parametrize(
    "token,authorization,status",
    [
        ("", "", 404),
        ("secret", "", 401),
        ("secret", "Bearer wrong", 401),
        ("secret", "Bearer secret", 200),
    ],
)
def test_metrics_endpoint_auth(
    monkeypatch: Any, token: str, authorization: str, status: int
) -> None:
    import app.asgi as asgi

    monkeypatch.setattr(asgi, "get_settings", lambda: settings(token))

    class Exporter:
        async def dispose(self) -> None:
            pass

        @property
        def engine(self) -> Any:
            return self

        async def render(self) -> str:
            return "zzap_poll_degraded 1\n"

    app = create_app()
    app.state.zzap_metrics_exporter = Exporter()
    with TestClient(app) as client:
        response = client.get("/metrics", headers={"authorization": authorization})
    assert response.status_code == status


async def test_exporter_cache_and_database_failure(monkeypatch: Any) -> None:
    import app.api.metrics as api

    exporter = MetricsExporter(settings("secret"))
    reads = []

    class Session:
        async def execute(self, statement: Any) -> Any:
            reads.append(statement)
            return SimpleNamespace(all=lambda: [(POLL_KEY, {"initialized_at": START.isoformat()})])

    @asynccontextmanager
    async def scope(factory: Any) -> Any:
        yield Session()

    monkeypatch.setattr(api, "session_scope", scope)
    await exporter.render()
    await exporter.render()
    assert len(reads) == 1
    exporter.expires = 0

    @asynccontextmanager
    async def failed(factory: Any) -> Any:
        raise OSError("secret")
        yield

    monkeypatch.setattr(api, "session_scope", failed)
    with pytest.raises(OSError):
        await exporter.render()
    await exporter.engine.dispose()


async def test_monitor_observes_without_polling(monkeypatch: Any) -> None:
    health = MemoryHealth()
    calls: list[Any] = []

    async def observe(now: Any = None) -> None:
        calls.append(now)
        raise asyncio.CancelledError

    monkeypatch.setattr(health, "observe", observe)
    with pytest.raises(asyncio.CancelledError):
        await health.monitor()
    assert calls == [None]


async def test_backoff_legacy_hints_preserved() -> None:
    for text, expected in [
        ("captcha private", 60),
        ("rate private", 60),
        ("Forbidden private", 30),
    ]:

        async def handler(request: httpx.Request, body: str = text) -> httpx.Response:
            return httpx.Response(403, text=body)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = ZZapClient(base_url="https://zzap.test", api_key="secret", http_client=http)
            with pytest.raises(ZZapApiError) as caught:
                await client.list_threads(page=1, page_size=100)
        assert jobs._zzap_poll_backoff_seconds(caught.value) == expected


async def test_durable_restart_and_web_exporter_shared_database(monkeypatch: Any) -> None:
    import app.api.metrics as api
    import app.services.poll_health as service

    database: dict[str, Any] = {}

    class Session:
        async def execute(self, statement: Any) -> Any:
            if getattr(statement, "is_select", False):
                return SimpleNamespace(
                    scalar_one_or_none=lambda: copy.deepcopy(database.get(POLL_KEY)),
                    all=lambda: list(copy.deepcopy(database).items()),
                )
            parameters = statement.compile().params
            database[parameters["key"]] = copy.deepcopy(parameters["value"])
            return None

    @asynccontextmanager
    async def scope(factory: Any) -> Any:
        yield Session()

    monkeypatch.setattr(service, "session_scope", scope)
    monkeypatch.setattr(api, "session_scope", scope)
    original = PollHealth(None, "id", 120)
    await original.initialize()
    original.state["initialized_at"] = START.isoformat()
    await original.attempt(START)
    await original.result(ZZapApiError(403, "safe"), START)
    restarted = PollHealth(None, "id", 120)
    await restarted.initialize()
    assert restarted.state["failure_since"] == START.isoformat()
    assert restarted.state["consecutive_failures"] == 1
    assert restarted.state["degraded"] is True
    exporter = MetricsExporter(settings("secret"))
    assert "zzap_poll_degraded 1" in await exporter.render()
    assert "zzap_poll_last_http_status 403" in await exporter.render()
    await restarted.result(None)
    exporter.expires = 0
    assert "zzap_poll_degraded 0" in await exporter.render()
    await exporter.engine.dispose()


async def test_health_persistence_failure_does_not_replace_http_or_limiter(
    monkeypatch: Any,
) -> None:
    health = MemoryHealth()
    clock = [10.0]
    calls: list[Any] = []
    limiter = ZZapRateLimiter(interval_seconds=3)

    async def broken_save() -> None:
        # Finish timestamp must already have been marked before result persistence.
        if calls:
            assert limiter.delay_until_next(now=clock[0]) == 3
        clock[0] += 1
        raise OSError("database")

    monkeypatch.setattr(health, "save", broken_save)

    class Client:
        async def list_threads(self, **kwargs: Any) -> list[Any]:
            calls.append("get")
            clock[0] = 20
            raise ZZapApiError(429, "safe")

    @asynccontextmanager
    async def scope(factory: Any) -> Any:
        yield None

    async def no_op(*args: Any, **kwargs: Any) -> None:
        pass

    monkeypatch.setattr(jobs, "session_scope", scope)
    monkeypatch.setattr(jobs, "_record_external_auth_failure", no_op)
    queue = ZZapActionQueue()
    queue.enqueue_summary_poll_if_due(now=10, interval_seconds=3)
    await jobs.process_next_zzap_action(
        session_factory=cast(Any, None),
        settings=cast(Any, SimpleNamespace(integration_id="id")),
        zzap_client=Client(),
        action_queue=queue,
        rate_limiter=limiter,
        monotonic=lambda: clock[0],
        poll_health=health,
    )
    assert calls == ["get"]
    assert queue.pop_next(now=69) is None
    assert limiter.delay_until_next(now=21) == 2


@pytest.mark.parametrize("operation", ["upload_file", "send_message"])
async def test_outbound_exceptions_use_shared_limiter(operation: str) -> None:
    limiter = ZZapRateLimiter(interval_seconds=3)

    class Client:
        async def upload_file(self, **kwargs: Any) -> str:
            raise httpx.ReadTimeout("safe")

        async def send_message(self, **kwargs: Any) -> None:
            raise ZZapApiError(403, "safe")

    wrapped = jobs.RateLimitedZZapClient(Client(), limiter=limiter, monotonic=lambda: 10)
    arguments = (
        {"file_name": "private", "file_body_base64": "private"}
        if operation == "upload_file"
        else {"user_key": "private", "message": "private", "message_date": START, "is_online": True}
    )
    with pytest.raises((httpx.ReadTimeout, ZZapApiError)):
        await getattr(wrapped, operation)(**arguments)
    assert limiter.delay_until_next(now=11) == 2


@pytest.mark.parametrize(
    "cause,category", [(socket.gaierror(), "dns"), (ConnectionResetError(), "connection_reset")]
)
async def test_client_network_cause_safe(cause: Exception, category: str, caplog: Any) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("PRIVATE_URL") from cause

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ZZapClient(base_url="https://zzap.test", api_key="private", http_client=http)
        with pytest.raises(httpx.ConnectError):
            await client.list_threads(page=1, page_size=100)
    assert json.loads(caplog.records[-1].message)["category"] == category
    assert "PRIVATE_URL" not in caplog.text


def test_settings_monitoring_defaults_and_validation() -> None:
    from pydantic import ValidationError

    config = settings("private_metrics_secret")
    assert config.zzap_poll_degraded_seconds == 120
    assert config.metrics_cache_seconds == 5
    assert config.metrics_db_timeout_seconds == 5
    assert "private_metrics_secret" not in repr(config)
    with pytest.raises(ValidationError):
        Settings.model_validate(
            {**config.model_dump(by_alias=True), "ZZAP_POLL_DEGRADED_SECONDS": 0}
        )


def test_metrics_actual_database_failure_response(monkeypatch: Any) -> None:
    import app.asgi as asgi

    monkeypatch.setattr(asgi, "get_settings", lambda: settings("secret"))

    class Exporter:
        engine: Any

        def __init__(self) -> None:
            self.engine = self

        async def dispose(self) -> None:
            pass

        async def render(self) -> str:
            raise OSError("PRIVATE")

    app = create_app()
    app.state.zzap_metrics_exporter = Exporter()
    with TestClient(app) as client:
        response = client.get("/metrics", headers={"authorization": "Bearer secret"})
    assert response.status_code == 503
    assert "PRIVATE" not in response.text


async def test_hex_secrets_cannot_be_reflected_as_diagnostic_ids(caplog: Any) -> None:
    key = "0123456789abcdef0123456789abcdef"
    user_key = "11111111-1111-4111-8111-111111111111"

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            text="<html>private</html>",
            headers={
                "cf-ray": key + "-FRA",
                "x-request-id": user_key,
                "traceparent": "00-" + key + "-0123456789abcdef-01",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ZZapClient(base_url="https://zzap.test", api_key=key, http_client=http)
        with pytest.raises(ZZapApiError):
            await client.list_messages(user_key=user_key, page=1, page_size=100)
    assert key not in caplog.text
    assert user_key not in caplog.text
    assert json.loads(caplog.records[-1].message)["headers"] == {}


def test_http_date_retry_after_server_family_and_cf_without_server() -> None:
    response = httpx.Response(
        403,
        text="<html>private</html>",
        headers={
            "retry-after": "Fri, 02 Oct 2026 13:00:00 GMT",
            "server": "nginx/1.26.0",
            "cf-ray": "0123456789abcdef-FRA",
        },
    )
    data = response_diagnostics(response)
    assert data["headers"]["retry-after"] == "2026-10-02T13:00:00+00:00"
    assert data["headers"]["server"] == "nginx/1.26.0"
    assert data["source"] == "cdn_waf_suspected"


async def test_application_failure_keeps_legacy_auth_but_actual_http_status(caplog: Any) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"success": False, "code": 401, "errors": "private"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ZZapClient(base_url="https://zzap.test", api_key="secret", http_client=http)
        with pytest.raises(ZZapApiError) as caught:
            await client.list_threads(page=1, page_size=100)
    assert caught.value.status_code == 401
    assert caught.value.actual_http_status == 200
    assert jobs._zzap_poll_backoff_seconds(caught.value) == 300
    health = MemoryHealth()
    await health.result(caught.value, START)
    assert health.state["last_http_status"] == 200
    assert health.state["last_error_category"] == "api_error"
    event = json.loads(caplog.records[-1].message)
    assert event["http_status"] == 200
    assert event["category"] == "api_error"


async def test_exporter_timeout_does_not_serve_expired_snapshot(monkeypatch: Any) -> None:
    import app.api.metrics as api

    exporter = MetricsExporter(
        settings("secret").model_copy(update={"metrics_db_timeout_seconds": 0.01})
    )
    exporter.snapshot = {POLL_KEY: {"last_success_at": START.isoformat()}}
    exporter.expires = 0

    @asynccontextmanager
    async def slow(factory: Any) -> Any:
        await asyncio.sleep(10)
        yield None

    monkeypatch.setattr(api, "session_scope", slow)
    with pytest.raises(TimeoutError):
        await exporter.render()
    await exporter.engine.dispose()


async def test_dirty_observation_retries_snapshot_without_duplicate_transition(caplog: Any) -> None:
    health = MemoryHealth()
    health.dirty = True
    await health.observe(START + timedelta(seconds=120))
    health.dirty = False
    await health.observe(START + timedelta(seconds=121))
    assert len(health.saved) == 1
    assert sum('"event": "zzap_poll_degraded"' in item.message for item in caplog.records) == 1


async def test_stall_incident_start_is_frozen_after_first_http_failure() -> None:
    health = MemoryHealth()
    await health.observe(START + timedelta(seconds=120))
    assert health.state["incident_started_at"] == START.isoformat()
    await health.result(ZZapApiError(403, "safe"), START + timedelta(seconds=180))
    assert health.state["failure_since"] == (START + timedelta(seconds=180)).isoformat()
    assert health.state["incident_started_at"] == START.isoformat()
    await health.result(None, START + timedelta(seconds=240))
    assert health.state["last_incident"]["started_at"] == START.isoformat()
    assert health.state["last_incident"]["duration_seconds"] == 240
    assert health.state["incident_started_at"] is None
    await health.result(ZZapApiError(403, "safe"), START + timedelta(seconds=250))
    assert health.state["incident_started_at"] == (START + timedelta(seconds=250)).isoformat()


@pytest.mark.parametrize("body", [b"not-json", b"[]"])
async def test_invalid_response_exception_matches_log_category(body: bytes, caplog: Any) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ZZapClient(base_url="https://zzap.test", api_key="secret", http_client=http)
        with pytest.raises(ZZapApiError) as caught:
            await client.list_threads(page=1, page_size=100)
    assert caught.value.category == "invalid_response"
    assert caught.value.actual_http_status == 200
    assert json.loads(caplog.records[-1].message)["category"] == "invalid_response"


def test_diagnostic_logger_uses_existing_root_handler_without_duplicate() -> None:
    import io
    import logging

    from app.services.diagnostics import emit, logger

    output = io.StringIO()
    handler = logging.StreamHandler(output)
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        emit("single_event", warning=True)
    finally:
        root.removeHandler(handler)
    assert not logger.handlers
    assert output.getvalue().count('"event": "single_event"') == 1
