from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import quote, unquote

import httpx

from app.services.diagnostics import emit, network_category, response_diagnostics


@dataclass(frozen=True)
class ZZapThreadDto:
    user_key: str
    user_name: str | None
    unread_count: int
    message_last_date: str | None
    message_last: str | None
    read_only: bool


@dataclass(frozen=True)
class ZZapMessageDto:
    user_key: str | None
    user_name: str | None
    message_date: str | None
    message: str | None
    unread: bool | None


@dataclass(frozen=True)
class ZZapMessagePage:
    messages: list[ZZapMessageDto]
    total_count: int | None


class ZZapApiError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.category = "http_unknown"
        self.actual_http_status: int | None = None
        self.legacy_backoff_hint: bool | None = None


class ZZapClient:
    def __init__(self, *, base_url: str, api_key: str, http_client: httpx.AsyncClient) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._http = http_client

    async def list_threads(self, *, page: int, page_size: int) -> list[ZZapThreadDto]:
        payload = await self._request_json(
            "GET",
            "/api/client/v1/messages",
            params={"page": page, "page_size": page_size},
        )
        return [
            ZZapThreadDto(
                user_key=item.get("user_key") or "",
                user_name=item.get("user_name"),
                unread_count=item.get("unread_count") or 0,
                message_last_date=item.get("message_last_date"),
                message_last=item.get("message_last"),
                read_only=bool(item.get("read_only")),
            )
            for item in _result_data(payload)
            if item.get("user_key")
        ]

    async def list_messages(
        self,
        *,
        user_key: str,
        page: int,
        page_size: int,
    ) -> list[ZZapMessageDto]:
        result = await self.list_messages_page(
            user_key=user_key,
            page=page,
            page_size=page_size,
        )
        return result.messages

    async def list_messages_page(
        self,
        *,
        user_key: str,
        page: int,
        page_size: int,
    ) -> ZZapMessagePage:
        encoded_user_key = quote(user_key, safe="")
        payload = await self._request_json(
            "GET",
            f"/api/client/v1/messages/{encoded_user_key}",
            params={"page": page, "page_size": page_size},
        )
        messages = [
            ZZapMessageDto(
                user_key=item.get("user_key"),
                user_name=item.get("user_name"),
                message_date=item.get("message_date"),
                message=item.get("message"),
                unread=item.get("unread"),
            )
            for item in _result_data(payload)
        ]
        info = payload.get("result_info")
        total_count = info.get("total_count") if isinstance(info, dict) else None
        if total_count is not None and (type(total_count) is not int or total_count < 0):
            raise ZZapApiError(200, "ZZap response result_info.total_count was invalid")
        return ZZapMessagePage(messages=messages, total_count=total_count)

    async def upload_file(
        self,
        *,
        file_name: str,
        file_body_base64: str,
        upload_type: int = 1,
    ) -> str:
        payload = await self._request_json(
            "POST",
            "/api/client/v1/upload",
            json={
                "file_name": file_name,
                "file_body": file_body_base64,
                "upload_type": upload_type,
            },
        )
        result = _result_object(payload)
        file_url = result.get("file_url")
        if not file_url:
            raise ZZapApiError(200, "ZZap upload response did not include file_url")
        return str(file_url)

    async def send_message(
        self,
        *,
        user_key: str,
        message: str,
        message_date: datetime,
        is_online: bool,
    ) -> None:
        await self._request_json(
            "POST",
            "/api/client/v1/messages",
            json={
                "user_key": user_key,
                "message": message,
                "message_date": message_date.isoformat(),
                "is_online": is_online,
            },
        )

    async def _request_json(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        endpoint = (
            "/api/client/v1/messages/{user_key}"
            if path.startswith("/api/client/v1/messages/")
            else path
        )
        query = {
            k: v
            for k, v in kwargs.get("params", {}).items()
            if k in {"page", "page_size"} and isinstance(v, int)
        }
        sensitive = [self._api_key]
        if path.startswith("/api/client/v1/messages/"):
            sensitive.extend([path.rsplit("/", 1)[-1], unquote(path.rsplit("/", 1)[-1])])
        sensitive.extend(
            value for value in kwargs.get("json", {}).values() if isinstance(value, str)
        )
        started = time.monotonic()
        try:
            response = await self._http.request(
                method, f"{self._base_url}{path}", headers={"zzap-api-key": self._api_key}, **kwargs
            )
        except httpx.RequestError as exc:
            category = network_category(exc)
            emit(
                "zzap_api_request_failed",
                warning=True,
                method=method,
                endpoint=endpoint,
                query=query,
                duration_seconds=time.monotonic() - started,
                category=category,
                exception_type=type(exc).__name__,
                source="unknown",
            )
            safe = type(exc)(f"ZZap request failed: {category}")
            safe.category = category  # type: ignore[attr-defined]
            safe.legacy_backoff_hint = any(  # type: ignore[attr-defined]
                word in str(exc).lower() for word in ("captcha", "rate")
            )
            raise safe from None
        if response.status_code >= 400:
            diagnostics = response_diagnostics(response, tuple(sensitive))
            emit(
                "zzap_api_request_failed",
                warning=True,
                method=method,
                endpoint=endpoint,
                query=query,
                duration_seconds=time.monotonic() - started,
                **diagnostics,
            )
            error = ZZapApiError(
                response.status_code, f"ZZap HTTP {response.status_code}: {diagnostics['category']}"
            )
            error.actual_http_status = response.status_code
            error.category = diagnostics["category"]
            error.legacy_backoff_hint = any(
                word in response.text.lower() for word in ("captcha", "rate")
            )
            raise error
        try:
            payload = response.json()
        except ValueError as exc:
            diagnostics = response_diagnostics(response, tuple(sensitive))
            diagnostics["category"] = "invalid_response"
            emit(
                "zzap_api_request_failed",
                warning=True,
                method=method,
                endpoint=endpoint,
                query=query,
                duration_seconds=time.monotonic() - started,
                **diagnostics,
            )
            error = ZZapApiError(response.status_code, "ZZap response was not valid JSON")
            error.category = "invalid_response"
            error.actual_http_status = response.status_code
            raise error from exc
        if not isinstance(payload, dict):
            diagnostics = response_diagnostics(response, tuple(sensitive))
            diagnostics["category"] = "invalid_response"
            emit(
                "zzap_api_request_failed",
                warning=True,
                method=method,
                endpoint=endpoint,
                query=query,
                duration_seconds=time.monotonic() - started,
                **diagnostics,
            )
            error = ZZapApiError(response.status_code, "ZZap response was not a JSON object")
            error.category = "invalid_response"
            error.actual_http_status = response.status_code
            raise error
        if payload.get("success") is False:
            error_code = response.status_code
            try:
                error_code = int(payload.get("code") or response.status_code)
            except TypeError, ValueError:
                pass
            error = ZZapApiError(error_code, "ZZap API reported failure")
            error.actual_http_status = response.status_code
            error.category = "api_error"
            error.legacy_backoff_hint = any(
                word in str(payload.get("errors")).lower() for word in ("captcha", "rate")
            )
            diagnostics = response_diagnostics(response, tuple(sensitive))
            diagnostics["category"] = "api_error"
            emit(
                "zzap_api_request_failed",
                warning=True,
                method=method,
                endpoint=endpoint,
                query=query,
                duration_seconds=time.monotonic() - started,
                **diagnostics,
            )
            raise error
        return payload


def _result_data(payload: dict[str, Any]) -> list[dict[str, Any]]:
    result = _result_object(payload)
    data = result.get("data")
    if not isinstance(data, list):
        raise ZZapApiError(200, "ZZap response result.data was not a list")
    if not all(isinstance(item, dict) for item in data):
        raise ZZapApiError(200, "ZZap response result.data contained invalid items")
    return data


def _result_object(payload: dict[str, Any]) -> dict[str, Any]:
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ZZapApiError(200, "ZZap response did not include result object")
    return result
