from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import quote

import httpx


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


class ZZapApiError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


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
        encoded_user_key = quote(user_key, safe="")
        payload = await self._request_json(
            "GET",
            f"/api/client/v1/messages/{encoded_user_key}",
            params={"page": page, "page_size": page_size},
        )
        return [
            ZZapMessageDto(
                user_key=item.get("user_key"),
                user_name=item.get("user_name"),
                message_date=item.get("message_date"),
                message=item.get("message"),
                unread=item.get("unread"),
            )
            for item in _result_data(payload)
        ]

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
        response = await self._http.request(
            method,
            f"{self._base_url}{path}",
            headers={"zzap-api-key": self._api_key},
            **kwargs,
        )
        if response.status_code >= 400:
            raise ZZapApiError(response.status_code, response.text)
        try:
            payload = response.json()
        except ValueError as exc:
            raise ZZapApiError(response.status_code, "ZZap response was not valid JSON") from exc
        if not isinstance(payload, dict):
            raise ZZapApiError(response.status_code, "ZZap response was not a JSON object")
        if payload.get("success") is False:
            error_code = response.status_code
            try:
                error_code = int(payload.get("code") or response.status_code)
            except (TypeError, ValueError):
                pass
            raise ZZapApiError(
                error_code,
                str(payload.get("errors")),
            )
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
