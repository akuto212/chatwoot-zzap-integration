from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx


@dataclass(frozen=True)
class ChatwootContactDto:
    contact_id: int
    source_id: str


class ChatwootApiError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


class ChatwootClient:
    def __init__(
        self,
        *,
        base_url: str,
        account_id: int,
        api_token: str,
        http_client: httpx.AsyncClient,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._account_id = account_id
        self._api_token = api_token
        self._http = http_client

    async def create_contact(
        self,
        *,
        inbox_id: int,
        name: str,
        custom_attributes: dict[str, str],
    ) -> ChatwootContactDto:
        payload = await self._request_json(
            "POST",
            "/contacts",
            json={
                "inbox_id": inbox_id,
                "name": name,
                "custom_attributes": custom_attributes,
            },
        )
        return _contact_from_payload(payload, inbox_id=inbox_id)

    async def create_conversation(
        self,
        *,
        inbox_id: int,
        contact_id: int,
        source_id: str,
        status: str = "open",
    ) -> int:
        payload = await self._request_json(
            "POST",
            "/conversations",
            json={
                "source_id": source_id,
                "inbox_id": inbox_id,
                "contact_id": contact_id,
                "status": status,
            },
        )
        return _required_int(payload, "id")

    async def update_conversation_status(self, *, conversation_id: int, status: str) -> None:
        await self._request_json(
            "POST",
            f"/conversations/{conversation_id}/toggle_status",
            json={"status": status},
        )

    async def create_incoming_message(self, *, conversation_id: int, content: str) -> int:
        payload = await self._request_json(
            "POST",
            f"/conversations/{conversation_id}/messages",
            json={"content": content, "message_type": "incoming"},
        )
        return _required_int(payload, "id")

    async def create_private_note(self, *, conversation_id: int, content: str) -> int:
        payload = await self._request_json(
            "POST",
            f"/conversations/{conversation_id}/messages",
            json={"content": content, "private": True},
        )
        return _required_int(payload, "id")

    async def download_attachment(self, url: str) -> bytes:
        resolved_url = urljoin(f"{self._base_url}/", url)
        parsed_url = urlparse(resolved_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ChatwootApiError(0, "invalid attachment URL")

        base_url = urlparse(self._base_url)
        headers = (
            self._headers()
            if (parsed_url.scheme, parsed_url.netloc) == (base_url.scheme, base_url.netloc)
            else {}
        )
        response = await self._http.get(resolved_url, headers=headers)
        if response.status_code >= 400:
            raise ChatwootApiError(response.status_code, response.text)
        return response.content

    async def _request_json(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = await self._http.request(
            method,
            f"{self._base_url}/api/v1/accounts/{self._account_id}{path}",
            headers=self._headers(),
            **kwargs,
        )
        if response.status_code >= 400:
            raise ChatwootApiError(response.status_code, response.text)
        try:
            payload = response.json()
        except ValueError as exc:
            raise ChatwootApiError(
                response.status_code,
                "Chatwoot response was not valid JSON",
            ) from exc
        if not isinstance(payload, dict):
            raise ChatwootApiError(response.status_code, "Chatwoot response was not a JSON object")
        return payload

    def _headers(self) -> dict[str, str]:
        return {"api_access_token": self._api_token}


def _contact_from_payload(payload: dict[str, Any], *, inbox_id: int) -> ChatwootContactDto:
    contact = _contact_object(payload)
    contact_id = _required_int(contact, "id", fallback=payload.get("id"))
    source_id = _source_id_for_inbox(contact, inbox_id=inbox_id)
    if not source_id:
        raise ChatwootApiError(200, "Chatwoot contact response did not include source_id")
    return ChatwootContactDto(contact_id=contact_id, source_id=source_id)


def _contact_object(payload: dict[str, Any]) -> dict[str, Any]:
    payload_value = payload.get("payload")
    if isinstance(payload_value, list) and payload_value and isinstance(payload_value[0], dict):
        return payload_value[0]
    if isinstance(payload_value, dict):
        nested_contact = payload_value.get("contact")
        if isinstance(nested_contact, dict):
            return nested_contact
        return payload_value
    return payload


def _source_id_for_inbox(contact: dict[str, Any], *, inbox_id: int) -> str | None:
    first_source_id: str | None = None
    contact_inboxes = contact.get("contact_inboxes") or []
    if not isinstance(contact_inboxes, list):
        return None

    for contact_inbox in contact_inboxes:
        if not isinstance(contact_inbox, dict):
            continue
        source_id = contact_inbox.get("source_id")
        if not source_id:
            continue
        first_source_id = first_source_id or str(source_id)
        inbox = contact_inbox.get("inbox") or {}
        if not isinstance(inbox, dict):
            continue
        try:
            if int(inbox.get("id") or 0) == inbox_id:
                return str(source_id)
        except (TypeError, ValueError):
            continue
    return first_source_id


def _required_int(payload: dict[str, Any], key: str, *, fallback: Any = None) -> int:
    value = payload.get(key, fallback)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ChatwootApiError(200, f"Chatwoot response did not include integer {key}") from exc
