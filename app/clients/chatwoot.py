from __future__ import annotations

from typing import Any

import httpx


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
    ) -> int:
        payload = await self._request_json(
            "POST",
            "/contacts",
            json={
                "inbox_id": inbox_id,
                "name": name,
                "custom_attributes": custom_attributes,
            },
        )
        return int(payload["payload"]["contact"]["id"] if "payload" in payload else payload["id"])

    async def create_conversation(
        self,
        *,
        inbox_id: int,
        contact_id: int,
        status: str = "open",
    ) -> int:
        payload = await self._request_json(
            "POST",
            "/conversations",
            json={"inbox_id": inbox_id, "contact_id": contact_id, "status": status},
        )
        return int(payload["id"])

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
        return int(payload["id"])

    async def create_private_note(self, *, conversation_id: int, content: str) -> int:
        payload = await self._request_json(
            "POST",
            f"/conversations/{conversation_id}/messages",
            json={"content": content, "private": True},
        )
        return int(payload["id"])

    async def download_attachment(self, url: str) -> bytes:
        response = await self._http.get(url, headers=self._headers())
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
        return response.json()

    def _headers(self) -> dict[str, str]:
        return {"api_access_token": self._api_token}
