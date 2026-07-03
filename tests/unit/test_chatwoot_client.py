from __future__ import annotations

import httpx
import pytest

from app.clients.chatwoot import ChatwootClient


@pytest.mark.asyncio
async def test_chatwoot_client_creates_private_note() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["api_access_token"] == "token"
        assert request.url.path == "/api/v1/accounts/1/conversations/2/messages"
        payload = await request.aread()
        assert b"private" in payload
        return httpx.Response(200, json={"id": 10})

    client = ChatwootClient(
        base_url="https://chatwoot.example.test",
        account_id=1,
        api_token="token",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    message_id = await client.create_private_note(conversation_id=2, content="failed")

    assert message_id == 10
