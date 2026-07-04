from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from app.clients.chatwoot import ChatwootApiError, ChatwootClient


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


@pytest.mark.asyncio
async def test_chatwoot_client_returns_contact_source_id() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/accounts/1/contacts"
        return httpx.Response(
            200,
            json={
                "payload": [
                    {
                        "id": 11,
                        "contact_inboxes": [
                            {"source_id": "source-1", "inbox": {"id": 2}},
                        ],
                    },
                ],
                "id": 11,
            },
        )

    client = ChatwootClient(
        base_url="https://chatwoot.example.test",
        account_id=1,
        api_token="token",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    contact = await client.create_contact(
        inbox_id=2,
        name="Alice",
        custom_attributes={"zzap_user_key": "user-1"},
    )

    assert contact.contact_id == 11
    assert contact.source_id == "source-1"


@pytest.mark.asyncio
async def test_chatwoot_client_rejects_contact_without_matching_inbox_source_id() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "payload": [
                    {
                        "id": 11,
                        "contact_inboxes": [
                            {"source_id": "wrong-source", "inbox": {"id": 999}},
                        ],
                    },
                ],
                "id": 11,
            },
        )

    client = ChatwootClient(
        base_url="https://chatwoot.example.test",
        account_id=1,
        api_token="token",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(ChatwootApiError):
        await client.create_contact(
            inbox_id=2,
            name="Alice",
            custom_attributes={"zzap_user_key": "user-1"},
        )


@pytest.mark.asyncio
async def test_chatwoot_client_creates_conversation_with_source_id() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/accounts/1/conversations"
        payload = await request.aread()
        assert b'"source_id":"source-1"' in payload
        return httpx.Response(200, json={"id": 12})

    client = ChatwootClient(
        base_url="https://chatwoot.example.test",
        account_id=1,
        api_token="token",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    conversation_id = await client.create_conversation(
        inbox_id=2,
        contact_id=11,
        source_id="source-1",
    )

    assert conversation_id == 12


@pytest.mark.asyncio
async def test_chatwoot_client_does_not_leak_token_to_external_attachment_url() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://storage.example.test/file.png"
        assert "api_access_token" not in request.headers
        return httpx.Response(200, content=b"file")

    client = ChatwootClient(
        base_url="https://chatwoot.example.test",
        account_id=1,
        api_token="token",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    content = await client.download_attachment("https://storage.example.test/file.png")

    assert content == b"file"


@pytest.mark.asyncio
async def test_chatwoot_client_resolves_relative_attachment_url_with_auth() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://chatwoot.example.test/rails/active_storage/file.png"
        assert request.headers["api_access_token"] == "token"
        return httpx.Response(200, content=b"file")

    client = ChatwootClient(
        base_url="https://chatwoot.example.test",
        account_id=1,
        api_token="token",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    content = await client.download_attachment("/rails/active_storage/file.png")

    assert content == b"file"


@pytest.mark.asyncio
async def test_chatwoot_client_redirects_attachment_without_leaking_auth() -> None:
    seen_requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_requests.append(request)
        if request.url.host == "chatwoot.example.test":
            assert request.headers["api_access_token"] == "token"
            return httpx.Response(
                302,
                headers={"location": "https://storage.example.test/file.png"},
            )
        assert request.url == "https://storage.example.test/file.png"
        assert "api_access_token" not in request.headers
        return httpx.Response(200, content=b"file")

    client = ChatwootClient(
        base_url="https://chatwoot.example.test",
        account_id=1,
        api_token="token",
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            follow_redirects=True,
        ),
    )

    content = await client.download_attachment("/rails/active_storage/file.png")

    assert content == b"file"
    assert len(seen_requests) == 2


@pytest.mark.asyncio
async def test_chatwoot_client_rejects_oversized_attachment_before_reading_body() -> None:
    stream = _ExplodingStream()

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-length": "11"},
            stream=stream,
        )

    client = ChatwootClient(
        base_url="https://chatwoot.example.test",
        account_id=1,
        api_token="token",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(ChatwootApiError, match="attachment exceeds maximum size"):
        await client.download_attachment("https://storage.example.test/file.png", max_bytes=10)

    assert stream.read_attempted is False


@pytest.mark.asyncio
async def test_chatwoot_client_wraps_invalid_json_response() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not-json")

    client = ChatwootClient(
        base_url="https://chatwoot.example.test",
        account_id=1,
        api_token="token",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(ChatwootApiError):
        await client.create_private_note(conversation_id=2, content="failed")


class _ExplodingStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.read_attempted = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        self.read_attempted = True
        raise AssertionError("body should not be read")
        yield b""
