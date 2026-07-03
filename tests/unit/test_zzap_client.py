from __future__ import annotations

import httpx
import pytest

from app.clients.zzap import ZZapApiError, ZZapClient


@pytest.mark.asyncio
async def test_zzap_client_sends_api_key() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["zzap-api-key"] == "secret"
        return httpx.Response(
            200,
            json={"success": True, "result": {"data": []}, "result_info": {}},
        )

    client = ZZapClient(
        base_url="https://zzap.example.test",
        api_key="secret",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    result = await client.list_threads(page=1, page_size=100)

    assert result == []


@pytest.mark.asyncio
async def test_zzap_client_url_encodes_user_key_path_segment() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url).startswith(
            "https://zzap.example.test/api/client/v1/messages/user%2F1%3Fx%3D1?",
        )
        return httpx.Response(200, json={"success": True, "result": {"data": []}})

    client = ZZapClient(
        base_url="https://zzap.example.test",
        api_key="secret",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    result = await client.list_messages(user_key="user/1?x=1", page=1, page_size=100)

    assert result == []


@pytest.mark.asyncio
async def test_zzap_client_wraps_invalid_json_response() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not-json")

    client = ZZapClient(
        base_url="https://zzap.example.test",
        api_key="secret",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(ZZapApiError):
        await client.list_threads(page=1, page_size=100)
