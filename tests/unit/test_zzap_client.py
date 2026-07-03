from __future__ import annotations

import httpx
import pytest

from app.clients.zzap import ZZapClient


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
