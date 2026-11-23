import asyncio
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from pipeline.api.client import TronGridClient, wait_for_api
from pipeline.api.keypool import MultiKeyPool


@pytest.mark.parametrize("failures", [0, 1, 2])
def test_api_probe_stops_at_first_success(failures):
    client = Mock(_request=AsyncMock(side_effect=[OSError("network")] * failures + [{"blockID": "abc"}]))
    assert asyncio.run(wait_for_api(client)) is None
    assert client._request.await_count == failures + 1
    client._request.assert_awaited_with("/wallet/getnowblock", method="POST", json={})


@pytest.mark.parametrize("response", [{}, {"blockID": ""}, {"blockID": 7}, {"Error": "bad key"}])
def test_api_probe_rejects_invalid_success_payload(response):
    client = Mock(_request=AsyncMock(return_value=response))
    with pytest.raises(RuntimeError, match="after 2 attempts"):
        asyncio.run(wait_for_api(client, attempts=2))
    assert client._request.await_count == 2


def test_api_probe_exhaustion_has_safe_actionable_error():
    client = Mock(_request=AsyncMock(side_effect=OSError("secret-key")))
    with pytest.raises(RuntimeError, match="API keys and network") as caught:
        asyncio.run(wait_for_api(client, attempts=4))
    assert client._request.await_count == 4
    assert "secret-key" not in str(caught.value)
    assert caught.value.__suppress_context__


@pytest.mark.parametrize("attempts", [0, -1, True, 1.5, "3"])
def test_api_probe_rejects_invalid_attempts_before_network(attempts):
    client = Mock(_request=AsyncMock())
    with pytest.raises(ValueError, match="positive integer"):
        asyncio.run(wait_for_api(client, attempts))
    client._request.assert_not_awaited()


def test_api_probe_preserves_cancellation():
    client = Mock(_request=AsyncMock(side_effect=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(wait_for_api(client))
    assert client._request.await_count == 1


def test_api_probe_uses_real_client_authentication_and_post(monkeypatch):
    original = httpx.AsyncClient

    def handler(request):
        assert request.method == "POST"
        assert request.url.path == "/wallet/getnowblock"
        assert request.headers["TRON-PRO-API-KEY"] == "configured-key"
        return httpx.Response(200, json={"blockID": "abc"})

    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(handler)))

    async def run():
        client = TronGridClient(MultiKeyPool(["configured-key"], 60), "https://example.test", 1)
        try:
            await wait_for_api(client)
        finally:
            await client.aclose()

    asyncio.run(run())
