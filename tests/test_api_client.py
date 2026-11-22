import asyncio

import httpx
import pytest

from pipeline.api.client import TronGridClient
from pipeline.api.keypool import MultiKeyPool


def test_constructor_configures_http2_and_scales_limits(monkeypatch):
    captured = {}
    original = httpx.AsyncClient

    def capture(**kwargs):
        captured.update(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", capture)
    pool = MultiKeyPool(["one", "two", "three"], 60)
    client = TronGridClient(pool, "https://example.test", 5)
    try:
        assert client.pool is pool
        assert client._client.base_url == httpx.URL("https://example.test")
        assert client._client.timeout == httpx.Timeout(5)
        assert captured["http2"] is True
        assert captured["limits"].max_connections == 44
        assert captured["limits"].max_keepalive_connections == 44
    finally:
        asyncio.run(client.aclose())


def test_explicit_limits_are_preserved_and_close_is_idempotent(monkeypatch):
    captured = {}
    original = httpx.AsyncClient

    def capture(**kwargs):
        captured.update(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", capture)
    limits = httpx.Limits(max_connections=7, max_keepalive_connections=2)
    client = TronGridClient(MultiKeyPool([], 60), "https://example.test", 3, limits)
    assert captured["limits"] is limits

    async def close():
        await client.aclose()
        assert client._client.is_closed
        await client.aclose()

    asyncio.run(close())


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_constructor_rejects_invalid_timeout(timeout):
    with pytest.raises(ValueError, match="finite and positive"):
        TronGridClient(MultiKeyPool([], 60), "https://example.test", timeout)


def test_empty_pool_has_workable_connection_limits(monkeypatch):
    captured = {}
    original = httpx.AsyncClient

    def capture(**kwargs):
        captured.update(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", capture)
    client = TronGridClient(MultiKeyPool([], 60), "https://example.test", 5)
    try:
        assert captured["limits"].max_connections == 20
        assert captured["limits"].max_keepalive_connections == 20
    finally:
        asyncio.run(client.aclose())
