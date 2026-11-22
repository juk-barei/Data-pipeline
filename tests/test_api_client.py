import asyncio
import functools
import itertools
from unittest.mock import Mock

import httpx
import pytest

from pipeline.api.client import TronGridClient
from pipeline.api.keypool import MultiKeyPool
from pipeline.utils.async_utils import async_retry


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


@pytest.fixture
def mock_client(monkeypatch):
    """Use real HTTPX request handling without sockets or retry delays."""
    clients = []
    original = httpx.AsyncClient
    monkeypatch.setattr(
        "pipeline.api.client.async_retry", functools.partial(async_retry, base_delay=0)
    )

    def make(handler, keys=("one", "two")):
        transport = httpx.MockTransport(handler)
        monkeypatch.setattr(
            httpx, "AsyncClient", lambda **kwargs: original(transport=transport, **kwargs)
        )
        pool = MultiKeyPool(list(keys), 60, rate_limit_per_key=1_000_000)
        client = TronGridClient(pool, "https://example.test", 5)
        clients.append(client)
        return client

    yield make
    for client in clients:
        asyncio.run(client.aclose())


def test_request_uses_pooled_key_and_query_params(mock_client):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"data": [{"address": "example"}]})

    client = mock_client(handler)
    result = asyncio.run(client._request("/v1/accounts/example", {"limit": 10}))
    assert result == {"data": [{"address": "example"}]}
    assert requests[0].method == "GET"
    assert requests[0].url.params["limit"] == "10"
    assert requests[0].headers["TRON-PRO-API-KEY"] == "one"
    assert "TRON-PRO-API-KEY" not in client._client.headers


def test_request_429_penalizes_only_failing_key_without_retry_backoff(mock_client, monkeypatch):
    used_keys = []

    def handler(request):
        key = request.headers["TRON-PRO-API-KEY"]
        used_keys.append(key)
        return httpx.Response(429 if key == "one" else 200, json={"ok": True})

    async def run_once(fn, **kwargs):
        return await fn()

    monkeypatch.setattr("pipeline.api.client.async_retry", run_once)
    client = mock_client(handler)
    assert asyncio.run(client._request("/test")) == {"ok": True}
    assert used_keys == ["one", "two"]
    assert set(client.pool.cooldown.cooldowns) == {"one"}


def test_request_404_is_empty_without_retry(mock_client):
    handler = Mock(return_value=httpx.Response(404))
    assert asyncio.run(mock_client(handler)._request("/missing")) == {}
    assert handler.call_count == 1


def test_request_public_mode_omits_api_key(mock_client):
    def handler(request):
        assert "TRON-PRO-API-KEY" not in request.headers
        return httpx.Response(200, json={})

    assert asyncio.run(mock_client(handler, keys=())._request("/test")) == {}


@pytest.mark.parametrize("failure", ["network", "500", "invalid-json", "list-json"])
def test_request_retries_errors_and_recovers(mock_client, failure):
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            if failure == "network":
                raise httpx.ConnectError("offline", request=request)
            if failure == "500":
                return httpx.Response(500)
            if failure == "invalid-json":
                return httpx.Response(200, content=b"not json")
            return httpx.Response(200, json=[])
        return httpx.Response(200, json={"ok": True})

    assert asyncio.run(mock_client(handler)._request("/test")) == {"ok": True}
    assert len(calls) == 2


@pytest.mark.parametrize("keys", [(), ("one",), ("one", "two")])
def test_request_all_keys_throttled_has_bounded_attempts(mock_client, keys):
    handler = Mock(return_value=httpx.Response(429))
    client = mock_client(handler, keys=keys)
    rotation = itertools.cycle(keys or ("",))

    async def acquire():
        return next(rotation)

    client.pool.get_key = acquire
    client.pool.penalize = Mock()
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(client._request("/test"))
    assert handler.call_count == 3 * max(1, len(keys))
    assert client.pool.penalize.call_count == handler.call_count


def test_request_exhausts_other_http_errors(mock_client):
    handler = Mock(return_value=httpx.Response(503))
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(mock_client(handler)._request("/test"))
    assert handler.call_count == 3


def test_request_supports_post_json(mock_client):
    def handler(request):
        assert request.method == "POST"
        assert request.content == b'{"parameter":"encoded"}'
        assert request.headers["TRON-PRO-API-KEY"] == "one"
        return httpx.Response(200, json={"constant_result": ["01"]})

    client = mock_client(handler)
    assert asyncio.run(client._request("/wallet/triggerconstantcontract", method="POST", json={"parameter": "encoded"})) == {"constant_result": ["01"]}


def test_request_cancellation_propagates_without_retry(mock_client):
    handler = Mock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(mock_client(handler)._request("/test"))
    assert handler.call_count == 1


def test_concurrent_requests_keep_their_own_key_headers(mock_client):
    observed = []

    async def handler(request):
        key = request.headers["TRON-PRO-API-KEY"]
        await asyncio.sleep(0)
        observed.append((request.url.path, key))
        return httpx.Response(200, json={"key": key})

    client = mock_client(handler)

    async def request_both():
        return await asyncio.gather(client._request("/a"), client._request("/b"))

    assert asyncio.run(request_both()) == [{"key": "one"}, {"key": "two"}]
    assert observed == [("/a", "one"), ("/b", "two")]


@pytest.mark.parametrize("payload", [{}, {"data": []}])
def test_get_account_unknown_returns_none(mock_client, payload):
    client = mock_client(lambda request: httpx.Response(200, json=payload))
    assert asyncio.run(client.get_account("wallet")) is None


def test_get_account_returns_first_record(mock_client):
    def handler(request):
        assert request.url.path == "/v1/accounts/wallet"
        return httpx.Response(200, json={"data": [{"address": "first"}, {"address": "second"}]})

    client = mock_client(handler)
    assert asyncio.run(client.get_account("wallet")) == {"address": "first"}


def test_get_account_404_returns_none(mock_client):
    client = mock_client(lambda request: httpx.Response(404))
    assert asyncio.run(client.get_account("missing")) is None


@pytest.mark.parametrize("data", [None, {}, "bad", [None]])
def test_get_account_rejects_malformed_data(mock_client, data):
    client = mock_client(lambda request: httpx.Response(200, json={"data": data}))
    with pytest.raises(ValueError, match="account data"):
        asyncio.run(client.get_account("wallet"))


def test_get_account_encodes_path_input(mock_client):
    def handler(request):
        assert request.url.raw_path == b"/v1/accounts/a%2Fb%3Fc"
        return httpx.Response(200, json={"data": []})

    assert asyncio.run(mock_client(handler).get_account("a/b?c")) is None
