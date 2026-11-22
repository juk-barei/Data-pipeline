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


@pytest.mark.parametrize("cap", [1, 3, 4])
def test_trx_history_paginates_with_fixed_filters_and_exact_cap(mock_client, monkeypatch, cap):
    monkeypatch.setattr("pipeline.api.client.MAX_RECORDS_PER_ADDRESS", cap)
    observed = []

    def handler(request):
        observed.append(dict(request.url.params))
        assert request.url.path == "/v1/accounts/wallet/transactions"
        assert request.url.params["only_confirmed"] == "true"
        assert request.url.params["limit"] == "200"
        page = len(observed)
        return httpx.Response(200, json={"data": [{"txID": f"{page}-a"}, {"txID": f"{page}-b"}], "meta": {"fingerprint": f"page-{page}"}})

    result = asyncio.run(mock_client(handler).get_trx_transactions("wallet"))
    assert len(result) == cap
    assert len(observed) == (cap + 1) // 2
    assert "fingerprint" not in observed[0]
    if cap > 2:
        assert observed[1]["fingerprint"] == "page-1"


@pytest.mark.parametrize("payload", [{}, {"data": []}, {"data": [], "meta": {"fingerprint": "unused"}}])
def test_trx_history_empty_stops_without_more_requests(mock_client, payload):
    handler = Mock(return_value=httpx.Response(200, json=payload))
    assert asyncio.run(mock_client(handler).get_trx_transactions("wallet")) == []
    assert handler.call_count == 1


def test_trx_history_stops_on_missing_fingerprint(mock_client):
    handler = Mock(return_value=httpx.Response(200, json={"data": [{"txID": "one"}]}))
    assert asyncio.run(mock_client(handler).get_trx_transactions("wallet")) == [{"txID": "one"}]
    assert handler.call_count == 1


def test_trx_history_stops_on_repeated_fingerprint(mock_client):
    handler = Mock(return_value=httpx.Response(200, json={"data": [{"txID": "one"}], "meta": {"fingerprint": "same"}}))
    asyncio.run(mock_client(handler).get_trx_transactions("wallet"))
    assert handler.call_count == 2


@pytest.mark.parametrize("payload", [{"data": None}, {"data": {}}, {"data": [None]}, {"data": [{}], "meta": []}, {"data": [{}], "meta": {"fingerprint": 42}}, *[{"data": [{}], "meta": {"fingerprint": value}} for value in [0, False, [], {}]]])
def test_trx_history_rejects_malformed_pages(mock_client, payload):
    client = mock_client(lambda request: httpx.Response(200, json=payload))
    with pytest.raises(ValueError):
        asyncio.run(client.get_trx_transactions("wallet"))


def test_trx_history_propagates_later_page_failure(mock_client):
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(200, json={"data": [{"txID": "one"}], "meta": {"fingerprint": "next"}})
        return httpx.Response(503)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(mock_client(handler).get_trx_transactions("wallet"))
    assert len(calls) == 4


@pytest.mark.parametrize("cap", [1, 3, 4])
def test_trc20_history_paginates_with_fixed_filters_and_exact_cap(mock_client, monkeypatch, cap):
    monkeypatch.setattr("pipeline.api.client.MAX_RECORDS_PER_ADDRESS", cap)
    observed = []

    def handler(request):
        observed.append(dict(request.url.params))
        assert request.url.path == "/v1/accounts/wallet/transactions/trc20"
        assert request.url.params["only_confirmed"] == "true"
        assert request.url.params["limit"] == "200"
        assert request.url.params["contract_address"] == "usdt-contract"
        page = len(observed)
        return httpx.Response(200, json={"data": [{"transaction_id": f"{page}-a"}, {"transaction_id": f"{page}-b"}], "meta": {"fingerprint": f"page-{page}"}})

    result = asyncio.run(mock_client(handler).get_trc20_transfers("wallet", "usdt-contract"))
    assert len(result) == cap
    assert len(observed) == (cap + 1) // 2
    assert "fingerprint" not in observed[0]
    if cap > 2:
        assert observed[1]["fingerprint"] == "page-1"


@pytest.mark.parametrize("payload", [{}, {"data": []}, {"data": [], "meta": {"fingerprint": "unused"}}])
def test_trc20_history_empty_stops_without_more_requests(mock_client, payload):
    handler = Mock(return_value=httpx.Response(200, json=payload))
    assert asyncio.run(mock_client(handler).get_trc20_transfers("wallet", "usdt-contract")) == []
    assert handler.call_count == 1


def test_trc20_history_stops_on_missing_fingerprint(mock_client):
    handler = Mock(return_value=httpx.Response(200, json={"data": [{"transaction_id": "one"}]}))
    assert asyncio.run(mock_client(handler).get_trc20_transfers("wallet", "usdt-contract")) == [{"transaction_id": "one"}]
    assert handler.call_count == 1


def test_trc20_history_stops_on_repeated_fingerprint(mock_client):
    handler = Mock(return_value=httpx.Response(200, json={"data": [{"transaction_id": "one"}], "meta": {"fingerprint": "same"}}))
    asyncio.run(mock_client(handler).get_trc20_transfers("wallet", "usdt-contract"))
    assert handler.call_count == 2


@pytest.mark.parametrize("payload", [{"data": None}, {"data": {}}, {"data": [None]}, {"data": [{}], "meta": []}, {"data": [{}], "meta": {"fingerprint": 42}}, *[{"data": [{}], "meta": {"fingerprint": value}} for value in [0, False, [], {}]]])
def test_trc20_history_rejects_malformed_pages(mock_client, payload):
    client = mock_client(lambda request: httpx.Response(200, json=payload))
    with pytest.raises(ValueError):
        asyncio.run(client.get_trc20_transfers("wallet", "usdt-contract"))


def test_trc20_history_propagates_later_page_failure(mock_client):
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(200, json={"data": [{"transaction_id": "one"}], "meta": {"fingerprint": "next"}})
        return httpx.Response(503)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(mock_client(handler).get_trc20_transfers("wallet", "usdt-contract"))
    assert len(calls) == 4


@pytest.mark.parametrize("limit", [0, 1, 200, 201, 401])
def test_recent_usdt_events_respect_limit_and_keep_filters(mock_client, limit):
    from pipeline.api.client import USDT_CONTRACT

    requests = []

    def handler(request):
        requests.append(dict(request.url.params))
        assert request.url.path == f"/v1/contracts/{USDT_CONTRACT}/events"
        assert request.url.params["event_name"] == "Transfer"
        assert request.url.params["only_confirmed"] == "true"
        assert request.url.params["order_by"] == "block_timestamp,desc"
        assert request.url.params["limit"] == str(min(limit, 200))
        page = len(requests)
        return httpx.Response(200, json={"data": [{"transaction_id": str(i)} for i in range((page-1)*200, page*200)], "meta": {"fingerprint": f"page-{page}"}})

    result = asyncio.run(mock_client(handler).get_recent_usdt_transfers(limit))
    assert len(result) == limit
    assert len(requests) == (limit + 199) // 200
    if limit > 200:
        assert requests[1]["fingerprint"] == "page-1"


@pytest.mark.parametrize("limit", [-1, True, 1.5, "2", None])
def test_recent_usdt_events_reject_invalid_limits_without_requests(mock_client, limit):
    handler = Mock(return_value=httpx.Response(200, json={}))
    with pytest.raises(ValueError, match="nonnegative integer"):
        asyncio.run(mock_client(handler).get_recent_usdt_transfers(limit))
    assert handler.call_count == 0


@pytest.mark.parametrize("payload, expected_calls", [({}, 1), ({"data": []}, 1), ({"data": [{}]}, 1), ({"data": [{}], "meta": {"fingerprint": "same"}}, 2)])
def test_recent_usdt_events_stop_without_progress(mock_client, payload, expected_calls):
    handler = Mock(return_value=httpx.Response(200, json=payload))
    result = asyncio.run(mock_client(handler).get_recent_usdt_transfers(1000))
    assert handler.call_count == expected_calls
    assert len(result) <= 1000


@pytest.mark.parametrize("payload", [{"data": None}, {"data": [None]}, {"data": [{}], "meta": []}, {"data": [{}], "meta": {"fingerprint": 1}}, *[{"data": [{}], "meta": {"fingerprint": value}} for value in [0, False, [], {}]]])
def test_recent_usdt_events_reject_malformed_response(mock_client, payload):
    with pytest.raises(ValueError):
        asyncio.run(mock_client(lambda request: httpx.Response(200, json=payload)).get_recent_usdt_transfers(10))
