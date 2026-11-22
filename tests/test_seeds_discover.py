import asyncio

import httpx
import pytest

from pipeline.api.client import TronGridClient, USDT_CONTRACT
from pipeline.api.keypool import MultiKeyPool
from pipeline.seeds.discover import discover_candidates
from pipeline.utils.address import to_hex


ADDRESS = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
OTHER = "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb"
NOW = 1_700_000_000_000


@pytest.fixture
def make_client(monkeypatch):
    clients = []
    original = httpx.AsyncClient
    monkeypatch.setattr("pipeline.seeds.discover.time.time", lambda: NOW / 1000)

    def make(handler, keys=("key-one", "key-two")):
        transport = httpx.MockTransport(handler)
        monkeypatch.setattr(
            httpx, "AsyncClient", lambda **kwargs: original(transport=transport, **kwargs)
        )
        client = TronGridClient(
            MultiKeyPool(list(keys), 60, rate_limit_per_key=1_000_000),
            "https://example.test",
            5,
        )
        clients.append(client)
        return client

    yield make
    for client in clients:
        asyncio.run(client.aclose())


def event(sender, receiver):
    return {"result": {"from": sender, "to": receiver}}


def test_paginates_with_stable_filters_and_normalizes_unique_counterparties(make_client):
    requests = []

    def handler(request):
        requests.append(request)
        if "fingerprint" not in request.url.params:
            payload = {
                "data": [event(to_hex(ADDRESS), "invalid"), event(ADDRESS, "")],
                "meta": {"fingerprint": "next-page"},
            }
        else:
            payload = {"data": [event("", "0x" + to_hex(OTHER)[2:])]}
        return httpx.Response(200, json=payload)

    client = make_client(handler, keys=("key-one",))
    assert asyncio.run(discover_candidates(client, 2, limit_per_req=2)) == {ADDRESS, OTHER}
    assert len(requests) == 2
    first = dict(requests[0].url.params)
    assert first == {
        "event_name": "Transfer", "only_confirmed": "true",
        "order_by": "block_timestamp,desc", "limit": "2",
        "min_timestamp": "0", "max_timestamp": str(NOW),
    }
    assert dict(requests[1].url.params) == {**first, "fingerprint": "next-page"}
    assert requests[0].url.path == f"/v1/contracts/{USDT_CONTRACT}/events"


def test_disjoint_ranges_fetch_concurrently_using_multiple_pooled_keys(make_client):
    started = set()
    requests = []
    both_started = asyncio.Event()

    async def handler(request):
        requests.append(request)
        started.add(request.headers["TRON-PRO-API-KEY"])
        if len(requests) == 2:
            both_started.set()
        await both_started.wait()
        address = ADDRESS if request.url.params["min_timestamp"] == "0" else OTHER
        return httpx.Response(200, json={"data": [event(address, address)]})

    async def run():
        return await asyncio.wait_for(discover_candidates(make_client(handler), 2), 1)

    assert asyncio.run(run()) == {ADDRESS, OTHER}
    assert started == {"key-one", "key-two"}
    bounds = sorted(
        (int(r.url.params["min_timestamp"]), int(r.url.params["max_timestamp"]))
        for r in requests
    )
    assert bounds == [(0, NOW - 86_400_000), (NOW - 86_400_000 + 1, NOW)]


def test_target_completion_cancels_and_awaits_other_requests(make_client):
    waiting = asyncio.Event()
    cleaned = []

    async def handler(request):
        if request.url.params["min_timestamp"] == "0":
            waiting.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.append(True)
        await waiting.wait()
        return httpx.Response(200, json={"data": [event(ADDRESS, OTHER)]})

    async def run():
        result = await asyncio.wait_for(discover_candidates(make_client(handler), 1), 1)
        assert cleaned == [True]
        return result

    assert asyncio.run(run()) == {ADDRESS}


def test_simultaneous_successes_do_not_exceed_target(make_client):
    both_started = asyncio.Event()
    calls = []

    async def handler(request):
        calls.append(request)
        if len(calls) == 2:
            both_started.set()
        await both_started.wait()
        address = ADDRESS if request.url.params["min_timestamp"] == "0" else OTHER
        return httpx.Response(200, json={"data": [event(address, address)]})

    async def run():
        return await asyncio.wait_for(discover_candidates(make_client(handler), 1), 1)

    result = asyncio.run(run())
    assert len(calls) == 2
    assert len(result) == 1
    assert result <= {ADDRESS, OTHER}


@pytest.mark.parametrize("stop", [None, "", "repeat"])
def test_exhaustion_and_repeated_cursor_return_available_candidates(make_client, stop):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={
            "data": [event(ADDRESS, ADDRESS)], "meta": {"fingerprint": stop}
        })

    client = make_client(handler, keys=())
    assert asyncio.run(discover_candidates(client, 5)) == {ADDRESS}
    assert len(calls) == (2 if stop == "repeat" else 1)
    assert "TRON-PRO-API-KEY" not in calls[0].headers


@pytest.mark.parametrize("payload", [{}, {"data": []}, {"data": [{"result": None}]}])
def test_empty_or_unusable_events_return_empty_set(make_client, payload):
    client = make_client(lambda _: httpx.Response(200, json=payload))
    assert asyncio.run(discover_candidates(client, 1)) == set()


@pytest.mark.parametrize("payload", [
    {"data": None}, {"data": [None]}, {"data": [{}], "meta": None},
    {"data": [{}], "meta": {"fingerprint": 123}},
])
def test_malformed_pages_are_reported(make_client, payload):
    client = make_client(lambda _: httpx.Response(200, json=payload), keys=())
    with pytest.raises(ValueError):
        asyncio.run(discover_candidates(client, 1))


@pytest.mark.parametrize("target,limit", [(-1, 200), (True, 200), (1.5, 200), (1, 0),
                                         (1, 201), (1, True), (1, 1.5)])
def test_invalid_arguments_make_no_requests(make_client, target, limit):
    def handler(_):
        pytest.fail("invalid arguments must not send HTTP")

    with pytest.raises(ValueError):
        asyncio.run(discover_candidates(make_client(handler), target, limit))


def test_zero_target_makes_no_requests(make_client):
    def handler(_):
        pytest.fail("zero target must not send HTTP")

    assert asyncio.run(discover_candidates(make_client(handler), 0)) == set()


def test_request_failure_cleans_up_other_workers(make_client, monkeypatch):
    started = asyncio.Event()
    cleaned = []

    async def handler(request):
        if request.url.params["min_timestamp"] == "0":
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.append(True)
        await started.wait()
        raise RuntimeError("request failed")

    async def no_retry(fn, **kwargs):
        return await fn()

    monkeypatch.setattr("pipeline.api.client.async_retry", no_retry)
    with pytest.raises(RuntimeError, match="request failed"):
        asyncio.run(discover_candidates(make_client(handler), 2))
    assert cleaned == [True]


def test_caller_cancellation_cleans_up_all_workers(make_client):
    started = asyncio.Event()
    active = []

    async def handler(request):
        active.append(request)
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            active.remove(request)

    async def run():
        task = asyncio.create_task(discover_candidates(make_client(handler), 2))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not active

    asyncio.run(run())
