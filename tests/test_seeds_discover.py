import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from pipeline.api.client import TronGridClient, USDT_CONTRACT
from pipeline.api.keypool import MultiKeyPool
from pipeline.seeds.discover import discover_candidates, fetch_licit_seeds
from pipeline.storage.csv_store import CSVStore
from pipeline.storage.schema import build_headers
from pipeline.utils.address import to_base58, to_hex


ADDRESS = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
OTHER = "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb"
NOW = 1_700_000_000_000
SEED_ADDRESSES = (ADDRESS, OTHER, to_base58("41" + "11" * 20), to_base58("41" + "22" * 20))
CREATED_AT = "2026-11-22T14:00:00Z"


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


@pytest.fixture
def seed_dependencies(monkeypatch):
    client = SimpleNamespace(pool=SimpleNamespace(keys=["one", "two"]))
    discover = AsyncMock(return_value=set(SEED_ADDRESSES))
    verify = AsyncMock(return_value=True)
    monkeypatch.setattr("pipeline.seeds.discover.discover_candidates", discover)
    monkeypatch.setattr("pipeline.seeds.discover.verify_licit", verify)
    monkeypatch.setattr("pipeline.seeds.discover.now_iso", lambda: CREATED_AT)
    return client, discover, verify


def test_fetch_simultaneous_clean_results_return_exact_target_with_csv_fields(seed_dependencies):
    client, discover, verify = seed_dependencies
    rows = asyncio.run(fetch_licit_seeds(client, 1))
    assert len(rows) == 1
    row = rows[0]
    assert list(row) == build_headers()["seed_wallets.csv"]
    assert row["wallet_address"] in SEED_ADDRESSES
    assert row["hex_address"] == to_hex(row["wallet_address"])
    assert row["label"] == "0"
    assert row["transaction_id"] == ""
    assert row["source_name"] == "TronGrid_Licit_USDT"
    assert row["created_at"] == CREATED_AT
    assert verify.await_count == len(SEED_ADDRESSES)


def test_fetch_replenishes_after_rejections_without_rechecking_seen_addresses(seed_dependencies):
    client, discover, verify = seed_dependencies
    discover.side_effect = [set(SEED_ADDRESSES[:2]), set(SEED_ADDRESSES)]
    verify.side_effect = lambda c, address, blacklist: address in SEED_ADDRESSES[2:]
    rows = asyncio.run(fetch_licit_seeds(client, 2, concurrency=1))
    assert {row["wallet_address"] for row in rows} == set(SEED_ADDRESSES[2:])
    assert [call.args[1] for call in discover.await_args_list] == [2, 4]
    assert sorted(call.args[1] for call in verify.await_args_list) == sorted(SEED_ADDRESSES)


@pytest.mark.parametrize("key_count,expected_limit", [(0, 2), (1, 2), (2, 4)])
def test_fetch_verification_is_bounded_and_scales_with_key_count(
    seed_dependencies, monkeypatch, key_count, expected_limit
):
    client, discover, verify = seed_dependencies
    client.pool.keys = [f"key-{index}" for index in range(key_count)]
    active = 0
    peak = 0
    full = asyncio.Event()

    async def check(client, address, blacklist):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == expected_limit:
            full.set()
        try:
            await full.wait()
            return True
        finally:
            active -= 1

    monkeypatch.setattr("pipeline.seeds.discover.verify_licit", check)

    async def run():
        return await asyncio.wait_for(fetch_licit_seeds(client, 4, concurrency=2), 1)

    assert len(asyncio.run(run())) == 4
    assert peak == expected_limit
    assert active == 0


def test_fetch_target_completion_cancels_and_awaits_outstanding_verifications(
    seed_dependencies, monkeypatch
):
    client, discover, verify = seed_dependencies
    waiting = asyncio.Event()
    cleaned = []
    selected = sorted(SEED_ADDRESSES)[0]

    async def check(client, address, blacklist):
        if address == selected:
            await waiting.wait()
            return True
        waiting.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.append(address)

    monkeypatch.setattr("pipeline.seeds.discover.verify_licit", check)

    async def run():
        return await asyncio.wait_for(fetch_licit_seeds(client, 1, concurrency=2), 1)

    rows = asyncio.run(run())
    assert rows[0]["wallet_address"] == selected
    assert set(cleaned) == set(SEED_ADDRESSES) - {selected}


@pytest.mark.parametrize("available,clean,found", [(set(), False, 0), ({ADDRESS}, False, 0),
                                                  ({ADDRESS}, True, 1)])
def test_fetch_exhaustion_raises_instead_of_returning_partial_seeds(
    seed_dependencies, available, clean, found
):
    client, discover, verify = seed_dependencies
    discover.return_value = available
    verify.return_value = clean
    with pytest.raises(RuntimeError, match=f"found {found} before discovery exhausted"):
        asyncio.run(fetch_licit_seeds(client, 2))


def test_fetch_discovery_error_propagates_without_verification(seed_dependencies):
    client, discover, verify = seed_dependencies
    discover.side_effect = RuntimeError("discovery failed")
    with pytest.raises(RuntimeError, match="discovery failed"):
        asyncio.run(fetch_licit_seeds(client, 1))
    verify.assert_not_awaited()


def test_fetch_verification_error_cleans_up_other_tasks(seed_dependencies, monkeypatch):
    client, discover, verify = seed_dependencies
    waiting = asyncio.Event()
    active = set()
    selected = sorted(SEED_ADDRESSES)[0]

    async def check(client, address, blacklist):
        if address == selected:
            await waiting.wait()
            raise RuntimeError("verification failed")
        active.add(address)
        waiting.set()
        try:
            await asyncio.Event().wait()
        finally:
            active.remove(address)

    monkeypatch.setattr("pipeline.seeds.discover.verify_licit", check)
    with pytest.raises(RuntimeError, match="verification failed"):
        asyncio.run(fetch_licit_seeds(client, 1))
    assert not active


def test_fetch_caller_cancellation_cleans_up_verification_tasks(seed_dependencies, monkeypatch):
    client, discover, verify = seed_dependencies
    started = asyncio.Event()
    active = set()

    async def check(client, address, blacklist):
        active.add(address)
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            active.remove(address)

    monkeypatch.setattr("pipeline.seeds.discover.verify_licit", check)

    async def run():
        task = asyncio.create_task(fetch_licit_seeds(client, 1))
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not active

    asyncio.run(run())


@pytest.mark.parametrize("target,concurrency", [
    (-1, 1), (True, 1), (1.5, 1), (1, 0), (1, -1), (1, True), (1, 1.5),
])
def test_fetch_invalid_arguments_do_no_work(seed_dependencies, target, concurrency):
    client, discover, verify = seed_dependencies
    with pytest.raises(ValueError):
        asyncio.run(fetch_licit_seeds(client, target, concurrency))
    discover.assert_not_awaited()
    verify.assert_not_awaited()


def test_fetch_zero_target_does_no_network_or_blacklist_io(seed_dependencies, monkeypatch):
    client, discover, verify = seed_dependencies
    load = Mock(side_effect=AssertionError("zero target must not load files"))
    monkeypatch.setattr("pipeline.seeds.discover.load_local_blacklist", load)
    assert asyncio.run(fetch_licit_seeds(client, 0)) == []
    load.assert_not_called()
    discover.assert_not_awaited()
    verify.assert_not_awaited()


def test_fetch_real_client_blacklist_probes_and_store_integration(make_client, monkeypatch, tmp_path):
    monkeypatch.delenv("TRONSCAN_API_KEY", raising=False)
    blacklist = tmp_path / "blacklist.csv"
    blacklist.write_text(f"address\n{ADDRESS}\n", encoding="utf-8")
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"data": [event(ADDRESS, OTHER)]})
        assert request.url.path == "/wallet/triggerconstantcontract"
        return httpx.Response(200, json={
            "result": {"result": True}, "constant_result": ["0" * 64]
        })

    client = make_client(handler)
    rows = asyncio.run(fetch_licit_seeds(
        client, 1, concurrency=1,
        blacklist_paths=[str(blacklist), str(tmp_path / "missing.csv")],
    ))
    assert [row["wallet_address"] for row in rows] == [OTHER]
    assert len([request for request in requests if request.method == "POST"]) == 1
    store = CSVStore(str(tmp_path / "output"))
    store.insert_seeds(rows)
    reopened = CSVStore(str(tmp_path / "output"))
    assert set(reopened.seeds) == {OTHER}
    assert reopened.seeds[OTHER] == rows[0]
    assert "key-one" not in str(rows)
    assert "key-two" not in str(rows)
