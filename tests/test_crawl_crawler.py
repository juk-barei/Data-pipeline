import asyncio
import functools
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from pipeline.api.client import USDT_CONTRACT, TronGridClient
from pipeline.api.keypool import MultiKeyPool
from pipeline.config import Settings
from pipeline.crawl.crawler import CrawlerWorker
from pipeline.utils.async_utils import async_retry
from pipeline.utils.address import to_hex

ADDRESS = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
OTHER = "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb"


def make_worker(*, keys=("one", "two"), settings=None, max_concurrency=None):
    pool = MultiKeyPool(list(keys), 60)
    client = SimpleNamespace(pool=pool)
    return CrawlerWorker(Mock(), client, settings or Settings(), max_concurrency)


def test_constructor_wires_dependencies_without_io():
    worker = make_worker()
    assert worker.store.mock_calls == []
    assert worker.settings == Settings()
    assert worker.client.pool.keys == ["one", "two"]


def test_automatic_limit_scales_with_pool_rate_and_honors_settings_minimum():
    many = make_worker(keys=tuple(str(i) for i in range(10)))
    assert many.max_concurrency == 49
    assert make_worker().max_concurrency == Settings().max_concurrency
    fast_pool = MultiKeyPool(["one"], 60, rate_limit_per_key=90)
    assert CrawlerWorker(Mock(), SimpleNamespace(pool=fast_pool), Settings()).max_concurrency == 30


def test_explicit_limit_overrides_autosizing_and_public_mode_defaults_to_one():
    assert make_worker(max_concurrency=2).max_concurrency == 2
    assert make_worker(keys=()).max_concurrency == 1
    assert make_worker(keys=(), max_concurrency=3).max_concurrency == 3


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "2"])
def test_invalid_explicit_limit_is_rejected(limit):
    with pytest.raises(ValueError, match="positive integer"):
        make_worker(max_concurrency=limit)


def test_worker_semaphores_are_independent():
    async def check():
        first = make_worker(max_concurrency=1)
        second = make_worker(max_concurrency=1)
        async with first.semaphore:
            assert first.semaphore.locked()
            assert not second.semaphore.locked()
        assert not first.semaphore.locked()

    asyncio.run(check())


def test_fetch_wallet_multiplexes_real_client_keys_and_penalizes_429(monkeypatch):
    observed = []
    original = httpx.AsyncClient

    def handler(request):
        key = request.headers["TRON-PRO-API-KEY"]
        observed.append((request.url.path, key))
        if key == "bad":
            return httpx.Response(429)
        if request.url.path.endswith("/transactions/trc20"):
            assert request.url.params["contract_address"] == USDT_CONTRACT
            return httpx.Response(200, json={"data": [{"transaction_id": "token"}]})
        if request.url.path.endswith("/transactions"):
            return httpx.Response(200, json={"data": [{"txID": "native"}]})
        return httpx.Response(200, json={"data": [{"address": ADDRESS}]})

    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: original(
        transport=httpx.MockTransport(handler), **kw
    ))
    monkeypatch.setattr("pipeline.api.client.async_retry", functools.partial(
        async_retry, base_delay=0
    ))
    pool = MultiKeyPool(["bad", "good", "other"], 60, 1_000_000)

    async def check():
        client = TronGridClient(pool, "https://example.test", 5)
        try:
            worker = CrawlerWorker(Mock(), client, Settings())
            result = await worker._fetch_wallet(ADDRESS)
            assert result == (ADDRESS, {"address": ADDRESS},
                              [{"txID": "native"}], [{"transaction_id": "token"}])
        finally:
            await client.aclose()

    asyncio.run(check())
    assert set(pool.cooldown.cooldowns) == {"bad"}
    assert {key for _, key in observed} == {"bad", "good", "other"}


@pytest.mark.parametrize("failed_method", [
    "get_account", "get_trx_transactions", "get_trc20_transfers"
])
def test_fetch_wallet_keeps_successes_when_one_endpoint_fails(failed_method):
    worker = make_worker()
    client = worker.client
    client.get_account = AsyncMock(return_value={"address": ADDRESS})
    client.get_trx_transactions = AsyncMock(return_value=[])
    client.get_trc20_transfers = AsyncMock(return_value=[])
    failure = httpx.ConnectError("offline")
    getattr(client, failed_method).side_effect = failure
    result = asyncio.run(worker._fetch_wallet(ADDRESS))
    expected = [ADDRESS, {"address": ADDRESS}, [], []]
    expected[["get_account", "get_trx_transactions", "get_trc20_transfers"].index(failed_method) + 1] = failure
    assert result == tuple(expected)
    client.get_account.assert_awaited_once_with(ADDRESS)
    client.get_trx_transactions.assert_awaited_once_with(ADDRESS)
    client.get_trc20_transfers.assert_awaited_once_with(ADDRESS, USDT_CONTRACT)


def test_fetch_wallet_limits_concurrent_wallets():
    async def check():
        worker = make_worker(max_concurrency=2)
        active = 0
        peak = 0

        async def endpoint(*args):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1
            return []

        worker.client.get_account = endpoint
        worker.client.get_trx_transactions = endpoint
        worker.client.get_trc20_transfers = endpoint
        results = await asyncio.gather(*(worker._fetch_wallet(str(i)) for i in range(7)))
        assert len(results) == 7
        assert peak == 6  # Three endpoints per wallet, two permitted wallets.
        assert active == 0

    asyncio.run(check())


def test_fetch_wallet_cancellation_drains_requests_and_releases_permit():
    async def check():
        worker = make_worker(max_concurrency=1)
        started = asyncio.Event()
        active = 0

        async def endpoint(*args):
            nonlocal active
            active += 1
            if active == 3:
                started.set()
            try:
                await asyncio.Event().wait()
            finally:
                active -= 1

        worker.client.get_account = endpoint
        worker.client.get_trx_transactions = endpoint
        worker.client.get_trc20_transfers = endpoint
        task = asyncio.create_task(worker._fetch_wallet(ADDRESS))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert active == 0
        assert not worker.semaphore.locked()

    asyncio.run(check())


@pytest.fixture
def raw_histories():
    return (
        {"address": ADDRESS, "create_time": 1700000000000,
         "latest_opration_time": 1700000060000},
        [{"txID": "native", "block_timestamp": 1700000000000,
          "raw_data": {"contract": [{"type": "TransferContract", "parameter": {
              "value": {"owner_address": to_hex(ADDRESS),
                        "to_address": to_hex(OTHER), "amount": 1234567}
          }}]}}],
        [{"transaction_id": "token", "block_timestamp": 1700000000000,
          "from": OTHER, "to": ADDRESS, "value": "1000001",
          "token_info": {"address": USDT_CONTRACT, "decimals": 6, "symbol": "USDT"}}],
    )


def test_normalize_uses_parsers_and_deduplicates_transactions(raw_histories):
    account, native, tokens = raw_histories
    row, txs, synced = make_worker()._normalize(account, native * 2, tokens * 2)
    assert synced is True
    assert row["address"] == ADDRESS
    assert row["last_seen_timestamp"] == "2023-11-14T22:14:20Z"
    assert [(tx["tx_hash"], tx["amount"]) for tx in txs] == [
        ("native", "1.234567"), ("token", "1.000001")
    ]
    assert txs[0]["from_address"] == ADDRESS
    assert txs[1]["from_address"] == OTHER


@pytest.mark.parametrize("slot", [0, 1, 2])
@pytest.mark.parametrize("failure", [httpx.ConnectError("offline"),
                                         RuntimeError("throttled")])
def test_normalize_preserves_partial_data_but_not_synced(raw_histories, slot, failure):
    raw = list(raw_histories)
    raw[slot] = failure
    row, txs, synced = make_worker()._normalize(*raw)
    assert synced is False
    assert bool(row) is (slot != 0)
    assert {tx["tx_hash"] for tx in txs} == (
        {"native", "token"} if slot == 0 else {"token"} if slot == 1 else {"native"}
    )


def test_normalize_unknown_account_and_empty_histories_are_successful():
    assert make_worker()._normalize(None, [], []) == ({}, [], True)


@pytest.mark.parametrize("raw", [([], [], []), ({}, [], []),
                                      (None, None, []), (None, [], {}),
                                      (None, [None], [])])
def test_normalize_malformed_response_cannot_mark_synced(raw):
    assert make_worker()._normalize(*raw)[2] is False


def test_normalize_skips_nontransfer_and_missing_identity_records(raw_histories):
    account, native, tokens = raw_histories
    native[0]["raw_data"]["contract"][0]["type"] = "TriggerSmartContract"
    tokens[0].pop("transaction_id")
    assert make_worker()._normalize(account, native, tokens)[1:] == ([], True)
