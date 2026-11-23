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
from pipeline.storage.csv_store import CSVStore
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


def test_detect_supernodes_counts_unique_transactions_and_includes_boundary():
    worker = make_worker(settings=Settings(supernode_threshold=2))
    shared = {"tx_hash": "shared", "from_address": ADDRESS, "to_address": OTHER}
    rows = [shared, dict(shared),
            {"tx_hash": "self", "from_address": ADDRESS, "to_address": ADDRESS},
            {"tx_hash": "outside", "from_address": "unclaimed", "to_address": "unclaimed"}]
    assert worker._detect_supernodes(rows, [ADDRESS, OTHER, ADDRESS]) == [ADDRESS]
    assert make_worker(settings=Settings(supernode_threshold=1))._detect_supernodes(
        rows, [ADDRESS, OTHER]
    ) == [ADDRESS, OTHER]
    assert make_worker(settings=Settings(supernode_threshold=3))._detect_supernodes(
        rows, [ADDRESS, OTHER]
    ) == []


def test_detect_supernodes_handles_empty_batch_and_missing_transaction_hash():
    worker = make_worker(settings=Settings(supernode_threshold=1))
    assert worker._detect_supernodes([], [ADDRESS]) == []
    assert worker._detect_supernodes([
        {"from_address": ADDRESS, "to_address": OTHER}
    ], [ADDRESS]) == []
    assert worker._detect_supernodes([
        {"tx_hash": "outside", "from_address": OTHER, "to_address": OTHER}
    ], []) == []


def test_run_batch_idle_does_not_write():
    worker = make_worker()
    worker.store.claim_crawl_batch.return_value = []
    assert asyncio.run(worker.run_batch()) == dict(
        claimed=0, synced=0, failed=0, accounts=0, transactions=0, supernodes=0
    )
    worker.store.upsert_crawled_data.assert_not_called()


def test_run_batch_persists_partial_failure_and_retry_is_idempotent(tmp_path, raw_histories):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}, {"wallet_address": OTHER}])
    account, native, tokens = raw_histories
    failed = True

    async def get_account(address):
        return {**account, "address": address}

    async def get_tokens(address, contract):
        if address == ADDRESS and failed:
            raise httpx.ConnectError("temporary failure")
        return tokens

    client = SimpleNamespace(pool=MultiKeyPool(["one"], 60),
                             get_account=get_account,
                             get_trx_transactions=AsyncMock(return_value=native),
                             get_trc20_transfers=get_tokens)
    worker = CrawlerWorker(store, client, Settings(batch_size=2, supernode_threshold=2))
    first = asyncio.run(worker.run_batch())
    assert first == dict(claimed=2, synced=1, failed=1, accounts=2, transactions=2, supernodes=2)
    reopened = CSVStore(str(tmp_path))
    assert reopened.crawl_state[ADDRESS]["synced"] is False
    assert reopened.crawl_state[OTHER]["synced"] is True
    assert reopened.crawl_state[ADDRESS]["is_supernode"] is True
    assert reopened.tx_hashes == {"native", "token"}
    assert reopened.accounts[ADDRESS]["last_seen_timestamp"] == "2023-11-14T22:14:20Z"
    failed = False
    retry = asyncio.run(worker.run_batch())
    assert retry["claimed"] == retry["synced"] == 1
    assert retry["failed"] == 0
    assert CSVStore(str(tmp_path)).tx_hashes == {"native", "token"}
    assert store.crawl_state[ADDRESS]["synced"] is True


def test_run_batch_serializes_overlapping_calls(tmp_path):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    worker = make_worker()
    worker.store = store
    worker.client.get_account = AsyncMock(return_value=None)
    worker.client.get_trx_transactions = AsyncMock(return_value=[])
    worker.client.get_trc20_transfers = AsyncMock(return_value=[])

    async def check():
        first, second = await asyncio.gather(worker.run_batch(), worker.run_batch())
        assert first["synced"] == 1
        assert second["claimed"] == 0

    asyncio.run(check())
    worker.client.get_account.assert_awaited_once()


def test_run_batch_write_failure_propagates_and_does_not_mark_synced(tmp_path, monkeypatch):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    worker = make_worker()
    worker.store = store
    worker.client.get_account = AsyncMock(return_value={"address": ADDRESS})
    worker.client.get_trx_transactions = AsyncMock(return_value=[])
    worker.client.get_trc20_transfers = AsyncMock(return_value=[])

    def fail(*args):
        raise OSError("disk full")

    monkeypatch.setattr(store, "_append_account_rows", fail)
    with pytest.raises(OSError, match="disk full"):
        asyncio.run(worker.run_batch())
    assert CSVStore(str(tmp_path)).crawl_state[ADDRESS]["synced"] is False


def test_run_batch_cancellation_leaves_frontier_retryable(tmp_path):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}, {"wallet_address": OTHER}])
    worker = make_worker(max_concurrency=1)
    worker.store = store

    async def check():
        started = asyncio.Event()
        active = 0

        async def endpoint(*args):
            nonlocal active
            active += 1
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                active -= 1

        worker.client.get_account = endpoint
        worker.client.get_trx_transactions = endpoint
        worker.client.get_trc20_transfers = endpoint
        task = asyncio.create_task(worker.run_batch())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert active == 0
        assert not worker.semaphore.locked()

    asyncio.run(check())
    assert all(not row["synced"] for row in CSVStore(str(tmp_path)).crawl_state.values())


def test_run_batch_rejects_mismatched_account_identity(tmp_path):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    worker = make_worker()
    worker.store = store
    worker.client.get_account = AsyncMock(return_value={"address": OTHER})
    worker.client.get_trx_transactions = AsyncMock(return_value=[])
    worker.client.get_trc20_transfers = AsyncMock(return_value=[])
    assert asyncio.run(worker.run_batch())["failed"] == 1
    assert OTHER not in store.accounts
    assert store.crawl_state[ADDRESS]["synced"] is False
