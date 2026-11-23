import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from pipeline.api.keypool import MultiKeyPool
from pipeline.config import Settings
from pipeline.crawl.crawler import CrawlerWorker
from pipeline.crawl.expander import ExpanderWorker
from pipeline.orchestrate.health import HealthReporter
from pipeline.orchestrate.runner import Runner
from pipeline.storage.csv_store import CSVStore


def make_runner(store=None, settings=None):
    client = SimpleNamespace(pool=MultiKeyPool([], 60))
    return Runner(store if store is not None else Mock(), client,
                  settings if settings is not None else Settings(sync_every=0))


def test_constructor_assembles_workers_without_io():
    runner = make_runner()
    assert isinstance(runner.crawler, CrawlerWorker)
    assert isinstance(runner.expander, ExpanderWorker)
    assert isinstance(runner.health, HealthReporter)
    assert runner.crawler.store is runner.expander.store is runner.health.store is runner.store
    assert runner.crawler.client is runner.client
    assert runner.crawler.settings is runner.expander.settings is runner.settings
    assert runner.health.key_pool is runner.client.pool
    assert runner.store.mock_calls == []
    assert not runner._cycle_lock.locked()
    assert not runner._run_lock.locked()
    assert runner._sync_state == {}


def test_runner_instances_keep_locks_and_sync_state_independent():
    first, second = make_runner(), make_runner()
    first._sync_state["cycles_since_sync"] = 1
    assert second._sync_state == {}
    assert first._cycle_lock is not second._cycle_lock
    assert first._run_lock is not second._run_lock


def test_idle_run_once_returns_all_worker_counts_as_zero(tmp_path):
    runner = make_runner(CSVStore(tmp_path))
    summary = asyncio.run(runner.run_once())
    assert summary == {
        "crawl": dict(claimed=0, synced=0, failed=0, accounts=0, transactions=0, supernodes=0),
        "expand": dict(claimed=0, expanded=0, pruned=0, accounts=0, frontier=0, hops=0),
    }


def test_run_once_preserves_worker_summaries_and_serializes_cycles():
    async def check():
        events = []
        runner = make_runner()
        async def crawl():
            events.append("crawl")
            await asyncio.sleep(0)
            return {"claimed": 2, "accounts": 1}
        async def expand():
            events.append("expand")
            await asyncio.sleep(0)
            return {"claimed": 3, "accounts": 4}
        runner.crawler.run_batch = crawl
        runner.expander.run_batch = expand
        results = await asyncio.gather(runner.run_once(), runner.run_once())
        assert events == ["crawl", "expand", "crawl", "expand"]
        assert results == [{"crawl": {"claimed": 2, "accounts": 1},
                            "expand": {"claimed": 3, "accounts": 4}}] * 2
    asyncio.run(check())


@pytest.mark.parametrize("failing_worker", ["crawler", "expander"])
def test_run_once_propagates_failures_and_releases_lock(failing_worker):
    async def check():
        runner = make_runner()
        runner.crawler.run_batch = AsyncMock(return_value={"claimed": 0})
        runner.expander.run_batch = AsyncMock(return_value={"claimed": 0})
        getattr(runner, failing_worker).run_batch.side_effect = OSError("write failed")
        with pytest.raises(OSError, match="write failed"):
            await runner.run_once()
        assert not runner._cycle_lock.locked()
        if failing_worker == "crawler":
            runner.expander.run_batch.assert_not_awaited()
    asyncio.run(check())


def test_cancelled_run_once_drains_crawler_tasks_and_leaves_csvs_retryable(tmp_path):
    async def check():
        store = CSVStore(tmp_path)
        address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
        store.insert_seeds([{"wallet_address": address}])
        runner = make_runner(store)
        started = asyncio.Event()
        cleaned = asyncio.Event()
        async def fetch(address):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()
        runner.crawler._fetch_wallet = fetch
        runner.expander.run_batch = AsyncMock()
        task = asyncio.create_task(runner.run_once())
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cleaned.is_set()
        assert not runner._cycle_lock.locked()
        runner.expander.run_batch.assert_not_awaited()
        reloaded = CSVStore(tmp_path)
        assert reloaded.crawl_state[address]["synced"] is False
        assert reloaded.crawl_state[address]["expanded"] is False
    asyncio.run(check())


def test_run_once_crawls_and_expands_one_seed_without_network(tmp_path):
    async def check():
        store = CSVStore(tmp_path)
        address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
        store.insert_seeds([{"wallet_address": address}])
        runner = make_runner(store)
        runner.client.get_account = AsyncMock(return_value=None)
        runner.client.get_trx_transactions = AsyncMock(return_value=[])
        runner.client.get_trc20_transfers = AsyncMock(return_value=[])
        result = await runner.run_once()
        assert result["crawl"]["synced"] == 1
        assert result["expand"]["expanded"] == 1
        assert CSVStore(tmp_path).get_metrics()["is_complete"] is True
    asyncio.run(check())
