import asyncio
import sys
import threading
from types import ModuleType, SimpleNamespace
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


@pytest.fixture
def sync_module(monkeypatch):
    """Isolate the Phase 7 boundary; combined tests use the real module later."""
    module = ModuleType("pipeline.sync.drive")
    module.maybe_sync = Mock(return_value=False)
    monkeypatch.setitem(sys.modules, "pipeline.sync.drive", module)
    return module


def completed_metrics():
    return dict(total_crawl_states=1, pending_crawls=0, pending_expands=0)


def test_run_forever_skips_already_completed_frontier():
    runner = make_runner()
    runner.store.get_metrics.return_value = completed_metrics()
    runner.run_once = AsyncMock()
    asyncio.run(runner.run_forever())
    runner.run_once.assert_not_awaited()
    runner.store.compact_if_needed.assert_not_called()


def test_run_forever_yields_and_waits_for_both_pending_queues(monkeypatch):
    async def check():
        runner = make_runner()
        metrics = [dict(total_crawl_states=1, pending_crawls=1, pending_expands=0),
                   dict(total_crawl_states=1, pending_crawls=0, pending_expands=1),
                   completed_metrics()]
        index = [0]
        runner.store.get_metrics.side_effect = lambda: metrics[index[0]]
        async def once():
            index[0] += 1
            return {"crawl": {"claimed": 1}, "expand": {"claimed": 1}}
        runner.run_once = AsyncMock(side_effect=once)
        original_sleep = asyncio.sleep
        sleeps = []
        async def sleep(delay):
            sleeps.append(delay)
            await original_sleep(0)
        monkeypatch.setattr("pipeline.orchestrate.runner.asyncio.sleep", sleep)
        await runner.run_forever()
        assert runner.run_once.await_count == 2
        assert runner.store.compact_if_needed.call_count == 2
        assert sleeps == [0]
    asyncio.run(check())


def test_empty_frontier_waits_without_busy_spinning_and_remains_cancellable(tmp_path, monkeypatch):
    async def check():
        runner = make_runner(CSVStore(tmp_path))
        async def cancel_at_yield(delay):
            assert delay > 0
            raise asyncio.CancelledError
        monkeypatch.setattr("pipeline.orchestrate.runner.asyncio.sleep", cancel_at_yield)
        with pytest.raises(asyncio.CancelledError):
            await runner.run_forever()
        assert not runner._run_lock.locked()
        assert not runner._cycle_lock.locked()
        assert CSVStore(tmp_path).get_metrics()["is_complete"] is False
    asyncio.run(check())


def test_run_forever_syncs_off_loop_before_compaction_and_stops(tmp_path, sync_module):
    async def check():
        store = CSVStore(tmp_path)
        address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
        store.insert_seeds([{"wallet_address": address}])
        runner = make_runner(store, Settings(sync_every=1))
        runner.client.get_account = AsyncMock(return_value=None)
        runner.client.get_trx_transactions = AsyncMock(return_value=[])
        runner.client.get_trc20_transfers = AsyncMock(return_value=[])
        runner.client.aclose = AsyncMock()
        events = []
        loop_thread = threading.get_ident()
        def sync(actual_store, settings, state):
            assert threading.get_ident() != loop_thread
            assert actual_store is store and settings is runner.settings
            assert state is runner._sync_state
            assert runner._cycle_lock.locked()
            state["cycles_since_sync"] = 0
            events.append("sync")
            return True
        sync_module.maybe_sync.side_effect = sync
        store.compact_if_needed = Mock(side_effect=lambda: events.append("compact"))
        await runner.run_forever()
        assert events == ["sync", "compact"]
        sync_module.maybe_sync.assert_called_once()
        assert CSVStore(tmp_path).get_metrics()["is_complete"] is True
        runner.client.aclose.assert_not_awaited()
    asyncio.run(check())


def test_disabled_sync_does_not_import_drive_module(monkeypatch):
    monkeypatch.setitem(sys.modules, "pipeline.sync.drive", None)
    runner = make_runner()
    runner.store.get_metrics.return_value = completed_metrics()
    asyncio.run(runner.run_forever())


def test_sync_failure_propagates_before_compaction(sync_module):
    runner = make_runner(settings=Settings(sync_every=1))
    runner.store.get_metrics.return_value = dict(total_crawl_states=1, pending_crawls=1,
                                               pending_expands=0)
    runner.run_once = AsyncMock(return_value={"crawl": {"claimed": 1}, "expand": {"claimed": 0}})
    sync_module.maybe_sync.side_effect = OSError("sync failed")
    with pytest.raises(OSError, match="sync failed"):
        asyncio.run(runner.run_forever())
    runner.store.compact_if_needed.assert_not_called()
    assert not runner._cycle_lock.locked()
    assert not runner._run_lock.locked()


def test_another_cycle_cannot_mutate_store_during_sync(sync_module):
    async def check():
        runner = make_runner(settings=Settings(sync_every=1))
        runner.store.get_metrics.return_value = dict(total_crawl_states=1, pending_crawls=1,
                                                   pending_expands=0)
        runner.crawler.run_batch = AsyncMock(return_value={"claimed": 0})
        runner.expander.run_batch = AsyncMock(return_value={"claimed": 0})
        started, release = threading.Event(), threading.Event()
        def sync(*args):
            started.set()
            assert release.wait(5)
            runner.store.get_metrics.return_value = completed_metrics()
            return True
        sync_module.maybe_sync.side_effect = sync
        loop = asyncio.create_task(runner.run_forever())
        try:
            assert await asyncio.to_thread(started.wait, 2)
            cycle = asyncio.create_task(runner.run_once())
            await asyncio.sleep(0)
            assert not cycle.done()
            assert runner.crawler.run_batch.await_count == 1
        finally:
            release.set()
        await loop
        await cycle
        assert runner.crawler.run_batch.await_count == 2
    asyncio.run(check())


@pytest.mark.parametrize("sync_fails", [False, True])
def test_cancellation_drains_sync_thread_before_unlocking_store(sync_module, sync_fails):
    async def check():
        runner = make_runner(settings=Settings(sync_every=1))
        runner.store.get_metrics.return_value = dict(total_crawl_states=1, pending_crawls=1,
                                                   pending_expands=0)
        runner.run_once = AsyncMock(return_value={"crawl": {"claimed": 1}, "expand": {"claimed": 0}})
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        def sync(*args):
            started.set()
            assert release.wait(5), "test did not release sync thread"
            finished.set()
            if sync_fails:
                raise OSError("shutdown failure")
            return True
        sync_module.maybe_sync.side_effect = sync
        task = asyncio.create_task(runner.run_forever())
        try:
            assert await asyncio.to_thread(started.wait, 2)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()  # Repeated interrupts must not release storage early.
            await asyncio.sleep(0)
            assert not task.done()
            assert runner._cycle_lock.locked()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
        assert not runner._cycle_lock.locked()
        assert not runner._run_lock.locked()
        runner.store.compact_if_needed.assert_not_called()
    asyncio.run(check())


def test_keyboard_interrupt_propagates_with_csv_state_retryable(tmp_path):
    async def check():
        store = CSVStore(tmp_path)
        address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
        store.insert_seeds([{"wallet_address": address}])
        runner = make_runner(store)
        runner.crawler.run_batch = AsyncMock(side_effect=KeyboardInterrupt)
        runner.expander.run_batch = AsyncMock()
        with pytest.raises(KeyboardInterrupt):
            await runner.run_forever()
        runner.expander.run_batch.assert_not_awaited()
        assert not runner._cycle_lock.locked()
        assert not runner._run_lock.locked()
        assert CSVStore(tmp_path).crawl_state[address]["synced"] is False
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
