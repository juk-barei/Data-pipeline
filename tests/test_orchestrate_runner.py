from types import SimpleNamespace
from unittest.mock import Mock

from pipeline.api.keypool import MultiKeyPool
from pipeline.config import Settings
from pipeline.crawl.crawler import CrawlerWorker
from pipeline.crawl.expander import ExpanderWorker
from pipeline.orchestrate.health import HealthReporter
from pipeline.orchestrate.runner import Runner


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
