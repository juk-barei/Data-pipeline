import asyncio
from unittest.mock import Mock

import pytest

from pipeline.api.keypool import MultiKeyPool
from pipeline.orchestrate.health import HealthReporter, is_complete
from pipeline.storage.csv_store import CSVStore


@pytest.mark.parametrize(
    "frontier,crawls,expands,expected",
    [(0, 0, 0, False), (1, 0, 0, True), (3, 1, 0, False),
     (3, 0, 1, False), (3, 1, 1, False)],
)
def test_completion_requires_nonempty_drained_frontier(frontier, crawls, expands, expected):
    metrics = dict(total_crawl_states=frontier, pending_crawls=crawls,
                   pending_expands=expands, is_complete=not expected)
    assert is_complete(metrics) is expected


def test_completion_matches_store_metrics_through_wallet_lifecycle(tmp_path):
    store = CSVStore(tmp_path)
    address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    for update in (
        lambda: None,
        lambda: store.insert_seeds([{"wallet_address": address}]),
        lambda: store.upsert_crawled_data([], [], [address], []),
        lambda: store.add_expansion_results([], [], [], [address]),
    ):
        update()
        metrics = store.get_metrics()
        assert is_complete(metrics) is metrics["is_complete"]


def test_reporter_constructor_retains_optional_dependencies_without_io():
    store = Mock()
    pool = MultiKeyPool(["secret-key"], 60)
    reporter = HealthReporter(store, pool)
    assert reporter.store is store
    assert reporter.key_pool is pool
    assert store.mock_calls == []
    assert not pool._lock.locked()
    assert pool.cooldown.cooldowns == {}
    assert HealthReporter(store).key_pool is None


def test_render_shows_hops_queues_completion_and_no_pool(tmp_path):
    reporter = HealthReporter(CSVStore(tmp_path))
    output = reporter.render()
    assert output.isascii()
    for hop in range(6):
        assert f"Hop {hop}: 0 wallets" in output
    assert "Pending crawls: 0" in output
    assert "Pending expands: 0" in output
    assert "Active keys: 0 | Cooling keys: 0" in output
    assert "Acquisition RPS (reporter average): 0.00" in output
    assert "Complete: no" in output


def test_render_counts_cooldowns_and_observed_acquisitions_without_secrets(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("pipeline.orchestrate.health.time.monotonic", lambda: now[0])
    pool = MultiKeyPool(["secret-one", "secret-two"], 60, 10)
    asyncio.run(pool.get_key())  # Acquisitions before reporter startup are excluded.
    metrics = dict(total_crawl_states=6, pending_crawls=0, pending_expands=0,
                   per_hop={hop: hop + 1 for hop in range(6)})
    store = Mock()
    store.get_metrics.return_value = metrics
    reporter = HealthReporter(store, pool)
    assert "Acquisition RPS (reporter average): 0.00" in reporter.render()
    asyncio.run(pool.get_key())
    pool.penalize("secret-two", 5)
    now[0] += 2
    with_lock = asyncio.run(_render_with_lock(reporter, pool))
    assert "Active keys: 1 | Cooling keys: 1" in with_lock
    assert "Acquisition RPS (reporter average): 0.50 | Capacity RPS: 10.00" in with_lock
    assert "secret" not in with_lock
    for hop in range(6):
        assert f"Hop {hop}: {hop + 1} wallets" in with_lock
    assert "Complete: yes" in with_lock
    now[0] = 105.0
    assert "Active keys: 2 | Cooling keys: 0" in reporter.render()


async def _render_with_lock(reporter, pool):
    async with pool._lock:
        return reporter.render()


def test_render_reports_public_mode_acquisitions(monkeypatch, tmp_path):
    now = [100.0]
    monkeypatch.setattr("pipeline.orchestrate.health.time.monotonic", lambda: now[0])
    pool = MultiKeyPool([], 60)
    reporter = HealthReporter(CSVStore(tmp_path), pool)
    asyncio.run(pool.get_key())
    now[0] += 1
    assert "Acquisition RPS (reporter average): 1.00 | Capacity RPS: 0.00" in reporter.render()
