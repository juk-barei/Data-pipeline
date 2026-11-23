import pytest
from unittest.mock import Mock

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
