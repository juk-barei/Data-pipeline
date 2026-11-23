import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from pipeline.api.keypool import MultiKeyPool
from pipeline.config import Settings
from pipeline.crawl.crawler import CrawlerWorker


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
