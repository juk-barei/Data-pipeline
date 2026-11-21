import asyncio
import logging

import pytest

from pipeline.api.keypool import MultiKeyPool


def test_pool_initializes_independent_per_key_pacers():
    keys = ["one", "two", "one"]
    pool = MultiKeyPool(keys, 60)
    keys.append("three")
    assert pool.keys == ["one", "two"]
    assert pool.rate_limit_per_key == 14.5
    assert pool._interval == pytest.approx(1 / 14.5)
    assert pool._next_allowed == {"one": 0.0, "two": 0.0}
    assert pool._index == 0
    assert isinstance(pool._lock, asyncio.Lock)
    assert not pool._lock.locked()
    assert pool.cooldown.cooldown_sec == 60
    assert pool.cooldown.cooldowns == {}


def test_empty_pool_warns_without_logging_secrets(caplog):
    with caplog.at_level(logging.WARNING, logger="pipeline.api.keypool"):
        pool = MultiKeyPool([], 60)
    assert pool.keys == []
    assert pool._next_allowed == {}
    assert "public mode" in caplog.text


@pytest.mark.parametrize("rate", [0, -1, float("inf"), float("nan")])
def test_pool_rejects_invalid_rates(rate):
    with pytest.raises(ValueError, match="finite and positive"):
        MultiKeyPool(["one"], 60, rate)


@pytest.mark.parametrize("keys", [[""], [" "], [None]])
def test_pool_rejects_blank_or_nonstring_keys(keys):
    with pytest.raises(ValueError, match="nonblank strings"):
        MultiKeyPool(keys, 60)


def test_pool_instances_have_independent_state():
    first = MultiKeyPool(["one"], 60)
    second = MultiKeyPool(["one"], 60)
    first._next_allowed["one"] = 100
    first.cooldown.cooldowns["one"] = 100
    assert second._next_allowed["one"] == 0
    assert second.cooldown.cooldowns == {}
    assert first._lock is not second._lock
