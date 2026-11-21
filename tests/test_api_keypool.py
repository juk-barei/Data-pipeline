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


class Clock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []
        self.real_sleep = asyncio.sleep

    async def sleep(self, delay):
        assert delay > 0, "busy spinning instead of waiting"
        self.sleeps.append(delay)
        self.now += delay
        await self.real_sleep(0)


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr("pipeline.api.keypool.time.monotonic", lambda: clock.now)
    monkeypatch.setattr("pipeline.api.keypool.asyncio.sleep", clock.sleep)
    return clock


def test_empty_pool_returns_public_key():
    assert asyncio.run(MultiKeyPool([], 60).get_key()) == ""


def test_round_robin_and_per_key_pacing(clock):
    pool = MultiKeyPool(["one", "two"], 60, 2)

    async def acquire():
        return [await pool.get_key() for _ in range(6)]

    assert asyncio.run(acquire()) == ["one", "two"] * 3
    assert clock.sleeps == [0.5, 0.5]


def test_all_cooling_waits_for_earliest_expiry(clock):
    pool = MultiKeyPool(["one", "two"], 60)
    pool.cooldown.penalize("one", 20)
    pool.cooldown.penalize("two", 5)
    assert asyncio.run(pool.get_key()) == "two"
    assert clock.sleeps == [5.0]
    assert not pool._lock.locked()


def test_waits_for_both_rate_and_cooldown_constraints(clock):
    pool = MultiKeyPool(["one"], 60, 0.5)

    async def acquire():
        assert await pool.get_key() == "one"
        pool.cooldown.penalize("one", 1)
        return await pool.get_key()

    assert asyncio.run(acquire()) == "one"
    assert clock.sleeps == [2.0]


def test_idle_time_does_not_accumulate_burst_capacity(clock):
    pool = MultiKeyPool(["one"], 60, 2)

    async def acquire():
        await pool.get_key()
        clock.now += 100
        await pool.get_key()
        await pool.get_key()

    asyncio.run(acquire())
    assert clock.sleeps == [0.5]


def test_concurrent_callers_respect_each_keys_capacity(clock):
    pool = MultiKeyPool(["one", "two", "three"], 60, 4)

    async def acquire():
        async def record():
            key = await pool.get_key()
            return key, clock.now
        return await asyncio.gather(*(record() for _ in range(30)))

    records = asyncio.run(acquire())
    for key in pool.keys:
        times = sorted(now for acquired, now in records if acquired == key)
        assert len(times) == 10
        assert all(later - earlier >= 0.25 for earlier, later in zip(times, times[1:]))
    assert clock.now - 100 == pytest.approx(2.25)


def test_cancellation_while_waiting_leaves_pool_usable(monkeypatch):
    async def scenario():
        pool = MultiKeyPool(["one"], 60)
        pool.cooldown.penalize("one", 60)
        sleeping = asyncio.Event()
        never = asyncio.Event()

        async def blocked_sleep(delay):
            assert delay > 0
            assert not pool._lock.locked()
            sleeping.set()
            await never.wait()

        monkeypatch.setattr("pipeline.api.keypool.asyncio.sleep", blocked_sleep)
        task = asyncio.create_task(pool.get_key())
        await sleeping.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not pool._lock.locked()
        pool.cooldown.penalize("one", 0)
        assert await pool.get_key() == "one"

    asyncio.run(scenario())


def test_pool_penalty_uses_default_and_masks_key(clock, caplog):
    secret = "private-api-key-123"
    pool = MultiKeyPool([secret, "healthy"], 60)
    with caplog.at_level(logging.WARNING, logger="pipeline.api.keypool"):
        assert pool.penalize(secret) is None
    assert pool.cooldown.cooldowns[secret] == 160.0
    assert secret not in caplog.text
    assert "privat..." in caplog.text
    assert asyncio.run(pool.get_key()) == "healthy"
    assert clock.sleeps == []


def test_explicit_pool_penalty_expires_and_returns_to_rotation(clock):
    pool = MultiKeyPool(["one", "two"], 60)
    pool.penalize("one", 5)
    assert asyncio.run(pool.get_key()) == "two"
    clock.now += 5
    assert asyncio.run(pool.get_key()) == "one"
    assert clock.sleeps == []


def test_zero_pool_penalty_is_not_replaced_by_default(clock):
    pool = MultiKeyPool(["one"], 60)
    pool.penalize("one", 0)
    assert asyncio.run(pool.get_key()) == "one"
    assert clock.sleeps == []


def test_unknown_pool_key_does_not_interrupt_healthy_keys(clock):
    pool = MultiKeyPool(["one"], 60)
    pool.penalize("unknown", 60)
    assert pool.keys == ["one"]
    assert asyncio.run(pool.get_key()) == "one"


def test_negative_pool_penalty_preserves_existing_deadline(clock):
    pool = MultiKeyPool(["one"], 60)
    pool.penalize("one", 5)
    with pytest.raises(ValueError):
        pool.penalize("one", -1)
    assert pool.cooldown.cooldowns["one"] == 105.0
