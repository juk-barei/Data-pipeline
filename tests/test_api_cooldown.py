import pytest

from pipeline.api.cooldown import CooldownTracker


def test_tracker_starts_empty_and_keeps_default():
    tracker = CooldownTracker(60)
    assert tracker.cooldown_sec == 60
    assert tracker.cooldowns == {}


def test_tracker_instances_do_not_share_state():
    first = CooldownTracker(60)
    second = CooldownTracker(0)
    first.cooldowns["one"] = 100.0
    assert second.cooldowns == {}
    assert second.cooldown_sec == 0


@pytest.mark.parametrize("seconds", [-1, float("nan"), float("inf")])
def test_tracker_rejects_invalid_duration(seconds):
    with pytest.raises(ValueError, match="finite and nonnegative"):
        CooldownTracker(seconds)


def test_unknown_key_is_available_even_at_zero(monkeypatch):
    monkeypatch.setattr("pipeline.api.cooldown.time.monotonic", lambda: 0.0)
    assert CooldownTracker(60).is_available("unknown") is True


@pytest.mark.parametrize("now, expected", [(99.0, False), (100.0, True), (101.0, True)])
def test_cooldown_expiry_uses_monotonic_boundary(monkeypatch, now, expected):
    tracker = CooldownTracker(60)
    tracker.cooldowns["one"] = 100.0
    monkeypatch.setattr("pipeline.api.cooldown.time.monotonic", lambda: now)
    monkeypatch.setattr("time.time", lambda: -1_000_000)
    assert tracker.is_available("one") is expected
    assert tracker.is_available("two") is True


def test_penalty_is_immediate_isolated_and_expires(monkeypatch):
    now = [10.0]
    monkeypatch.setattr("pipeline.api.cooldown.time.monotonic", lambda: now[0])
    tracker = CooldownTracker(60)
    assert tracker.penalize("one", 5) is None
    assert tracker.cooldowns == {"one": 15.0}
    assert not tracker.is_available("one")
    assert tracker.is_available("two")
    now[0] = 15.0
    assert tracker.is_available("one")


def test_penalty_replaces_deadline_and_zero_expires_immediately(monkeypatch):
    monkeypatch.setattr("pipeline.api.cooldown.time.monotonic", lambda: 10.0)
    tracker = CooldownTracker(60)
    tracker.penalize("one", 30)
    tracker.penalize("one", 2)
    assert tracker.cooldowns["one"] == 12.0
    tracker.penalize("one", 0)
    assert tracker.is_available("one")


@pytest.mark.parametrize("seconds", [-1, float("inf"), float("nan")])
def test_invalid_penalty_does_not_change_state(seconds):
    tracker = CooldownTracker(60)
    with pytest.raises(ValueError, match="finite and nonnegative"):
        tracker.penalize("one", seconds)
    assert tracker.cooldowns == {}
