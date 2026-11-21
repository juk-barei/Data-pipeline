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
