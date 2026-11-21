"""Track per-key cooldowns using monotonic time."""

from __future__ import annotations

import math
import time


class CooldownTracker:
    """Keep independent cooldown deadlines for API keys."""

    def __init__(self, cooldown_sec: int) -> None:
        if not math.isfinite(cooldown_sec) or cooldown_sec < 0:
            raise ValueError("cooldown_sec must be finite and nonnegative")
        self.cooldown_sec = cooldown_sec
        self.cooldowns: dict[str, float] = {}

    def is_available(self, key: str) -> bool:
        """Return whether this key has no active cooldown."""
        deadline = self.cooldowns.get(key)
        return deadline is None or time.monotonic() >= deadline
