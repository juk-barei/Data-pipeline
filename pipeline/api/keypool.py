"""Rotate API keys with independent cooldowns and request pacing."""

from __future__ import annotations

import asyncio
import logging
import math
import time

from pipeline.api.cooldown import CooldownTracker

logger = logging.getLogger(__name__)


class MultiKeyPool:
    """Distribute acquisitions across independently paced API keys."""

    def __init__(
        self,
        keys: list[str],
        cooldown_sec: int,
        rate_limit_per_key: float = 14.5,
    ) -> None:
        if not math.isfinite(rate_limit_per_key) or rate_limit_per_key <= 0:
            raise ValueError("rate_limit_per_key must be finite and positive")
        if any(not isinstance(key, str) or not key.strip() for key in keys):
            raise ValueError("keys must contain only nonblank strings")
        self.keys = list(dict.fromkeys(keys))
        self.rate_limit_per_key = rate_limit_per_key
        self.cooldown = CooldownTracker(cooldown_sec)
        self._interval = 1.0 / rate_limit_per_key
        self._next_allowed = dict.fromkeys(self.keys, 0.0)
        self._index = 0
        self._lock = asyncio.Lock()
        if not self.keys:
            logger.warning("No API keys configured; using public mode")


    async def get_key(self) -> str:
        """Acquire the next eligible key, yielding while every key is busy."""
        if not self.keys:
            return ""
        while True:
            async with self._lock:
                now = time.monotonic()
                earliest = math.inf
                for offset in range(len(self.keys)):
                    index = (self._index + offset) % len(self.keys)
                    key = self.keys[index]
                    ready_at = max(
                        self._next_allowed[key],
                        self.cooldown.cooldowns.get(key, 0.0),
                    )
                    if ready_at <= now:
                        self._next_allowed[key] = now + self._interval
                        self._index = (index + 1) % len(self.keys)
                        return key
                    earliest = min(earliest, ready_at)
                delay = earliest - now
            # Never hold the rotation lock while waiting: other keys and callers
            # must remain independent, and cancellation must release all state.
            await asyncio.sleep(delay)
