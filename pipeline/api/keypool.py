"""Rotate API keys with independent cooldowns and request pacing."""

from __future__ import annotations

import asyncio
import logging
import math

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
