"""Report progress from the store's in-memory metrics."""

from __future__ import annotations

import time

from pipeline.api.keypool import MultiKeyPool
from pipeline.storage.csv_store import CSVStore


def is_complete(metrics: dict) -> bool:
    """Finish only after a nonempty frontier has no crawl or expand work."""
    return (
        metrics["total_crawl_states"] > 0
        and metrics["pending_crawls"] == 0
        and metrics["pending_expands"] == 0
    )


class HealthReporter:
    """Read store and key-pool progress on their owning event-loop thread."""

    def __init__(self, store: CSVStore, key_pool: MultiKeyPool | None = None) -> None:
        """Retain shared dependencies without I/O or acquiring worker locks."""
        self.store = store
        self.key_pool = key_pool
        self._started_at = time.monotonic()
        self._initial_acquisitions = key_pool.acquisitions if key_pool else 0

    def render(self) -> str:
        """Render a nonblocking snapshot; RPS counts acquisitions, not successes.

        The average covers this reporter's lifetime. Capacity excludes cooling
        keys; pacing alone does not classify an otherwise available key as cooling.
        Call on the same event-loop thread as the workers and shared key pool.
        """
        metrics = self.store.get_metrics()
        now = time.monotonic()
        active = cooling = 0
        rps = capacity = 0.0
        if self.key_pool is not None:
            pool = self.key_pool
            cooling = sum(pool.cooldown.cooldowns.get(key, 0.0) > now for key in pool.keys)
            active = len(pool.keys) - cooling
            elapsed = now - self._started_at
            if elapsed > 0:
                rps = (pool.acquisitions - self._initial_acquisitions) / elapsed
            capacity = active * pool.rate_limit_per_key
        lines = ["Pipeline health"]
        lines.extend(f"Hop {hop}: {metrics['per_hop'].get(hop, 0)} wallets" for hop in range(6))
        lines.extend([
            f"Pending crawls: {metrics['pending_crawls']}",
            f"Pending expands: {metrics['pending_expands']}",
            f"Active keys: {active} | Cooling keys: {cooling}",
            f"Acquisition RPS (reporter average): {rps:.2f} | Capacity RPS: {capacity:.2f}",
            f"Complete: {'yes' if is_complete(metrics) else 'no'}",
        ])
        return "\n".join(lines)
