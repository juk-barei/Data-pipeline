"""Report progress from the store's in-memory metrics."""

from __future__ import annotations

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
