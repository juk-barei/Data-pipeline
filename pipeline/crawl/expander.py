"""Expand synced wallet histories into a bounded graph frontier."""

from __future__ import annotations

from pipeline.config import Settings
from pipeline.storage.csv_store import CSVStore


class ExpanderWorker:
    """Derive graph edges without owning the store lifecycle."""

    def __init__(self, store: CSVStore, settings: Settings) -> None:
        """Retain dependencies without reading or writing storage."""
        self.store = store
        self.settings = settings
