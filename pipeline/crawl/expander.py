"""Expand synced wallet histories into a bounded graph frontier."""

from __future__ import annotations

from pipeline.config import Settings
from pipeline.storage.csv_store import CSVStore
from pipeline.utils.address import normalize_address


class ExpanderWorker:
    """Derive graph edges without owning the store lifecycle."""

    def __init__(self, store: CSVStore, settings: Settings) -> None:
        """Retain dependencies without reading or writing storage."""
        self.store = store
        self.settings = settings

    def _classify_direction(self, tx: dict, address: str) -> str | None:
        """Classify a canonical wallet's transfer direction; ignore self-transfers."""
        if not isinstance(tx, dict):
            return None
        address = normalize_address(address)
        sender = normalize_address(tx.get("from_address"))
        receiver = normalize_address(tx.get("to_address"))
        if not address or not sender or not receiver or sender == receiver:
            return None
        if address == sender:
            return "FORWARD"
        if address == receiver:
            return "BACKWARD"
        return None
