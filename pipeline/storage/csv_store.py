"""Append-only local CSV storage."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Sequence


class CSVStore:
    """Store pipeline state in five append-only CSV files."""

    def _init_file(self, path: str | Path, header: Sequence[str]) -> None:
        """Create a missing or empty CSV with its header without truncating data."""

        csv_path = Path(path)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        if csv_path.exists() and csv_path.stat().st_size > 0:
            return
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            csv.writer(handle).writerow(list(header))
