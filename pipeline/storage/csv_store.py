"""Append-only local CSV storage."""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


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

    def _row_to_list(
        self, row: Mapping[str, Any], header: Sequence[str]
    ) -> list[Any]:
        """Serialize a mapping in canonical header order, filling missing cells."""

        return [row.get(column, "") for column in header]

    def _append_rows(
        self,
        path: str | Path,
        header: Sequence[str],
        rows: Iterable[Mapping[str, Any]],
    ) -> None:
        """Durably append rows, adding a header only to an empty file."""

        pending = list(rows)
        if not pending:
            return

        csv_path = Path(path)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        needs_header = not csv_path.exists() or csv_path.stat().st_size == 0
        with csv_path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            if needs_header:
                writer.writerow(list(header))
            writer.writerows(self._row_to_list(row, header) for row in pending)
            handle.flush()
            os.fsync(handle.fileno())

        self._appended_since_compact = (
            getattr(self, "_appended_since_compact", 0) + len(pending)
        )
