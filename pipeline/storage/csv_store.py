"""Append-only local CSV storage."""

from __future__ import annotations

import csv
import io
import os
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence


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
        needs_record_separator = False
        if not needs_header:
            with csv_path.open("rb") as existing:
                existing.seek(-1, os.SEEK_END)
                needs_record_separator = existing.read(1) not in (b"\n", b"\r")
        with csv_path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            if needs_header:
                writer.writerow(list(header))
            elif needs_record_separator:
                handle.write("\r\n")
            writer.writerows(self._row_to_list(row, header) for row in pending)
            handle.flush()
            os.fsync(handle.fileno())

        self._appended_since_compact = (
            getattr(self, "_appended_since_compact", 0) + len(pending)
        )

    def _write_snapshot(
        self,
        path: str | Path,
        header: Sequence[str],
        rows: Iterable[Mapping[str, Any]],
    ) -> None:
        """Atomically replace a CSV with a complete, fsynced snapshot."""

        csv_path = Path(path)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(f"{csv_path}.tmp")
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(list(header))
            writer.writerows(self._row_to_list(row, header) for row in rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, csv_path)

    def _iter_rows(self, path: str | Path) -> Iterator[dict[str, str]]:
        """Stream complete CSV records and remove only a torn final record."""

        csv_path = Path(path)
        if not csv_path.is_file() or csv_path.stat().st_size == 0:
            return

        file_size = csv_path.stat().st_size
        last_good_offset = 0
        record_start = 0
        header: list[str] | None = None
        buffer = bytearray()
        torn_tail = False

        with csv_path.open("rb") as handle:
            while True:
                physical_line = handle.readline()
                if not physical_line:
                    break
                buffer.extend(physical_line)
                at_eof = handle.tell() == file_size

                try:
                    decoded = buffer.decode("utf-8-sig" if record_start == 0 else "utf-8")
                    records = list(csv.reader(io.StringIO(decoded, newline=""), strict=True))
                except UnicodeDecodeError:
                    if at_eof:
                        torn_tail = True
                        break
                    raise
                except csv.Error as exc:
                    if "unexpected end of data" in str(exc).lower():
                        if at_eof:
                            torn_tail = True
                            break
                        continue
                    raise

                if len(records) != 1:
                    raise csv.Error("one logical CSV record expected")
                fields = records[0]
                record_end = handle.tell()

                if header is None:
                    if not fields or any(not column for column in fields):
                        raise csv.Error("CSV header contains an empty column")
                    header = fields
                elif len(fields) != len(header):
                    if (
                        len(fields) < len(header)
                        and at_eof
                        and not physical_line.endswith((b"\n", b"\r"))
                    ):
                        torn_tail = True
                        break
                    raise csv.Error(
                        f"CSV record has {len(fields)} fields; expected {len(header)}"
                    )
                else:
                    self._current_row_offset = record_start
                    yield dict(zip(header, fields))

                last_good_offset = record_end
                record_start = record_end
                buffer.clear()

        if torn_tail:
            with csv_path.open("r+b") as handle:
                handle.truncate(last_good_offset)
                handle.flush()
                os.fsync(handle.fileno())

    def _load_seeds(self) -> None:
        """Load last-row-wins seed state, retaining licit labels by default."""

        latest: dict[str, dict[str, str]] = {}
        for row in self._iter_rows(self.seed_wallets_path):
            wallet = row.get("wallet_address", "")
            if wallet:
                latest[wallet] = row
        if getattr(self, "retain_all_seeds", False):
            self.seeds = latest
        else:
            self.seeds = {
                wallet: row for wallet, row in latest.items() if row.get("label") == "0"
            }

    def _load_accounts(self) -> None:
        """Load account rows keyed by address with latest append winning."""

        self.accounts = {}
        for row in self._iter_rows(self.accounts_path):
            address = row.get("address", "")
            if address:
                self.accounts[address] = row

    def _load_transactions(self) -> None:
        """Build compact hash, address, and byte-offset transaction indexes."""

        self.tx_hashes: set[str] = set()
        self.wallet_tx_hashes: dict[str, set[str]] = {}
        self.tx_offsets: dict[str, int] = {}
        for row in self._iter_rows(self.transactions_path):
            tx_hash = row.get("tx_hash", "")
            if not tx_hash or tx_hash in self.tx_hashes:
                continue

            self.tx_hashes.add(tx_hash)
            self.tx_offsets[tx_hash] = self._current_row_offset
            for address in {row.get("from_address", ""), row.get("to_address", "")}:
                if address:
                    self.wallet_tx_hashes.setdefault(address, set()).add(tx_hash)
