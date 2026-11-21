"""Append-only local CSV storage."""

from __future__ import annotations

import csv
import io
import logging
import os
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

from pipeline.storage.schema import build_headers


logger = logging.getLogger(__name__)


class CSVStore:
    """Store pipeline state in five append-only CSV files."""

    def __init__(self, csv_dir: str, *, retain_all_seeds: bool = False) -> None:
        """Initialize canonical files and load their current append-log state."""

        self.csv_dir = Path(csv_dir).expanduser().resolve()
        self.headers = build_headers()
        self.paths = {name: self.csv_dir / name for name in self.headers}
        self.seed_wallets_path = self.paths["seed_wallets.csv"]
        self.accounts_path = self.paths["accounts.csv"]
        self.transactions_path = self.paths["transactions.csv"]
        self.wallet_hops_path = self.paths["wallet_hops.csv"]
        self.crawl_state_path = self.paths["pipeline_crawl_state.csv"]
        self.retain_all_seeds = retain_all_seeds
        self._appended_since_compact = 0

        self.seeds: dict[str, dict[str, str]] = {}
        self.accounts: dict[str, dict[str, str]] = {}
        self.tx_hashes: set[str] = set()
        self.wallet_tx_hashes: dict[str, set[str]] = {}
        self.tx_offsets: dict[str, int] = {}
        self.hops: list[dict[str, str]] = []
        self.hop_keys: set[tuple[str, str, int]] = set()
        self.max_hop_id = 0
        self.crawl_state: dict[str, dict[str, Any]] = {}

        for name, header in self.headers.items():
            self._init_file(self.paths[name], header)
        self.load()

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

    def _load_hops(self) -> None:
        """Load unique hop edges and the greatest observed hop identifier."""

        self.hops: list[dict[str, str]] = []
        self.hop_keys: set[tuple[str, str, int]] = set()
        self.max_hop_id = 0
        for row in self._iter_rows(self.wallet_hops_path):
            try:
                hop_id = int(row.get("hop_id", ""))
                hop_level = int(row.get("hop_level", ""))
            except (TypeError, ValueError) as exc:
                raise ValueError("hop_id and hop_level must be integers") from exc
            if hop_id < 0 or hop_level < 0:
                raise ValueError("hop_id and hop_level must be non-negative")

            wallet = row.get("wallet_address", "")
            seed = row.get("seed_address", "")
            if not wallet or not seed:
                raise ValueError("wallet_address and seed_address are required")

            self.max_hop_id = max(self.max_hop_id, hop_id)
            key = (wallet, seed, hop_level)
            if key not in self.hop_keys:
                self.hop_keys.add(key)
                self.hops.append(row)

    def _load_crawl_state(self) -> None:
        """Load typed crawl state with latest append winning per address."""

        self.crawl_state: dict[str, dict[str, Any]] = {}
        for source_row in self._iter_rows(self.crawl_state_path):
            address = source_row.get("address", "")
            if not address:
                raise ValueError("crawl-state address is required")
            try:
                hop_level = int(source_row.get("hop_level", ""))
            except (TypeError, ValueError) as exc:
                raise ValueError("crawl-state hop_level must be an integer") from exc
            if hop_level < 0:
                raise ValueError("crawl-state hop_level must be non-negative")

            row: dict[str, Any] = dict(source_row)
            row["hop_level"] = hop_level
            for field in ("synced", "expanded", "is_supernode"):
                encoded = source_row.get(field, "").strip().upper()
                if encoded not in {"TRUE", "FALSE"}:
                    raise ValueError(f"crawl-state {field} must be TRUE or FALSE")
                row[field] = encoded == "TRUE"
            self.crawl_state[address] = row

    def load(self) -> None:
        """Rebuild all in-memory state from CSVs in dependency order."""

        self._load_seeds()
        self._load_accounts()
        self._load_transactions()
        self._load_hops()
        self._load_crawl_state()
        logger.info(
            "loaded CSV state: seeds=%d accounts=%d transactions=%d hops=%d crawl_state=%d",
            len(self.seeds),
            len(self.accounts),
            len(self.tx_hashes),
            len(self.hops),
            len(self.crawl_state),
        )

    def _append_account_rows(self, addresses: Sequence[str]) -> None:
        """Append current rows for known addresses once per requested address."""

        seen: set[str] = set()
        rows: list[Mapping[str, Any]] = []
        for address in addresses:
            if address not in seen and address in self.accounts:
                seen.add(address)
                rows.append(self.accounts[address])
        self._append_rows(
            self.accounts_path,
            self.headers["accounts.csv"],
            rows,
        )
