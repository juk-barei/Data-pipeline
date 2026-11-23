"""Stream local dataset exports and copy them to a configured rclone remote."""

from __future__ import annotations

import csv
import gzip
import os
from pathlib import Path
import shutil
import tempfile


def ensure_rclone() -> str:
    """Locate rclone without invoking it, or explain how to install it."""
    executable = shutil.which("rclone")
    if executable is None:
        raise FileNotFoundError(
            "rclone was not found on PATH; install it from https://rclone.org/install/ "
            "and configure a remote with 'rclone config'."
        )
    return executable


def build_dest(remote: str, remote_path: str) -> str:
    """Compose a configured remote and a root-relative remote directory."""
    name = remote.strip().removesuffix(":")
    if not name or any(character in name for character in ":/\\\r\n") or name.startswith("-"):
        raise ValueError("remote must be a configured rclone remote name")
    if "\x00" in remote_path or "\x00" in name:
        raise ValueError("rclone destination cannot contain a null character")
    return f"{name}:{remote_path.strip('/')}"


def compress_gzip(path: Path) -> Path:
    """Stream to an atomic sibling .gz export without changing the source."""
    path = Path(path)
    destination = path.with_suffix(path.suffix + ".gz")
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".gz.tmp", delete=False) as handle:
        temporary = Path(handle.name)
    try:
        with path.open("rb") as source, temporary.open("wb") as output:
            with gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def csv_to_parquet(path: Path) -> Path:
    """Stream CSV batches to atomic zstd Parquet, preserving CSV values as strings.

    Keeping string columns avoids inference changing addresses, large integers,
    leading zeros, or types between batches. Install the ``parquet`` extra to
    enable this export; ordinary CSV/gzip syncing does not require pyarrow.
    """
    try:
        import pyarrow as pa
        import pyarrow.csv as arrow_csv
        import pyarrow.parquet as parquet
    except ImportError as error:
        raise ImportError("Parquet export requires pip install 'tron-data-pipeline[parquet]'") from error

    path = Path(path)
    destination = path.with_suffix(".parquet")
    with path.open(encoding="utf-8-sig", newline="") as source:
        columns = next(csv.reader(source), [])
    if not columns or any(not column for column in columns) or len(set(columns)) != len(columns):
        raise ValueError("CSV must have a nonempty header with unique column names")
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".parquet.tmp", delete=False) as handle:
        temporary = Path(handle.name)
    try:
        with arrow_csv.open_csv(
            path,
            read_options=arrow_csv.ReadOptions(block_size=1024 * 1024),
            parse_options=arrow_csv.ParseOptions(newlines_in_values=True),
            convert_options=arrow_csv.ConvertOptions(
                column_types={column: pa.string() for column in columns},
                strings_can_be_null=False,
            ),
        ) as reader:
            with parquet.ParquetWriter(temporary, reader.schema, compression="zstd") as writer:
                for batch in reader:
                    writer.write_batch(batch)
        with temporary.open("r+b") as output:
            os.fsync(output.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination
