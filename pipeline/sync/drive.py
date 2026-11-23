"""Stream local dataset exports and copy them to a configured rclone remote."""

from __future__ import annotations

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
