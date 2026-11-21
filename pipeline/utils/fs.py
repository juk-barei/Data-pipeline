"""Filesystem helpers."""

from pathlib import Path


def ensure_dir(path: Path) -> Path:
    """Create ``path`` and its parents, then return the same path."""

    path.mkdir(parents=True, exist_ok=True)
    return path
