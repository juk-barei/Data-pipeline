"""Stream local dataset exports and copy them to a configured rclone remote."""

from __future__ import annotations

import shutil


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
