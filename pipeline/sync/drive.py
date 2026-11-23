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
