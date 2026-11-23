"""Environment and logging configuration for the pipeline."""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from pathlib import Path
import tempfile

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    """Runtime settings parsed from environment variables."""

    api_keys: tuple[str, ...] = ()
    csv_dir: Path = Path("data")
    seed_count: int = 100
    batch_size: int = 5
    hop_depth: int = 5
    supernode_threshold: int = 50
    cooldown_sec: int = 60
    rate_limit_per_key: float = 14.5
    max_concurrency: int = 20
    drive_remote: str = ""
    drive_path: str = ""
    sync_every: int = 10


def load_env(path: str | Path | None = None) -> None:
    """Load environment values from ``path`` or the nearest ``.env`` file."""

    load_dotenv(dotenv_path=path, override=False)


def _get_int(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if value >= minimum else default


def _get_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if value > minimum else default


def load_settings() -> Settings:
    """Return validated settings from the current process environment."""

    raw_keys = os.getenv("TRONGRID_API_KEYS", "")
    api_keys = tuple(key.strip() for key in raw_keys.split(",") if key.strip())

    return Settings(
        api_keys=api_keys,
        csv_dir=Path(os.getenv("CSV_DIR", "data")).expanduser(),
        seed_count=_get_int("SEED_COUNT", 100),
        batch_size=_get_int("BATCH_SIZE", 5),
        hop_depth=_get_int("HOP_DEPTH", 5),
        supernode_threshold=_get_int("SUPERNODE_THRESHOLD", 50),
        cooldown_sec=_get_int("COOLDOWN_SEC", 60),
        rate_limit_per_key=_get_float("RATE_LIMIT_PER_KEY", 14.5),
        max_concurrency=_get_int("MAX_CONCURRENCY", 20),
        drive_remote=os.getenv("DRIVE_REMOTE", "").strip(),
        drive_path=os.getenv("DRIVE_PATH", "").strip(),
        sync_every=_get_int("SYNC_EVERY", 10, minimum=0),
    )


def setup_logging(level: str = "INFO") -> None:
    """Configure one consistent console handler on the root logger."""

    root = logging.getLogger()
    resolved_level = getattr(logging, level.upper(), logging.INFO)
    root.setLevel(resolved_level)

    handler = next(
        (item for item in root.handlers if getattr(item, "_tron_pipeline", False)),
        None,
    )
    if handler is None:
        handler = logging.StreamHandler()
        handler._tron_pipeline = True  # type: ignore[attr-defined]
        root.addHandler(handler)

    handler.setLevel(resolved_level)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )


def validate_config(settings: Settings) -> list[str]:
    """Report startup configuration errors without mutating frozen settings.

    Duplicate keys are rejected and counted once for throughput, matching the
    key pool's deduplication. Callers may disable sync in a replaced Settings
    value for operations that do not use Drive. Offline commands need not call
    this network startup validator.
    """
    errors: list[str] = []
    keys = []
    for key in settings.api_keys:
        if not isinstance(key, str) or not key.strip() or key != key.strip():
            errors.append("TRONGRID_API_KEYS must contain nonblank keys without surrounding whitespace")
        else:
            keys.append(key)
    unique_keys = set(keys)
    if not unique_keys:
        errors.append("TRONGRID_API_KEYS requires at least one API key")
    if len(unique_keys) != len(keys):
        errors.append("TRONGRID_API_KEYS contains duplicate keys; configure each key once")
    rate = settings.rate_limit_per_key
    if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate <= 0:
        errors.append("RATE_LIMIT_PER_KEY must be finite and positive")
    else:
        logging.getLogger(__name__).info(
            "Expected aggregate throughput: %g requests/s across %d unique API keys",
            len(unique_keys) * rate, len(unique_keys),
        )
    for field in ("seed_count", "batch_size", "hop_depth", "supernode_threshold", "cooldown_sec", "max_concurrency"):
        value = getattr(settings, field)
        if type(value) is not int or value < 1:
            errors.append(f"{field} must be a positive integer")
    if type(settings.sync_every) is not int or settings.sync_every < 0:
        errors.append("SYNC_EVERY must be a nonnegative integer")
    elif settings.sync_every > 0:
        remote = settings.drive_remote.strip().removesuffix(":")
        if not remote or remote.startswith("-") or any(char in remote for char in ":/\\\r\n\x00"):
            errors.append("DRIVE_REMOTE must name a configured rclone remote when sync is enabled")
        if any(char in settings.drive_path for char in "\r\n\x00"):
            errors.append("DRIVE_PATH cannot contain control characters")
    try:
        directory = Path(settings.csv_dir).expanduser()
        directory.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryFile(dir=directory):
            pass
    except (OSError, ValueError, TypeError):
        errors.append("CSV_DIR must be a writable directory")
    return errors
