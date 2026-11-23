"""Command-line entry points for the CSV pipeline."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path

from pipeline.api.client import TronGridClient
from pipeline.api.keypool import MultiKeyPool
from pipeline.config import Settings, load_env, load_settings
from pipeline.seeds.discover import fetch_licit_seeds
from pipeline.storage.csv_store import CSVStore


def build_parser() -> argparse.ArgumentParser:
    """Define pipeline commands without loading settings or touching data."""
    parser = argparse.ArgumentParser(prog="pipeline", description="CSV crawl pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    descriptions = {
        "fetch-seeds": "Discover and verify licit seeds",
        "run": "Crawl and expand the dataset",
        "monitor": "Print the offline health dashboard",
        "reset": "Reset the dataset to hop-0 seeds (stop writers first)",
        "compact": "Deduplicate append logs (stop writers first)",
        "sync": "Copy dataset exports to Drive (stop writers first)",
    }
    for name, description in descriptions.items():
        command = commands.add_parser(name, help=description, description=description)
        command.add_argument("--env-file", type=Path, help="Environment file to load")
        command.add_argument("--csv-dir", type=Path, help="Override CSV_DIR")
        if name == "run":
            command.add_argument("--step", action="store_true", help="Run one cycle only")
        elif name == "reset":
            command.add_argument("--yes", action="store_true", help="Confirm reset without prompting")
        elif name == "sync":
            command.add_argument("--compress", action="store_true", help="Export gzip copies before upload")
            command.add_argument("--dry-run", action="store_true", help="Preview upload without changing files")
            command.add_argument("--delete-local", action="store_true", help="Delete uploaded files after success")
    return parser


def _settings(args: argparse.Namespace) -> Settings:
    """Resolve environment settings once, with an optional CLI directory override."""
    settings = getattr(args, "settings", None)
    if settings is None:
        load_env(getattr(args, "env_file", None))
        settings = load_settings()
    directory = getattr(args, "csv_dir", None)
    return replace(settings, csv_dir=directory) if directory is not None else settings


@asynccontextmanager
async def _client(settings: Settings):
    """Keep HTTP creation and cleanup on the command's event loop."""
    pool = MultiKeyPool(list(settings.api_keys), settings.cooldown_sec, settings.rate_limit_per_key)
    client = TronGridClient(pool, base_url="https://api.trongrid.io", timeout=30.0)
    try:
        yield client
    finally:
        await client.aclose()


def cmd_fetch_seeds(args: argparse.Namespace) -> int:
    """Discover verified rows and register them using idempotent store insertion."""
    settings = _settings(args)
    store = CSVStore(str(settings.csv_dir))

    async def fetch() -> None:
        async with _client(settings) as client:
            seeds = await fetch_licit_seeds(client, settings.seed_count)
            store.insert_seeds(seeds)

    asyncio.run(fetch())
    return 0
