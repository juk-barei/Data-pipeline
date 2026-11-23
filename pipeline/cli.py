"""Command-line entry points for the CSV pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path


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
