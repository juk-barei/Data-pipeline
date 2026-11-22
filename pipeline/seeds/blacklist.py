"""Load normalized wallet addresses from local blacklist CSV files."""

from __future__ import annotations

import csv

from pipeline.utils.address import normalize_address


def load_local_blacklist(paths: list[str]) -> set[str]:
    """Read address columns; ignore missing files and unusable addresses.

    Supported headers are ``wallet_address``, ``address`` and ``hex_address``
    (case insensitive). Empty files are allowed; nonempty files without an
    address column raise ValueError so a misconfigured blacklist is visible.
    Other filesystem and CSV errors propagate to the caller.
    """
    addresses: set[str] = set()
    for path in paths:
        try:
            handle = open(path, encoding="utf-8-sig", newline="")
        except FileNotFoundError:
            continue
        with handle:
            rows = csv.reader(handle, strict=True)
            headers = next(rows, None)
            if headers is None:
                continue
            columns = [
                index
                for index, name in enumerate(headers)
                if name.strip().lower() in {"wallet_address", "address", "hex_address"}
            ]
            if not columns:
                raise ValueError(f"Blacklist CSV has no address column: {path}")
            for row in rows:
                for index in columns:
                    if index < len(row):
                        address = normalize_address(row[index])
                        if address:
                            addresses.add(address)
    return addresses
