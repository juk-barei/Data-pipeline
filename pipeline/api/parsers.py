"""Convert TronGrid records to the canonical CSV row schemas."""

from __future__ import annotations

from pipeline.utils.address import normalize_address, to_hex
from pipeline.utils.timeutils import format_iso, now_iso, parse_timestamp


def parse_account(raw: dict) -> dict:
    """Normalize an account record; return an empty row for invalid addresses."""
    if not isinstance(raw, dict):
        return {}
    address = normalize_address(raw.get("address"))
    if not address:
        return {}
    first_seen = format_iso(parse_timestamp(raw.get("create_time")))
    last_seen = format_iso(parse_timestamp(raw.get("latest_opration_time")))
    return {
        "address": address,
        "hex_address": to_hex(address),
        "first_seen_timestamp": first_seen,
        "last_seen_timestamp": last_seen,
        "created_date": first_seen[:10],
        "created_at": now_iso(),
    }
