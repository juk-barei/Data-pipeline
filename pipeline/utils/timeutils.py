"""Fault-tolerant UTC timestamp parsing and formatting."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional


def _parse_numeric(value: float) -> datetime:
    integer = int(value)
    text = str(abs(integer))
    if value == integer and len(text) == 8:
        return datetime.strptime(text, "%Y%m%d").replace(tzinfo=timezone.utc)

    magnitude = abs(value)
    if magnitude >= 1e18:
        value /= 1e9
    elif magnitude >= 1e15:
        value /= 1e6
    elif magnitude >= 1e12:
        value /= 1e3
    return datetime.fromtimestamp(value, tz=timezone.utc)


def parse_timestamp(ts: Any) -> Optional[datetime]:
    """Parse common timestamp forms into UTC; return ``None`` on failure."""

    if ts is None or ts == "" or ts == 0:
        return None
    try:
        if isinstance(ts, datetime):
            parsed = ts
        elif isinstance(ts, (int, float)) and not isinstance(ts, bool):
            parsed = _parse_numeric(float(ts))
        elif isinstance(ts, str):
            value = ts.strip()
            if not value:
                return None
            if value.replace(".", "", 1).isdigit():
                parsed = _parse_numeric(float(value))
            else:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (OverflowError, OSError, TypeError, ValueError):
        return None


def format_iso(dt: Optional[datetime]) -> str:
    """Format a datetime as an ISO-8601 UTC string."""

    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""

    return format_iso(datetime.now(timezone.utc))
