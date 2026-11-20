"""Helpers for keeping secrets out of logs."""


def mask_key(key: str) -> str:
    """Show only the first six characters of a non-trivial API key."""

    if not key:
        return ""
    if len(key) <= 6:
        return "*" * len(key)
    return f"{key[:6]}..."
