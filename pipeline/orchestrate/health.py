"""Report progress from the store's in-memory metrics."""

from __future__ import annotations


def is_complete(metrics: dict) -> bool:
    """Finish only after a nonempty frontier has no crawl or expand work."""
    return (
        metrics["total_crawl_states"] > 0
        and metrics["pending_crawls"] == 0
        and metrics["pending_expands"] == 0
    )
