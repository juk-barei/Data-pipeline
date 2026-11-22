"""Discover normalized seed candidates from confirmed USDT transfers."""

from __future__ import annotations

import asyncio
import time

from pipeline.api.client import PAGE_SIZE, USDT_CONTRACT, TronGridClient
from pipeline.utils.address import normalize_address


async def discover_candidates(
    client: TronGridClient, target: int, limit_per_req: int = 200
) -> set[str]:
    """Return up to target unique counterparties, or fewer if history ends.

    Each key gets a disjoint time range with its own pagination cursor. Recent
    workers cover one day each; the final worker covers all older history.
    Requests use the client's shared pacing, retries and per-key cooldowns.
    Outstanding requests are cancelled and awaited once the target is met.
    """
    if isinstance(target, bool) or not isinstance(target, int) or target < 0:
        raise ValueError("target must be a nonnegative integer")
    if (
        isinstance(limit_per_req, bool)
        or not isinstance(limit_per_req, int)
        or not 1 <= limit_per_req <= PAGE_SIZE
    ):
        raise ValueError(f"limit_per_req must be between 1 and {PAGE_SIZE}")
    if target == 0:
        return set()

    candidates: set[str] = set()
    workers = max(1, len(client.pool.keys))
    upper = int(time.time() * 1000)
    ranges: list[tuple[int, int]] = []
    for index in range(workers):
        lower = 0 if index == workers - 1 else max(0, upper - 86_400_000 + 1)
        ranges.append((lower, upper))
        if lower == 0:
            break
        upper = lower - 1

    async def read_range(lower: int, upper: int) -> None:
        params = {
            "event_name": "Transfer",
            "only_confirmed": "true",
            "order_by": "block_timestamp,desc",
            "limit": limit_per_req,
            "min_timestamp": lower,
            "max_timestamp": upper,
        }
        seen_cursors: set[str] = set()
        while len(candidates) < target:
            response = await client._request(f"/v1/contracts/{USDT_CONTRACT}/events", params)
            if len(candidates) >= target:
                return
            page = response.get("data", [])
            if not isinstance(page, list) or any(not isinstance(row, dict) for row in page):
                raise ValueError("TronGrid event data must be a list of objects")
            for event in page:
                result = event.get("result", {})
                if not isinstance(result, dict):
                    continue
                for field in ("from", "to"):
                    address = normalize_address(result.get(field))
                    if address:
                        candidates.add(address)
                    if len(candidates) >= target:
                        return
            if not page:
                return
            meta = response.get("meta", {})
            if not isinstance(meta, dict):
                raise ValueError("TronGrid pagination metadata must be an object")
            cursor = meta.get("fingerprint")
            if cursor is None or cursor == "":
                return
            if not isinstance(cursor, str):
                raise ValueError("TronGrid fingerprint must be a string")
            if cursor in seen_cursors:
                return
            seen_cursors.add(cursor)
            params["fingerprint"] = cursor

    tasks = [asyncio.create_task(read_range(lower, upper)) for lower, upper in ranges]
    pending = set(tasks)
    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
            if len(candidates) >= target:
                break
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return candidates
