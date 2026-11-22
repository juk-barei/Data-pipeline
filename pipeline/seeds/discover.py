"""Discover normalized seed candidates from confirmed USDT transfers."""

from __future__ import annotations

import asyncio
import time

from pipeline.api.client import PAGE_SIZE, USDT_CONTRACT, TronGridClient
from pipeline.seeds.blacklist import load_local_blacklist
from pipeline.seeds.verify import verify_licit
from pipeline.utils.address import normalize_address, to_hex
from pipeline.utils.timeutils import now_iso


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


async def fetch_licit_seeds(
    client: TronGridClient,
    target: int,
    concurrency: int = 10,
    *,
    blacklist_paths: list[str] | None = None,
) -> list[dict]:
    """Return exactly target unique verified seed rows, or raise RuntimeError.

    concurrency is the verification limit per pooled key (one worker group in
    public mode). Optional blacklist_paths supplies local CSV blacklists.
    Discovery budgets grow when candidates are rejected; already checked
    addresses are not verified again. All unfinished probes are cancelled and
    awaited on success, failure or cancellation. This function writes no CSVs.
    """
    if isinstance(target, bool) or not isinstance(target, int) or target < 0:
        raise ValueError("target must be a nonnegative integer")
    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
        raise ValueError("concurrency must be a positive integer")
    if target == 0:
        return []

    blacklist = load_local_blacklist(blacklist_paths or [])
    workers = concurrency * max(1, len(client.pool.keys))
    candidate_target = max(target, workers)
    seen: set[str] = set()
    verified: list[str] = []
    while len(verified) < target:
        candidates = await discover_candidates(client, candidate_target)
        fresh = iter(sorted(candidates - seen))
        seen.update(candidates)
        pending: dict[asyncio.Task, str] = {}
        tasks: list[asyncio.Task] = []
        try:
            while True:
                while len(pending) < workers:
                    address = next(fresh, None)
                    if address is None:
                        break
                    task = asyncio.create_task(verify_licit(client, address, blacklist))
                    pending[task] = address
                    tasks.append(task)
                if not pending:
                    break
                done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    address = pending.pop(task)
                    if task.result() and len(verified) < target:
                        verified.append(address)
                if len(verified) >= target:
                    break
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        if len(verified) >= target:
            break
        if len(candidates) < candidate_target:
            raise RuntimeError(
                f"Requested {target} verified seeds, found {len(verified)} before discovery exhausted"
            )
        candidate_target *= 2

    timestamp = now_iso()
    return [
        {
            "wallet_address": address,
            "hex_address": to_hex(address),
            "transaction_id": "",
            "source_name": "TronGrid_Licit_USDT",
            "label": "0",
            "created_at": timestamp,
        }
        for address in sorted(verified)
    ]
