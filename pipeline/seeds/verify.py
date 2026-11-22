"""Check seed candidates against local and remote risk sources."""

from __future__ import annotations

import asyncio
import logging
import os
import re

from pipeline.api.client import TronGridClient
from pipeline.utils.address import normalize_address


logger = logging.getLogger(__name__)
_RISK_MARKER = re.compile(r"\b(?:scam(?:mers?|ming|s)?|phish(?:ers?|ing)?|hack(?:ers?|ed|ing)?)\b")


async def is_banned_on_tether(client: TronGridClient, address: str) -> bool:
    """Return Tether's blacklist flag, logging errors and failing open.

    Invalid addresses return False without I/O. Cancellation propagates to the
    caller; it must never turn into an apparently successful verification.
    """
    canonical = normalize_address(address)
    if not canonical:
        return False
    try:
        return await client.call_is_blacklisted(canonical)
    except Exception as exc:
        # Exception messages can contain request headers or credentials.
        logger.warning("Tether check failed for %s (%s)", canonical, type(exc).__name__)
        return False


async def is_flagged_on_tronscan(client: TronGridClient, address: str) -> bool:
    """Read TronScan account risk flags, failing open when unavailable.

    TRONSCAN_API_KEY is separate from the TronGrid pool. Reuse the HTTP
    connection pool, but bypass _request, which adds a TronGrid credential.
    Missing credentials and invalid addresses make no HTTP requests.
    """
    canonical = normalize_address(address)
    key = os.getenv("TRONSCAN_API_KEY", "").strip()
    if not canonical or not key:
        return False
    try:
        response = await client._client.get(
            "https://apilist.tronscanapi.com/api/accountv2",
            params={"address": canonical},
            headers={"TRON-PRO-API-KEY": key},
            follow_redirects=False,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("TronScan account response must be an object")
        if payload.get("feedbackRisk") is True:
            return True
        # redTag is explicitly a risk label, unlike ordinary public labels.
        red_tag = payload.get("redTag")
        if isinstance(red_tag, str) and red_tag.strip():
            return True
        for field in ("addressTag", "publicTag", "greyTag"):
            tag = payload.get(field)
            if isinstance(tag, str) and _RISK_MARKER.search(tag.lower().replace("_", " ")):
                return True
        return False
    except Exception as exc:
        logger.warning("TronScan check failed for %s (%s)", canonical, type(exc).__name__)
        return False


async def verify_licit(client: TronGridClient, address: str, blacklist: set[str]) -> bool:
    """Accept a valid address only when no configured risk source rejects it.

    The blacklist must contain normalized addresses, as load_local_blacklist
    returns. A local rejection requires no network I/O. Remote checks overlap
    and a rejection cancels and awaits the other request. Each probe retains
    its documented fail-open behavior; cancellation and unexpected bugs still
    propagate instead of silently accepting the candidate.
    """
    canonical = normalize_address(address)
    if not canonical or canonical in blacklist:
        return False
    tasks = [
        asyncio.create_task(is_banned_on_tether(client, canonical)),
        asyncio.create_task(is_flagged_on_tronscan(client, canonical)),
    ]
    pending = set(tasks)
    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            # Read every completed result so an unexpected probe error cannot
            # be hidden by a rejection that completed in the same loop turn.
            results = [task.result() for task in done]
            if any(results):
                return False
        return True
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
