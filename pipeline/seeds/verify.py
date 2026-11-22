"""Check seed candidates against local and remote risk sources."""

from __future__ import annotations

import logging

from pipeline.api.client import TronGridClient
from pipeline.utils.address import normalize_address


logger = logging.getLogger(__name__)


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
