"""Asynchronous TronGrid access using independently paced API keys."""

from __future__ import annotations

import logging
import math
from urllib.parse import quote

import httpx

from pipeline.api.keypool import MultiKeyPool
from pipeline.utils.async_utils import async_retry
from pipeline.utils.address import normalize_address, to_hex


logger = logging.getLogger(__name__)

MAX_RECORDS_PER_ADDRESS = 10_000
PAGE_SIZE = 200
USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"


class TronGridClient:
    """Own a reusable HTTP connection pool and a shared key pool."""

    def __init__(
        self,
        pool: MultiKeyPool,
        base_url: str,
        timeout: float,
        limits: httpx.Limits | None = None,
    ) -> None:
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be finite and positive")
        if limits is None:
            # Reserve enough in-flight slots to keep each paced key occupied.
            connections = max(20, math.ceil(len(pool.keys) * pool.rate_limit_per_key))
            limits = httpx.Limits(
                max_connections=connections,
                max_keepalive_connections=connections,
            )
        self.pool = pool
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            limits=limits,
            http2=True,
        )

    async def aclose(self) -> None:
        """Release the HTTP connections owned by this client."""
        await self._client.aclose()

    async def _request(
        self,
        path: str,
        params: dict | None = None,
        *,
        method: str = "GET",
        json: dict | None = None,
    ) -> dict:
        """Request JSON, rotating throttled keys within three bounded rounds."""

        async def attempt() -> dict:
            # Cap each round at the pool size before normal retry backoff.
            # The pool itself controls pacing and waits for cooldown expiry.
            for index in range(max(1, len(self.pool.keys))):
                key = await self.pool.get_key()
                headers = {"TRON-PRO-API-KEY": key} if key else {}
                response = await self._client.request(
                    method, path, params=params, json=json, headers=headers
                )
                if response.status_code == 404:
                    return {}
                if response.status_code == 429:
                    self.pool.penalize(key)
                    if index + 1 < max(1, len(self.pool.keys)):
                        continue
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("TronGrid response must be a JSON object")
                return payload
            raise RuntimeError("unreachable")

        return await async_retry(attempt, attempts=3)


    async def get_account(self, address: str) -> dict | None:
        """Return the first account record, or None for an unknown account."""
        response = await self._request(f"/v1/accounts/{quote(address, safe='')}")
        data = response.get("data", [])
        if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
            raise ValueError("TronGrid account data must be a list of objects")
        return data[0] if data else None


    async def get_trx_transactions(self, address: str) -> list[dict]:
        """Fetch confirmed account history, bounded by the per-address cap."""
        params = {"only_confirmed": "true", "limit": PAGE_SIZE}
        records: list[dict] = []
        seen: set[str] = set()
        while len(records) < MAX_RECORDS_PER_ADDRESS:
            response = await self._request(
                f"/v1/accounts/{quote(address, safe='')}/transactions", params
            )
            page = response.get("data", [])
            if not isinstance(page, list) or any(not isinstance(row, dict) for row in page):
                raise ValueError("TronGrid transaction data must be a list of objects")
            records.extend(page[: MAX_RECORDS_PER_ADDRESS - len(records)])
            if not page or len(records) >= MAX_RECORDS_PER_ADDRESS:
                break
            meta = response.get("meta", {})
            if not isinstance(meta, dict):
                raise ValueError("TronGrid pagination metadata must be an object")
            fingerprint = meta.get("fingerprint")
            if fingerprint is None:
                break
            if not isinstance(fingerprint, str):
                raise ValueError("TronGrid fingerprint must be a string")
            if not fingerprint:
                break
            if fingerprint in seen:
                break
            seen.add(fingerprint)
            params["fingerprint"] = fingerprint
        return records


    async def get_trc20_transfers(self, address: str, contract: str) -> list[dict]:
        """Fetch confirmed token transfers for one contract, up to the cap."""
        params = {
            "only_confirmed": "true",
            "limit": PAGE_SIZE,
            "contract_address": contract,
        }
        records: list[dict] = []
        seen: set[str] = set()
        while len(records) < MAX_RECORDS_PER_ADDRESS:
            response = await self._request(
                f"/v1/accounts/{quote(address, safe='')}/transactions/trc20", params
            )
            page = response.get("data", [])
            if not isinstance(page, list) or any(not isinstance(row, dict) for row in page):
                raise ValueError("TronGrid transaction data must be a list of objects")
            records.extend(page[: MAX_RECORDS_PER_ADDRESS - len(records)])
            if not page or len(records) >= MAX_RECORDS_PER_ADDRESS:
                break
            meta = response.get("meta", {})
            if not isinstance(meta, dict):
                raise ValueError("TronGrid pagination metadata must be an object")
            fingerprint = meta.get("fingerprint")
            if fingerprint is None:
                break
            if not isinstance(fingerprint, str):
                raise ValueError("TronGrid fingerprint must be a string")
            if not fingerprint:
                break
            if fingerprint in seen:
                break
            seen.add(fingerprint)
            params["fingerprint"] = fingerprint
        return records


    async def get_recent_usdt_transfers(self, limit: int) -> list[dict]:
        """Return up to limit recent confirmed USDT Transfer events."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit must be a nonnegative integer")
        if limit == 0:
            return []
        params = {
            "event_name": "Transfer",
            "only_confirmed": "true",
            "order_by": "block_timestamp,desc",
            "limit": min(PAGE_SIZE, limit),
        }
        records: list[dict] = []
        seen: set[str] = set()
        while len(records) < limit:
            response = await self._request(f"/v1/contracts/{USDT_CONTRACT}/events", params)
            page = response.get("data", [])
            if not isinstance(page, list) or any(not isinstance(row, dict) for row in page):
                raise ValueError("TronGrid event data must be a list of objects")
            records.extend(page[: limit - len(records)])
            if not page or len(records) >= limit:
                break
            meta = response.get("meta", {})
            if not isinstance(meta, dict):
                raise ValueError("TronGrid pagination metadata must be an object")
            fingerprint = meta.get("fingerprint")
            if fingerprint is None:
                break
            if not isinstance(fingerprint, str):
                raise ValueError("TronGrid fingerprint must be a string")
            if not fingerprint:
                break
            if fingerprint in seen:
                break
            seen.add(fingerprint)
            params["fingerprint"] = fingerprint
        return records


    async def call_is_blacklisted(self, address: str) -> bool:
        """Read Tether's blacklist flag; unavailable or invalid results are False."""
        canonical = normalize_address(address)
        if not canonical:
            return False
        hex_address = to_hex(canonical)
        try:
            response = await self._request(
                "/wallet/triggerconstantcontract",
                method="POST",
                json={
                    "owner_address": hex_address,
                    "contract_address": to_hex(USDT_CONTRACT),
                    "function_selector": "isBlackListed(address)",
                    # ABI addresses omit TRON's 41 network prefix.
                    "parameter": hex_address[2:].rjust(64, "0"),
                    "visible": False,
                },
            )
            status = response.get("result", {})
            if not isinstance(status, dict) or status.get("result") is not True:
                return False
            values = response.get("constant_result")
            if not isinstance(values, list) or not values:
                return False
            word = values[0]
            if not isinstance(word, str) or len(word) != 64:
                return False
            if any(char not in "0123456789abcdefABCDEF" for char in word):
                return False
            return int(word, 16) == 1
        except Exception as exc:
            logger.warning("Blacklist query failed for %s (%s)", canonical, type(exc).__name__)
            return False


async def wait_for_api(client: TronGridClient, attempts: int = 3) -> None:
    """Probe the current block through the authenticated, paced client.

    ``attempts`` bounds probe calls; each uses the client's existing bounded
    transport retries. Cancellation propagates immediately. The caller owns
    the client and must close it even when this startup check fails.
    """
    if type(attempts) is not int or attempts < 1:
        raise ValueError("attempts must be a positive integer")
    for attempt in range(attempts):
        try:
            response = await client._request("/wallet/getnowblock", method="POST", json={})
            if not isinstance(response.get("blockID"), str) or not response["blockID"]:
                raise ValueError("TronGrid startup probe returned no block ID")
            return
        except Exception:
            if attempt == attempts - 1:
                # Transport exceptions may include sensitive URLs/headers.
                raise RuntimeError(
                    f"TronGrid startup probe failed after {attempts} attempts; "
                    "check API keys and network access."
                ) from None
