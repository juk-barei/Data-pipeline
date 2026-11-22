"""Asynchronous TronGrid access using independently paced API keys."""

from __future__ import annotations

import math

import httpx

from pipeline.api.keypool import MultiKeyPool


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
