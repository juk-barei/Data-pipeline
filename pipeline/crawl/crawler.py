"""Fetch wallet batches using a shared, independently paced API-key pool."""

from __future__ import annotations

import asyncio
import math

from pipeline.api.client import USDT_CONTRACT, TronGridClient
from pipeline.config import Settings
from pipeline.storage.csv_store import CSVStore


class CrawlerWorker:
    """Coordinate wallet requests without owning the client or storage lifecycle."""

    def __init__(
        self,
        store: CSVStore,
        client: TronGridClient,
        settings: Settings,
        max_concurrency: int | None = None,
    ) -> None:
        """Wire dependencies without I/O; an explicit limit overrides autosizing.

        Each wallet issues three requests. The automatic wallet limit covers
        one second of aggregate key capacity, with settings.max_concurrency as
        a minimum. The pool, rather than this semaphore, enforces request rate.
        Public mode defaults to one wallet because it has no per-key pacing.
        """
        if max_concurrency is None:
            if client.pool.keys:
                capacity = len(client.pool.keys) * client.pool.rate_limit_per_key
                max_concurrency = max(settings.max_concurrency, math.ceil(capacity / 3))
            else:
                max_concurrency = 1
        if type(max_concurrency) is not int or max_concurrency < 1:
            raise ValueError("max_concurrency must be a positive integer")
        self.store = store
        self.client = client
        self.settings = settings
        self.max_concurrency = max_concurrency
        self.semaphore = asyncio.Semaphore(max_concurrency)

    async def _fetch_wallet(self, address: str) -> tuple:
        """Fetch three histories independently; retain failures for normalization.

        The client acquires a pooled key for every request/page and handles 429
        penalties and retries. Holding one permit bounds whole-wallet work.
        Cancellation remains a control signal, rather than a failed result.
        """
        async with self.semaphore:
            results = await asyncio.gather(
                self.client.get_account(address),
                self.client.get_trx_transactions(address),
                self.client.get_trc20_transfers(address, USDT_CONTRACT),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, asyncio.CancelledError):
                    raise result
            return (address, *results)
