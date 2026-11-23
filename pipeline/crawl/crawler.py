"""Fetch wallet batches using a shared, independently paced API-key pool."""

from __future__ import annotations

import asyncio
import math

from pipeline.api.client import TronGridClient
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
