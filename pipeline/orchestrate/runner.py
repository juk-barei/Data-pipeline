"""Coordinate crawl, expansion, and maintenance on one event loop."""

from __future__ import annotations

import asyncio

from pipeline.api.client import TronGridClient
from pipeline.config import Settings
from pipeline.crawl.crawler import CrawlerWorker
from pipeline.crawl.expander import ExpanderWorker
from pipeline.orchestrate.health import HealthReporter
from pipeline.storage.csv_store import CSVStore


class Runner:
    """Coordinate shared dependencies; their creator retains lifecycle ownership."""

    def __init__(self, store: CSVStore, client: TronGridClient, settings: Settings) -> None:
        """Assemble workers without network or storage I/O.

        The caller owns closing the client. CSVStore durably closes each append
        or snapshot itself; it has no persistent handle for this runner to close.
        """
        self.store = store
        self.client = client
        self.settings = settings
        self.crawler = CrawlerWorker(store, client, settings)
        self.expander = ExpanderWorker(store, settings)
        self.health = HealthReporter(store, client.pool)
        self._cycle_lock = asyncio.Lock()
        self._run_lock = asyncio.Lock()
        self._sync_state: dict = {}

    async def run_once(self) -> dict:
        """Run crawl then expansion, retaining each worker's named counters.

        Serializing complete cycles prevents overlapping claims on this runner.
        Worker exceptions and cancellation propagate after worker cleanup.
        """
        async with self._cycle_lock:
            crawl = await self.crawler.run_batch()
            expand = await self.expander.run_batch()
            return {"crawl": crawl, "expand": expand}
