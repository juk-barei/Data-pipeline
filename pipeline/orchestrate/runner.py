"""Coordinate crawl, expansion, and maintenance on one event loop."""

from __future__ import annotations

import asyncio
import logging

from pipeline.api.client import TronGridClient
from pipeline.config import Settings
from pipeline.crawl.crawler import CrawlerWorker
from pipeline.crawl.expander import ExpanderWorker
from pipeline.orchestrate.health import HealthReporter, is_complete
from pipeline.storage.csv_store import CSVStore

logger = logging.getLogger(__name__)


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

    async def run_forever(self) -> None:
        """Run until complete, yielding between cycles and maintaining durable CSVs.

        Sync snapshots the latest cycle before optional compaction. Both are
        serialized against this runner's cycles. Sync runs off the event-loop
        thread with the mutation lock held. Cancellation waits for its snapshot /
        subprocess to finish before releasing the store to another cycle.
        Cancellation and KeyboardInterrupt propagate; the caller closes the client.
        All mutation of this store must go through this runner while it is active.
        """
        async with self._run_lock:
            # Keep one-cycle and monitoring use independent of optional Drive sync.
            if self.settings.sync_every != 0:
                from pipeline.sync.drive import maybe_sync

            while True:
                async with self._cycle_lock:
                    if is_complete(self.store.get_metrics()):
                        return
                summary = await self.run_once()
                async with self._cycle_lock:
                    if self.settings.sync_every != 0:
                        sync_task = asyncio.create_task(asyncio.to_thread(
                            maybe_sync, self.store, self.settings, self._sync_state,
                        ))
                        try:
                            await asyncio.shield(sync_task)
                        except (asyncio.CancelledError, KeyboardInterrupt):
                            # Cancelling to_thread does not stop its thread. Drain
                            # even repeated cancellation before unlocking storage.
                            while not sync_task.done():
                                try:
                                    await asyncio.shield(sync_task)
                                except asyncio.CancelledError:
                                    continue
                                except Exception:
                                    break
                            if not sync_task.cancelled() and sync_task.exception() is not None:
                                error = sync_task.exception()
                                logger.error("Drive sync failed during shutdown",
                                             exc_info=(type(error), error, error.__traceback__))
                            raise
                    self.store.compact_if_needed()
                    if is_complete(self.store.get_metrics()):
                        return
                idle = not (summary["crawl"]["claimed"] or summary["expand"]["claimed"])
                await asyncio.sleep(0.1 if idle else 0)
