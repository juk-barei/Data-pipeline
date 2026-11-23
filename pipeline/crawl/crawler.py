"""Fetch wallet batches using a shared, independently paced API-key pool."""

from __future__ import annotations

import asyncio
import math

from pipeline.api.client import USDT_CONTRACT, TronGridClient
from pipeline.api.parsers import parse_account, parse_trc20_transfer, parse_trx_transaction
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
        self._batch_lock = asyncio.Lock()

    async def run_batch(self) -> dict:
        """Persist one claimed batch; summary counts describe this batch's rows.

        Claims are snapshots, so concurrent calls on this worker are serialized.
        Successful partial rows are durable even when their wallet must retry.
        Storage errors propagate; CSVStore reloads its durable state on failure.
        """
        async with self._batch_lock:
            batch = self.store.claim_crawl_batch(self.settings.batch_size)
            summary = dict(claimed=len(batch), synced=0, failed=0,
                           accounts=0, transactions=0, supernodes=0)
            if not batch:
                return summary
            tasks = [asyncio.create_task(self._fetch_wallet(row["address"])) for row in batch]
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            accounts = []
            transactions = {}
            synced_addresses = []
            for claimed, result in zip(batch, results):
                if isinstance(result, asyncio.CancelledError):
                    raise result
                if isinstance(result, Exception):
                    continue
                address, raw_account, raw_trx, raw_trc20 = result
                account, txs, synced = self._normalize(raw_account, raw_trx, raw_trc20)
                if address != claimed["address"]:
                    continue
                if account and account["address"] != address:
                    # Never persist an API account under a different claimed wallet.
                    synced = False
                    account = {}
                if account:
                    accounts.append(account)
                for tx in txs:
                    transactions.setdefault(tx["tx_hash"], tx)
                if synced:
                    synced_addresses.append(address)
            tx_rows = list(transactions.values())
            supernodes = self._detect_supernodes(tx_rows, [row["address"] for row in batch])
            self.store.upsert_crawled_data(accounts, tx_rows, synced_addresses, supernodes)
            summary.update(synced=len(synced_addresses), failed=len(batch) - len(synced_addresses),
                           accounts=len(accounts), transactions=len(tx_rows), supernodes=len(supernodes))
            return summary

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

    def _normalize(self, raw_account, raw_trx, raw_trc20) -> tuple:
        """Keep usable partial data, but mark synced only if all requests succeeded.

        A missing account (None) is a successful unknown-account response.
        Parsers may intentionally drop non-transfer or unusable transactions.
        Transaction identity follows CSVStore's first-row-wins tx_hash key.
        """
        synced = True
        account_row = {}
        if isinstance(raw_account, dict):
            account_row = parse_account(raw_account)
            if not account_row:
                synced = False
        elif raw_account is not None:
            synced = False

        tx_rows = []
        seen = set()
        for payload, parser in (
            (raw_trx, parse_trx_transaction),
            (raw_trc20, parse_trc20_transfer),
        ):
            if not isinstance(payload, list):
                synced = False
                continue
            for raw in payload:
                if not isinstance(raw, dict):
                    synced = False
                    continue
                row = parser(raw)
                if row is not None and row["tx_hash"] not in seen:
                    seen.add(row["tx_hash"])
                    tx_rows.append(row)
        return account_row, tx_rows, synced

    def _detect_supernodes(self, tx_rows, addresses) -> list[str]:
        """Flag batch wallets touching at least the configured number of txs.

        Count unique transaction hashes per address, including a self-transfer
        once. The same transaction can appear in several fetched histories.
        """
        counts = {address: set() for address in addresses}
        for row in tx_rows:
            tx_hash = row.get("tx_hash")
            if not tx_hash:
                continue
            for address in {row.get("from_address"), row.get("to_address")}:
                if address in counts:
                    counts[address].add(tx_hash)
        return [address for address, hashes in counts.items()
                if len(hashes) >= self.settings.supernode_threshold]
