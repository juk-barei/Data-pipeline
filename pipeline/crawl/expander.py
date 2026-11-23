"""Expand synced wallet histories into a bounded graph frontier."""

from __future__ import annotations

import heapq

from pipeline.config import Settings
from pipeline.storage.csv_store import CSVStore
from pipeline.utils.address import normalize_address, to_hex


class ExpanderWorker:
    """Derive graph edges without owning the store lifecycle."""

    def __init__(self, store: CSVStore, settings: Settings) -> None:
        """Retain dependencies without reading or writing storage."""
        self.store = store
        self.settings = settings

    async def run_batch(self) -> dict:
        """Expand claimed histories and persist discoveries before completion.

        Crawl state is global per wallet; hop rows retain each seed's lineage.
        Reuse an already-expanded wallet's stored history for a newly reaching
        seed, including after restart. Promoting a discovered wallet to a seed
        resets its global depth to zero and replays its synced history once.
        This method has no awaits: one event-loop call computes and persists
        the batch without another call interleaving snapshot claims.
        """
        batch = self.store.claim_expand_batch(self.settings.batch_size)
        claimed = {row["address"] for row in batch}
        promoted_roots = {}
        for seed in self.store.seeds:
            state = self.store.crawl_state.get(seed)
            if state is None or (state["hop_level"] == 0 and state["seed_address"] == seed):
                continue
            if state["synced"] and seed not in claimed:
                if len(batch) >= self.settings.batch_size:
                    continue
                batch.append(dict(state))
                claimed.add(seed)
            # A partial frontier append must leave the normalized root claimable.
            promoted_roots[seed] = {**state, "seed_address": seed, "hop_level": 0,
                                    "expanded": False}
        summary = dict(claimed=len(batch), expanded=0, pruned=0,
                       accounts=0, frontier=0, hops=0)
        if not batch and not promoted_roots:
            return summary

        lineages: dict[str, dict[str, int]] = {}

        def remember(address: str, seed: str, level: int) -> None:
            levels = lineages.setdefault(address, {})
            levels[seed] = min(level, levels.get(seed, level))

        for state in self.store.crawl_state.values():
            remember(state["address"], state["seed_address"], state["hop_level"])
        for seed in self.store.seeds:
            remember(seed, seed, 0)
        for hop in self.store.hops:
            remember(hop["wallet_address"], hop["seed_address"], int(hop["hop_level"]))

        queue = [(level, row["address"], seed) for row in batch
                 for seed, level in lineages[row["address"]].items()]
        heapq.heapify(queue)
        processed: set[tuple[str, str]] = set()
        histories: dict[str, list[dict]] = {}
        accounts: dict[str, dict] = {}
        frontier: dict[str, dict] = dict(promoted_roots)
        hops: dict[tuple[str, str, int], dict] = {}
        active_claims = set()
        while queue:
            level, address, seed = heapq.heappop(queue)
            if (address, seed) in processed or level > lineages[address][seed]:
                continue
            processed.add((address, seed))
            state = self.store.crawl_state[address]
            if state["is_supernode"] or level >= self.settings.hop_depth:
                continue
            if address in claimed:
                active_claims.add(address)
            if address not in histories:
                histories[address] = self._collect_counterparties(address)
            for candidate in histories[address]:
                target = candidate["target"]
                next_level = level + 1
                known_level = lineages.get(target, {}).get(seed)
                if known_level is not None and next_level > known_level:
                    continue
                remember(target, seed, next_level)
                key = (target, seed, next_level)
                if key not in self.store.hop_keys:
                    hops.setdefault(key, {
                        "wallet_address": target, "seed_address": seed,
                        "parent_address": address, "via_tx_hash": candidate["via_tx"],
                        "hop_level": next_level, "direction": candidate["direction"],
                    })
                if target not in self.store.accounts:
                    accounts.setdefault(target, {"address": target, "hex_address": to_hex(target)})
                current = self.store.crawl_state.get(target)
                pending = frontier.get(target, current)
                if pending is None or next_level < pending["hop_level"]:
                    # Retain fetched history/flags if only the shortest global
                    # path changed; do not force another network crawl.
                    frontier[target] = {
                        "address": target, "seed_address": seed, "hop_level": next_level,
                        "synced": current["synced"] if current else False,
                        "expanded": current["expanded"] if current else False,
                        "is_supernode": current["is_supernode"] if current else False,
                    }
                if current and current["synced"] and (current["expanded"] or target in claimed):
                    heapq.heappush(queue, (next_level, target, seed))

        self.store.add_expansion_results(
            list(accounts.values()), list(frontier.values()), list(hops.values()),
            [row["address"] for row in batch],
        )
        summary.update(expanded=len(active_claims), pruned=len(batch) - len(active_claims),
                       accounts=len(accounts), frontier=len(frontier), hops=len(hops))
        return summary

    def _classify_direction(self, tx: dict, address: str) -> str | None:
        """Classify a canonical wallet's transfer direction; ignore self-transfers."""
        if not isinstance(tx, dict):
            return None
        address = normalize_address(address)
        sender = normalize_address(tx.get("from_address"))
        receiver = normalize_address(tx.get("to_address"))
        if not address or not sender or not receiver or sender == receiver:
            return None
        if address == sender:
            return "FORWARD"
        if address == receiver:
            return "BACKWARD"
        return None

    def _collect_counterparties(self, address: str) -> list[dict]:
        """Return one edge candidate per canonical target in stored tx order.

        If transfers exist in both directions, retain the first transaction's
        direction and hash as deterministic discovery provenance.
        """
        address = normalize_address(address)
        if not address:
            return []
        candidates = []
        seen = set()
        for tx in self.store.get_wallet_transactions(address):
            direction = self._classify_direction(tx, address)
            if direction is None:
                continue
            target = normalize_address(tx["to_address"] if direction == "FORWARD"
                                       else tx["from_address"])
            tx_hash = tx.get("tx_hash")
            if not isinstance(tx_hash, str) or not tx_hash.strip() or target in seen:
                continue
            seen.add(target)
            candidates.append({"target": target, "direction": direction, "via_tx": tx_hash})
        return candidates
