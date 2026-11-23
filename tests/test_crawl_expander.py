import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from pipeline.config import Settings
from pipeline.crawl.expander import ExpanderWorker
from pipeline.crawl.crawler import CrawlerWorker
from pipeline.api.keypool import MultiKeyPool
from pipeline.storage.csv_store import CSVStore
from pipeline.utils.address import to_base58, to_hex

ADDRESS = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
OTHER = "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb"
THIRD = "TUoHaVjx7n5xz8LwPRDckgFrDWhMhuSuJM"


def test_constructor_retains_dependencies_without_io():
    store = Mock()
    settings = Settings(hop_depth=3, batch_size=7)
    worker = ExpanderWorker(store, settings)
    assert worker.store is store
    assert worker.settings is settings
    assert store.mock_calls == []


@pytest.mark.parametrize("sender,receiver,address,expected", [
    (ADDRESS, OTHER, ADDRESS, "FORWARD"),
    (OTHER, ADDRESS, ADDRESS, "BACKWARD"),
    (ADDRESS, ADDRESS, ADDRESS, None),
    (OTHER, THIRD, ADDRESS, None),
    (to_hex(ADDRESS), OTHER, ADDRESS, "FORWARD"),
    (ADDRESS, to_hex(ADDRESS), ADDRESS, None),
    (OTHER, ADDRESS, to_hex(ADDRESS), "BACKWARD"),
    ("bad", ADDRESS, ADDRESS, None),
    (ADDRESS, None, ADDRESS, None),
    (ADDRESS, OTHER, "bad", None),
])
def test_classify_direction(sender, receiver, address, expected):
    tx = {"from_address": sender, "to_address": receiver}
    original = dict(tx)
    worker = ExpanderWorker(Mock(), Settings())
    assert worker._classify_direction(tx, address) == expected
    assert tx == original
    assert worker.store.mock_calls == []


@pytest.mark.parametrize("tx", [None, [], {}, "bad"])
def test_classify_direction_ignores_invalid_rows(tx):
    assert ExpanderWorker(Mock(), Settings())._classify_direction(tx, ADDRESS) is None


def test_collect_counterparties_deduplicates_canonical_targets_in_stored_order():
    store = Mock()
    store.get_wallet_transactions.return_value = [
        {"tx_hash": "first", "from_address": to_hex(OTHER), "to_address": ADDRESS},
        {"tx_hash": "later", "from_address": ADDRESS, "to_address": OTHER},
        {"tx_hash": "third", "from_address": ADDRESS, "to_address": THIRD},
        {"tx_hash": "self", "from_address": ADDRESS, "to_address": ADDRESS},
        {"tx_hash": "unrelated", "from_address": OTHER, "to_address": THIRD},
        {"tx_hash": "bad", "from_address": ADDRESS, "to_address": "invalid"},
        {"from_address": ADDRESS, "to_address": THIRD},
    ]
    worker = ExpanderWorker(store, Settings())
    assert worker._collect_counterparties(to_hex(ADDRESS)) == [
        {"target": OTHER, "direction": "BACKWARD", "via_tx": "first"},
        {"target": THIRD, "direction": "FORWARD", "via_tx": "third"},
    ]
    store.get_wallet_transactions.assert_called_once_with(ADDRESS)


def test_collect_counterparties_reads_real_store_after_reload(tmp_path):
    store = CSVStore(str(tmp_path))
    store.upsert_crawled_data([], [
        {"tx_hash": "out", "from_address": ADDRESS, "to_address": OTHER},
        {"tx_hash": "in", "from_address": THIRD, "to_address": ADDRESS},
    ], [], [])
    worker = ExpanderWorker(CSVStore(str(tmp_path)), Settings())
    assert worker._collect_counterparties(ADDRESS) == [
        {"target": OTHER, "direction": "FORWARD", "via_tx": "out"},
        {"target": THIRD, "direction": "BACKWARD", "via_tx": "in"},
    ]
    assert worker._collect_counterparties(OTHER) == [
        {"target": ADDRESS, "direction": "BACKWARD", "via_tx": "out"}
    ]


def test_collect_counterparties_empty_invalid_or_failed_storage():
    store = Mock()
    worker = ExpanderWorker(store, Settings())
    assert worker._collect_counterparties("invalid") == []
    store.get_wallet_transactions.assert_not_called()
    store.get_wallet_transactions.return_value = []
    assert worker._collect_counterparties(ADDRESS) == []
    store.get_wallet_transactions.side_effect = OSError("unreadable history")
    with pytest.raises(OSError, match="unreadable history"):
        worker._collect_counterparties(ADDRESS)


def test_collect_counterparties_skips_missing_hash_before_deduplicating():
    store = Mock()
    store.get_wallet_transactions.return_value = [
        {"from_address": ADDRESS, "to_address": OTHER},
        {"tx_hash": "", "from_address": ADDRESS, "to_address": OTHER},
        {"tx_hash": "valid", "from_address": ADDRESS, "to_address": OTHER},
    ]
    assert ExpanderWorker(store, Settings())._collect_counterparties(ADDRESS) == [
        {"target": OTHER, "direction": "FORWARD", "via_tx": "valid"}
    ]


def graph_address(index):
    return to_base58("41" + f"{index:040x}")


def test_run_batch_idle_does_not_write(tmp_path):
    store = CSVStore(str(tmp_path))
    before = {name: path.read_bytes() for name, path in store.paths.items()}
    assert asyncio.run(ExpanderWorker(store, Settings()).run_batch()) == dict(
        claimed=0, expanded=0, pruned=0, accounts=0, frontier=0, hops=0
    )
    assert before == {name: path.read_bytes() for name, path in store.paths.items()}


def test_run_batch_persists_directions_and_frontier_once_then_reloads(tmp_path):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    store.upsert_crawled_data([], [
        {"tx_hash": "out", "from_address": ADDRESS, "to_address": OTHER},
        {"tx_hash": "again", "from_address": ADDRESS, "to_address": OTHER},
        {"tx_hash": "in", "from_address": THIRD, "to_address": ADDRESS},
        {"tx_hash": "self", "from_address": ADDRESS, "to_address": ADDRESS},
    ], [ADDRESS], [])
    result = asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    assert result == dict(claimed=1, expanded=1, pruned=0, accounts=2, frontier=2, hops=2)
    reopened = CSVStore(str(tmp_path))
    assert reopened.crawl_state[ADDRESS]["expanded"] is True
    for target in (OTHER, THIRD):
        state = reopened.crawl_state[target]
        assert state["seed_address"] == ADDRESS
        assert state["hop_level"] == 1
        assert state["synced"] is state["expanded"] is False
        assert reopened.accounts[target]["hex_address"] == to_hex(target)
    assert {(h["wallet_address"], h["direction"], h["via_tx_hash"]) for h in reopened.hops} == {
        (OTHER, "FORWARD", "out"), (THIRD, "BACKWARD", "in")
    }
    assert asyncio.run(ExpanderWorker(reopened, Settings()).run_batch())["claimed"] == 0
    assert len(reopened.hops) == 2


@pytest.mark.parametrize("depth", [1, 3, 5])
def test_run_batch_prunes_configured_depth_and_supernodes(tmp_path, monkeypatch, depth):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    store.add_expansion_results([], [
        {"address": OTHER, "seed_address": ADDRESS, "hop_level": depth},
        {"address": THIRD, "seed_address": ADDRESS, "hop_level": 1},
    ], [], [])
    store.upsert_crawled_data([], [], [OTHER, THIRD], [THIRD])
    worker = ExpanderWorker(store, Settings(hop_depth=depth))
    collect = Mock(side_effect=AssertionError("pruned wallet must not read history"))
    monkeypatch.setattr(worker, "_collect_counterparties", collect)
    result = asyncio.run(worker.run_batch())
    assert result["claimed"] == result["pruned"] == 2
    assert result["expanded"] == result["hops"] == 0
    collect.assert_not_called()
    reopened = CSVStore(str(tmp_path))
    assert reopened.crawl_state[OTHER]["expanded"] is True
    assert reopened.crawl_state[THIRD]["expanded"] is True
    assert reopened.crawl_state[ADDRESS]["expanded"] is False


def test_run_batch_shared_wallet_preserves_seed_lineages(tmp_path):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}, {"wallet_address": OTHER}])
    store.upsert_crawled_data([], [
        {"tx_hash": "a-shared", "from_address": ADDRESS, "to_address": THIRD},
        {"tx_hash": "b-shared", "from_address": OTHER, "to_address": THIRD},
    ], [ADDRESS, OTHER], [])
    worker = ExpanderWorker(store, Settings())
    assert asyncio.run(worker.run_batch())["frontier"] == 1
    assert {(h["wallet_address"], h["seed_address"], h["hop_level"]) for h in store.hops} == {
        (THIRD, ADDRESS, 1), (THIRD, OTHER, 1)
    }
    child = graph_address(7)
    store.upsert_crawled_data([], [
        {"tx_hash": "shared-child", "from_address": THIRD, "to_address": child}
    ], [THIRD], [])
    # Both root lineages must reach the child, even though crawl_state has one seed.
    worker = ExpanderWorker(CSVStore(str(tmp_path)), Settings())
    asyncio.run(worker.run_batch())
    assert (child, ADDRESS, 2) in worker.store.hop_keys
    assert (child, OTHER, 2) in worker.store.hop_keys


def test_run_batch_late_seed_reuses_already_expanded_histories_after_restart(tmp_path):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    child = graph_address(8)
    store.upsert_crawled_data([], [
        {"tx_hash": "a-shared", "from_address": ADDRESS, "to_address": THIRD},
        {"tx_hash": "shared-child", "from_address": THIRD, "to_address": child},
    ], [ADDRESS], [])
    asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    store.upsert_crawled_data([], [], [THIRD], [])
    asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    assert store.crawl_state[THIRD]["expanded"] is True
    store.insert_seeds([{"wallet_address": OTHER}])
    store.upsert_crawled_data([], [
        {"tx_hash": "b-shared", "from_address": OTHER, "to_address": THIRD}
    ], [OTHER], [])
    reopened = CSVStore(str(tmp_path))
    result = asyncio.run(ExpanderWorker(reopened, Settings()).run_batch())
    assert result["claimed"] == 1
    assert (THIRD, OTHER, 1) in reopened.hop_keys
    assert (child, OTHER, 2) in reopened.hop_keys
    assert reopened.crawl_state[THIRD]["synced"] is True
    assert reopened.crawl_state[THIRD]["expanded"] is True


def test_run_batch_read_or_write_failure_leaves_parent_retryable(tmp_path, monkeypatch):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    store.upsert_crawled_data([], [
        {"tx_hash": "edge", "from_address": ADDRESS, "to_address": OTHER}
    ], [ADDRESS], [])
    worker = ExpanderWorker(store, Settings())
    original_read = store.get_wallet_transactions

    def fail_read(*args):
        raise OSError("history unavailable")

    monkeypatch.setattr(store, "get_wallet_transactions", fail_read)
    with pytest.raises(OSError, match="history unavailable"):
        asyncio.run(worker.run_batch())
    assert store.crawl_state[ADDRESS]["expanded"] is False
    monkeypatch.setattr(store, "get_wallet_transactions", original_read)
    original_append = store._append_rows

    def fail_hops(path, header, rows):
        if path == store.wallet_hops_path:
            raise OSError("disk full")
        original_append(path, header, rows)

    monkeypatch.setattr(store, "_append_rows", fail_hops)
    with pytest.raises(OSError, match="disk full"):
        asyncio.run(worker.run_batch())
    assert CSVStore(str(tmp_path)).crawl_state[ADDRESS]["expanded"] is False
    monkeypatch.setattr(store, "_append_rows", original_append)
    assert asyncio.run(worker.run_batch())["expanded"] == 1
    assert CSVStore(str(tmp_path)).hop_keys == {(OTHER, ADDRESS, 1)}


def test_offline_crawl_expand_reaches_hop_five_and_stops_with_reload(tmp_path):
    addresses = [graph_address(i) for i in range(10, 17)]
    payloads = [{
        "txID": f"edge-{i}", "block_timestamp": 1700000000000,
        "raw_data": {"contract": [{"type": "TransferContract", "parameter": {"value": {
            "owner_address": to_hex(addresses[i]),
            "to_address": to_hex(addresses[i + 1]), "amount": 1_000_000,
        }}}]},
    } for i in range(6)]

    async def native(address):
        index = addresses.index(address)
        return payloads[max(0, index - 1):index + 1]

    client = SimpleNamespace(pool=MultiKeyPool(["fixture"], 60),
                             get_account=AsyncMock(return_value=None),
                             get_trx_transactions=native,
                             get_trc20_transfers=AsyncMock(return_value=[]))
    settings = Settings(batch_size=2)
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": addresses[0]}])
    for _ in range(8):
        asyncio.run(CrawlerWorker(store, client, settings).run_batch())
        asyncio.run(ExpanderWorker(store, settings).run_batch())
        store = CSVStore(str(tmp_path))
        if store.get_metrics()["is_complete"]:
            break
    assert store.get_metrics()["is_complete"] is True
    assert set(store.crawl_state) == set(addresses[:6])
    assert addresses[6] not in store.accounts
    assert [store.crawl_state[a]["hop_level"] for a in addresses[:6]] == list(range(6))
    assert all(row["synced"] and row["expanded"] for row in store.crawl_state.values())
    assert store.hop_keys == {(address, addresses[0], i) for i, address in enumerate(addresses[1:6], 1)}


def test_run_batch_shorter_path_reuses_synced_history_and_preserves_flags(tmp_path):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    child = graph_address(30)
    store.add_expansion_results([], [
        {"address": THIRD, "seed_address": ADDRESS, "hop_level": 5},
    ], [{"wallet_address": THIRD, "seed_address": ADDRESS, "hop_level": 5}], [])
    store.upsert_crawled_data([], [
        {"tx_hash": "short", "from_address": ADDRESS, "to_address": THIRD},
        {"tx_hash": "child", "from_address": THIRD, "to_address": child},
    ], [THIRD], [])
    # The previous depth-5 visit was pruned, but its history was synced.
    asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    assert store.crawl_state[THIRD]["expanded"] is True
    store.upsert_crawled_data([], [], [ADDRESS], [])
    asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    reopened = CSVStore(str(tmp_path))
    assert reopened.crawl_state[THIRD]["hop_level"] == 1
    assert reopened.crawl_state[THIRD]["synced"] is True
    assert reopened.crawl_state[THIRD]["expanded"] is True
    assert (child, ADDRESS, 2) in reopened.hop_keys


def test_run_batch_depth_is_applied_per_seed_lineage(tmp_path):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}, {"wallet_address": OTHER}])
    child = graph_address(31)
    store.add_expansion_results([], [
        {"address": THIRD, "seed_address": ADDRESS, "hop_level": 1},
    ], [
        {"wallet_address": THIRD, "seed_address": ADDRESS, "hop_level": 1},
        {"wallet_address": THIRD, "seed_address": OTHER, "hop_level": 5},
    ], [])
    store.upsert_crawled_data([], [
        {"tx_hash": "child", "from_address": THIRD, "to_address": child}
    ], [THIRD], [])
    asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    assert (child, ADDRESS, 2) in store.hop_keys
    assert not any(h["wallet_address"] == child and h["seed_address"] == OTHER for h in store.hops)


def test_run_batch_replays_durable_hop_after_frontier_write_failure(tmp_path, monkeypatch):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    store.upsert_crawled_data([], [
        {"tx_hash": "edge", "from_address": ADDRESS, "to_address": OTHER}
    ], [ADDRESS], [])

    def fail_frontier(*args):
        raise OSError("frontier write failed")

    monkeypatch.setattr(store, "_append_crawl_state_rows", fail_frontier)
    with pytest.raises(OSError, match="frontier write failed"):
        asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    reopened = CSVStore(str(tmp_path))
    assert (OTHER, ADDRESS, 1) in reopened.hop_keys
    assert OTHER not in reopened.crawl_state
    assert reopened.crawl_state[ADDRESS]["expanded"] is False
    asyncio.run(ExpanderWorker(reopened, Settings()).run_batch())
    final = CSVStore(str(tmp_path))
    assert final.crawl_state[OTHER]["hop_level"] == 1
    assert final.crawl_state[ADDRESS]["expanded"] is True
    assert len(final.hops) == 1


def test_run_batch_cyclic_graph_terminates_with_one_claim_per_call(tmp_path):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}, {"wallet_address": OTHER}])
    store.upsert_crawled_data([], [
        {"tx_hash": "a-b", "from_address": ADDRESS, "to_address": OTHER},
        {"tx_hash": "b-c", "from_address": OTHER, "to_address": THIRD},
        {"tx_hash": "c-a", "from_address": THIRD, "to_address": ADDRESS},
    ], [ADDRESS, OTHER], [])
    worker = ExpanderWorker(store, Settings(batch_size=1))
    assert asyncio.run(worker.run_batch())["claimed"] == 1
    assert asyncio.run(worker.run_batch())["claimed"] == 1
    store.upsert_crawled_data([], [], [THIRD], [])
    assert asyncio.run(worker.run_batch())["claimed"] == 1
    assert asyncio.run(worker.run_batch())["claimed"] == 0
    assert {(h["wallet_address"], h["seed_address"]) for h in store.hops} == {
        (OTHER, ADDRESS), (THIRD, ADDRESS), (ADDRESS, OTHER), (THIRD, OTHER)
    }
    assert all(int(h["hop_level"]) == 1 for h in store.hops)


def test_run_batch_promoted_expanded_wallet_becomes_seed_root_after_restart(tmp_path):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    store.upsert_crawled_data([], [
        {"tx_hash": "a-b", "from_address": ADDRESS, "to_address": OTHER},
        {"tx_hash": "b-c", "from_address": OTHER, "to_address": THIRD},
    ], [ADDRESS], [])
    asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    store.upsert_crawled_data([], [], [OTHER], [])
    asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    assert store.crawl_state[OTHER]["expanded"] is True
    store.insert_seeds([{"wallet_address": OTHER}])
    reopened = CSVStore(str(tmp_path))
    result = asyncio.run(ExpanderWorker(reopened, Settings()).run_batch())
    assert result["claimed"] == 1
    final = CSVStore(str(tmp_path))
    assert (THIRD, OTHER, 1) in final.hop_keys
    assert final.crawl_state[OTHER]["hop_level"] == 0
    assert final.crawl_state[OTHER]["seed_address"] == OTHER
    assert final.crawl_state[OTHER]["synced"] is True
    assert final.crawl_state[OTHER]["expanded"] is True
    assert asyncio.run(ExpanderWorker(final, Settings()).run_batch())["claimed"] == 0


def test_run_batch_promoted_unsynced_wallet_stays_retryable_at_depth_zero(tmp_path):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    store.add_expansion_results([], [
        {"address": OTHER, "seed_address": ADDRESS, "hop_level": 4},
    ], [], [])
    store.insert_seeds([{"wallet_address": OTHER}])
    result = asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    assert result["claimed"] == result["expanded"] == 0
    assert result["frontier"] == 1
    reopened = CSVStore(str(tmp_path))
    state = reopened.crawl_state[OTHER]
    assert state["seed_address"] == OTHER
    assert state["hop_level"] == 0
    assert state["synced"] is state["expanded"] is False
    assert OTHER in {row["address"] for row in reopened.claim_crawl_batch(10)}


def test_run_batch_promoted_seeds_respect_batch_limit(tmp_path):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    store.add_expansion_results([], [
        {"address": OTHER, "seed_address": ADDRESS, "hop_level": 1},
        {"address": THIRD, "seed_address": ADDRESS, "hop_level": 1},
    ], [], [ADDRESS])
    store.upsert_crawled_data([], [], [OTHER, THIRD], [])
    store.add_expansion_results([], [], [], [OTHER, THIRD])
    store.insert_seeds([{"wallet_address": OTHER}, {"wallet_address": THIRD}])
    worker = ExpanderWorker(store, Settings(batch_size=1))
    assert asyncio.run(worker.run_batch())["claimed"] == 1
    assert asyncio.run(worker.run_batch())["claimed"] == 1
    assert asyncio.run(worker.run_batch())["claimed"] == 0
    assert store.crawl_state[OTHER]["hop_level"] == store.crawl_state[THIRD]["hop_level"] == 0


def test_promoted_seed_retries_after_root_only_frontier_write_and_restart(tmp_path, monkeypatch):
    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    store.upsert_crawled_data([], [
        {"tx_hash": "a-b", "from_address": ADDRESS, "to_address": OTHER},
        {"tx_hash": "b-c", "from_address": OTHER, "to_address": THIRD},
    ], [ADDRESS], [])
    asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    store.upsert_crawled_data([], [], [OTHER], [])
    asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    store.insert_seeds([{"wallet_address": OTHER}])
    original_append = store._append_crawl_state_rows

    def root_only_then_fail(addresses):
        assert addresses[0] == OTHER
        original_append([OTHER])
        raise OSError("partial frontier append")

    monkeypatch.setattr(store, "_append_crawl_state_rows", root_only_then_fail)
    with pytest.raises(OSError, match="partial frontier append"):
        asyncio.run(ExpanderWorker(store, Settings()).run_batch())
    reopened = CSVStore(str(tmp_path))
    assert reopened.crawl_state[OTHER]["seed_address"] == OTHER
    assert reopened.crawl_state[OTHER]["hop_level"] == 0
    assert reopened.crawl_state[OTHER]["expanded"] is False
    assert reopened.crawl_state[THIRD]["hop_level"] == 2
    assert (THIRD, OTHER, 1) in reopened.hop_keys
    asyncio.run(ExpanderWorker(reopened, Settings()).run_batch())
    final = CSVStore(str(tmp_path))
    assert final.crawl_state[OTHER]["expanded"] is True
    assert final.crawl_state[THIRD]["seed_address"] == OTHER
    assert final.crawl_state[THIRD]["hop_level"] == 1
