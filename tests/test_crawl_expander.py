from unittest.mock import Mock

import pytest

from pipeline.config import Settings
from pipeline.crawl.expander import ExpanderWorker
from pipeline.storage.csv_store import CSVStore
from pipeline.utils.address import to_hex

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
