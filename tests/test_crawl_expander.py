from unittest.mock import Mock

import pytest

from pipeline.config import Settings
from pipeline.crawl.expander import ExpanderWorker
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
