import copy

import pytest

from pipeline.api.parsers import parse_account
from pipeline.storage.schema import build_headers
from pipeline.utils.address import to_hex

ADDRESS = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
OTHER = "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb"
NOW = "2026-11-22T00:00:00Z"


@pytest.fixture(autouse=True)
def freeze_row_creation_time(monkeypatch):
    monkeypatch.setattr("pipeline.api.parsers.now_iso", lambda: NOW)


@pytest.mark.parametrize("address", [ADDRESS, to_hex(ADDRESS), "0x"+to_hex(ADDRESS)[2:]])
def test_account_uses_csv_schema_canonical_addresses_and_protocol_timestamps(address):
    raw = {"address": address, "create_time": 1700000000000, "latest_opration_time": 1700000060000, "latest_operation_time": 1800000000000}
    original = copy.deepcopy(raw)
    row = parse_account(raw)
    assert row == {
        "address": ADDRESS,
        "hex_address": to_hex(ADDRESS),
        "first_seen_timestamp": "2023-11-14T22:13:20Z",
        "last_seen_timestamp": "2023-11-14T22:14:20Z",
        "created_date": "2023-11-14",
        "created_at": NOW,
    }
    assert set(row) == set(build_headers()["accounts.csv"])
    assert raw == original


@pytest.mark.parametrize("raw", [None, [], {}, {"address": "bad"}, {"address": None}])
def test_account_unusable_address_returns_empty_row(raw):
    assert parse_account(raw) == {}


@pytest.mark.parametrize("value", [None, "", "bad", 0, True])
def test_account_missing_or_invalid_times_remain_empty(value):
    row = parse_account({"address": ADDRESS, "create_time": value, "latest_opration_time": value})
    assert row["first_seen_timestamp"] == ""
    assert row["last_seen_timestamp"] == ""
    assert row["created_date"] == ""
    assert row["created_at"] == NOW
