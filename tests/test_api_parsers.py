import copy

import pytest

from pipeline.api.parsers import parse_account, parse_trc20_transfer, parse_trx_transaction
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


@pytest.fixture
def trx_payload():
    return {
        "txID": "native-hash",
        "blockNumber": 123,
        "block_timestamp": 1700000000000,
        "ret": [{"contractRet": "SUCCESS", "fee": 1000}],
        "raw_data": {"contract": [{
            "type": "TransferContract",
            "parameter": {"value": {
                "owner_address": to_hex(ADDRESS),
                "to_address": to_hex(OTHER),
                "amount": 1234567,
            }},
        }]},
    }


def test_trx_normalizes_native_units_addresses_and_csv_schema(trx_payload):
    original = copy.deepcopy(trx_payload)
    row = parse_trx_transaction(trx_payload)
    assert row == {
        "tx_hash": "native-hash", "block_number": 123,
        "block_timestamp": "2023-11-14T22:13:20Z",
        "from_address": ADDRESS, "to_address": OTHER,
        "token_symbol": "TRX", "contract_address": "",
        "amount": "1.234567", "fee_trx": "0.001", "created_at": NOW,
    }
    assert set(row) == set(build_headers()["transactions.csv"])
    assert trx_payload == original


@pytest.mark.parametrize("field,value", [
    ("txID", None), ("txID", ""), ("txID", 123),
    ("block_timestamp", None), ("block_timestamp", "bad"),
    ("ret", [{"contractRet": "REVERT"}]), ("ret", [None]),
    ("raw_data", None), ("raw_data", {"contract": []}),
    ("fee", -1), ("fee", True),
])
def test_trx_unusable_or_failed_records_are_dropped(trx_payload, field, value):
    trx_payload[field] = value
    assert parse_trx_transaction(trx_payload) is None


@pytest.mark.parametrize("field,value", [
    ("owner_address", "bad"), ("to_address", None),
    ("amount", None), ("amount", -1), ("amount", True),
    ("amount", 1.5), ("amount", "NaN"),
])
def test_trx_invalid_transfer_fields_are_dropped(trx_payload, field, value):
    trx_payload["raw_data"]["contract"][0]["parameter"]["value"][field] = value
    assert parse_trx_transaction(trx_payload) is None


def test_trx_skips_smart_contract_calls(trx_payload):
    trx_payload["raw_data"]["contract"][0]["type"] = "TriggerSmartContract"
    assert parse_trx_transaction(trx_payload) is None


def test_trx_accepts_integer_strings_and_preserves_smallest_unit(trx_payload):
    trx_payload["raw_data"]["contract"][0]["parameter"]["value"]["amount"] = "1"
    trx_payload["fee"] = 0
    row = parse_trx_transaction(trx_payload)
    assert row["amount"] == "0.000001"
    assert row["fee_trx"] == "0"


@pytest.mark.parametrize("raw", [None, [], "bad", {}])
def test_trx_invalid_outer_payload_is_safe(raw):
    assert parse_trx_transaction(raw) is None


@pytest.fixture
def trc20_payload():
    return {
        "transaction_id": "token-hash", "block_timestamp": 1700000000000,
        "from": to_hex(ADDRESS), "to": OTHER, "type": "Transfer",
        "value": "1234567",
        "token_info": {"address": ADDRESS, "symbol": "USDT", "decimals": "6"},
    }


def test_trc20_normalizes_six_decimals_and_matches_csv_schema(trc20_payload):
    original = copy.deepcopy(trc20_payload)
    row = parse_trc20_transfer(trc20_payload)
    assert row == {
        "tx_hash": "token-hash", "block_number": "",
        "block_timestamp": "2023-11-14T22:13:20Z",
        "from_address": ADDRESS, "to_address": OTHER,
        "token_symbol": "USDT", "contract_address": ADDRESS,
        "amount": "1.234567", "fee_trx": "", "created_at": NOW,
    }
    assert set(row) == set(build_headers()["transactions.csv"])
    assert trc20_payload == original


@pytest.mark.parametrize("value,decimals,expected", [
    ("1", 6, "0.000001"), (0, 0, "0"), (12, 0, "12"),
    ("123456789012345678901234567890123456789", 6,
     "123456789012345678901234567890123.456789"),
])
def test_trc20_preserves_exact_amounts(trc20_payload, value, decimals, expected):
    trc20_payload["value"] = value
    trc20_payload["token_info"]["decimals"] = decimals
    assert parse_trc20_transfer(trc20_payload)["amount"] == expected


@pytest.mark.parametrize("field,value", [
    ("transaction_id", None), ("transaction_id", ""),
    ("block_timestamp", None), ("block_timestamp", "bad"),
    ("from", "bad"), ("to", None), ("token_info", []),
    ("value", "NaN"), ("value", -1), ("value", True),
    ("value", 1.5), ("type", "Approval"),
])
def test_trc20_unusable_records_are_dropped(trc20_payload, field, value):
    trc20_payload[field] = value
    assert parse_trc20_transfer(trc20_payload) is None


@pytest.mark.parametrize("field,value", [
    ("decimals", None), ("decimals", -1), ("decimals", 256),
    ("decimals", True), ("decimals", "6.0"), ("decimals", 1.5),
    ("address", "bad"), ("symbol", []),
])
def test_trc20_invalid_token_metadata_is_dropped(trc20_payload, field, value):
    trc20_payload["token_info"][field] = value
    assert parse_trc20_transfer(trc20_payload) is None


@pytest.mark.parametrize("raw", [None, [], "bad", {}])
def test_trc20_invalid_outer_payload_is_safe(raw):
    assert parse_trc20_transfer(raw) is None
