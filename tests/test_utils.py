import asyncio
from datetime import datetime, timezone

import pytest

from pipeline.utils.address import normalize_address, to_base58, to_hex
from pipeline.utils.async_utils import async_retry, chunked
from pipeline.utils.fs import ensure_dir
from pipeline.utils.security import mask_key
from pipeline.utils.timeutils import format_iso, now_iso, parse_timestamp

ADDRESS = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
ADDRESS_HEX = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"


def test_ensure_dir_is_idempotent(tmp_path):
    target = tmp_path / "nested" / "directory"
    assert ensure_dir(target) == target
    assert ensure_dir(target) == target
    assert target.is_dir()


def test_address_round_trip_and_normalization():
    assert to_base58(ADDRESS_HEX) == ADDRESS
    assert to_base58("0x" + ADDRESS_HEX[2:]) == ADDRESS
    assert to_hex(ADDRESS) == ADDRESS_HEX
    assert normalize_address(ADDRESS_HEX.upper()) == ADDRESS
    assert normalize_address(ADDRESS) == ADDRESS


def test_invalid_address_never_raises():
    assert to_hex(ADDRESS[:-1] + "x") == ""
    assert normalize_address("invalid") == ""


@pytest.mark.parametrize(
    "value",
    [
        1_700_000_000,
        1_700_000_000_000,
        1_700_000_000_000_000,
        1_700_000_000_000_000_000,
        "2023-11-14T22:13:20Z",
        20231114,
    ],
)
def test_parse_timestamp_supported_forms(value):
    parsed = parse_timestamp(value)
    assert parsed is not None
    assert parsed.tzinfo == timezone.utc


@pytest.mark.parametrize("value", [None, "", 0, "not-a-date", object()])
def test_parse_timestamp_invalid_returns_none(value):
    assert parse_timestamp(value) is None


def test_iso_helpers():
    dt = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    assert format_iso(None) == ""
    assert format_iso(dt) == "2024-01-02T03:04:05Z"
    assert parse_timestamp(now_iso()) is not None


def test_mask_key_never_returns_full_key():
    assert mask_key("123456789") == "123456..."
    assert mask_key("short") == "*****"


def test_async_retry_succeeds_after_failures(monkeypatch):
    calls = 0
    delays = []

    async def fake_sleep(delay):
        delays.append(delay)

    async def operation():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("retry")
        return "done"

    monkeypatch.setattr("pipeline.utils.async_utils.asyncio.sleep", fake_sleep)
    result = asyncio.run(async_retry(operation, attempts=3, base_delay=0.5))
    assert result == "done"
    assert calls == 3
    assert delays == [0.5, 1.0]


def test_chunked_handles_short_and_empty_chunks():
    assert list(chunked(range(5), 2)) == [[0, 1], [2, 3], [4]]
    assert list(chunked([], 2)) == []
    with pytest.raises(ValueError):
        list(chunked([1], 0))
