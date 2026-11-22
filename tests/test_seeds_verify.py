import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pipeline.seeds.verify import is_banned_on_tether
from pipeline.utils.address import to_hex


ADDRESS = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"


@pytest.mark.parametrize("banned", [True, False])
def test_tether_returns_the_contract_blacklist_flag(banned):
    client = SimpleNamespace(call_is_blacklisted=AsyncMock(return_value=banned))
    assert asyncio.run(is_banned_on_tether(client, ADDRESS)) is banned
    client.call_is_blacklisted.assert_awaited_once_with(ADDRESS)


def test_tether_normalizes_addresses_before_the_contract_call():
    client = SimpleNamespace(call_is_blacklisted=AsyncMock(return_value=True))
    assert asyncio.run(is_banned_on_tether(client, "0x" + to_hex(ADDRESS)[2:])) is True
    client.call_is_blacklisted.assert_awaited_once_with(ADDRESS)


@pytest.mark.parametrize("address", [None, "", "bad", ADDRESS[:-1] + "x"])
def test_invalid_tether_address_skips_the_contract_call(address):
    client = SimpleNamespace(call_is_blacklisted=AsyncMock())
    assert asyncio.run(is_banned_on_tether(client, address)) is False
    client.call_is_blacklisted.assert_not_awaited()


def test_tether_failure_is_logged_and_fails_open_without_exposing_credentials(caplog):
    secret = "private-test-credential"
    client = SimpleNamespace(call_is_blacklisted=AsyncMock(side_effect=RuntimeError(secret)))
    assert asyncio.run(is_banned_on_tether(client, ADDRESS)) is False
    assert "Tether check failed" in caplog.text
    assert "RuntimeError" in caplog.text
    assert ADDRESS in caplog.text
    assert secret not in caplog.text


def test_tether_cancellation_is_not_swallowed_or_logged_as_a_failure(caplog):
    client = SimpleNamespace(call_is_blacklisted=AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(is_banned_on_tether(client, ADDRESS))
    assert "Tether check failed" not in caplog.text
