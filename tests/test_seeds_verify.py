import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import httpx

from pipeline.api.client import TronGridClient
from pipeline.api.keypool import MultiKeyPool
from pipeline.seeds.verify import is_banned_on_tether, is_flagged_on_tronscan
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


@pytest.fixture
def make_scan_client(monkeypatch):
    clients = []
    original = httpx.AsyncClient
    monkeypatch.setenv("TRONSCAN_API_KEY", "scan-credential")

    def make(handler):
        transport = httpx.MockTransport(handler)
        monkeypatch.setattr(
            httpx, "AsyncClient", lambda **kwargs: original(transport=transport, **kwargs)
        )
        client = TronGridClient(
            MultiKeyPool(["grid-credential"], 60), "https://example.test", 5
        )
        clients.append(client)
        return client

    yield make
    for client in clients:
        asyncio.run(client.aclose())


@pytest.mark.parametrize("payload", [
    {"redTag": "Scam"}, {"redTag": "other risk"}, {"feedbackRisk": True},
    {"addressTag": "PHISHING"}, {"publicTag": "Hacker wallet"},
    {"greyTag": "SCAM_ACCOUNT"}, {"publicTag": "Known scammers"},
])
def test_tronscan_flags_documented_risk_fields_and_markers(make_scan_client, payload):
    client = make_scan_client(lambda _: httpx.Response(200, json=payload))
    assert asyncio.run(is_flagged_on_tronscan(client, ADDRESS)) is True


@pytest.mark.parametrize("payload", [
    {}, {"redTag": "", "feedbackRisk": False, "publicTag": "Exchange"},
    {"feedbackRisk": "false", "redTag": None},
    {"publicTag": "Hackathon"}, {"name": "Scam", "publicTag": "Verified service"},
    {"addressTag": None, "greyTag": [], "publicTag": {}},
])
def test_tronscan_does_not_flag_clean_or_unrelated_metadata(make_scan_client, payload):
    client = make_scan_client(lambda _: httpx.Response(200, json=payload))
    assert asyncio.run(is_flagged_on_tronscan(client, ADDRESS)) is False


@pytest.mark.parametrize("key", [None, "", "   "])
def test_missing_tronscan_credentials_make_no_request(monkeypatch, key):
    if key is None:
        monkeypatch.delenv("TRONSCAN_API_KEY", raising=False)
    else:
        monkeypatch.setenv("TRONSCAN_API_KEY", key)
    http = SimpleNamespace(get=AsyncMock())
    assert asyncio.run(is_flagged_on_tronscan(SimpleNamespace(_client=http), ADDRESS)) is False
    http.get.assert_not_awaited()


def test_invalid_tronscan_address_makes_no_request(monkeypatch):
    monkeypatch.setenv("TRONSCAN_API_KEY", "scan-credential")
    http = SimpleNamespace(get=AsyncMock())
    assert asyncio.run(is_flagged_on_tronscan(SimpleNamespace(_client=http), "bad")) is False
    http.get.assert_not_awaited()


def test_tronscan_normalizes_address_and_keeps_service_credentials_separate(make_scan_client):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={})

    client = make_scan_client(handler)

    async def run():
        assert await is_flagged_on_tronscan(client, to_hex(ADDRESS)) is False
        await client.get_account(ADDRESS)

    asyncio.run(run())
    scan, grid = requests
    assert str(scan.url) == f"https://apilist.tronscanapi.com/api/accountv2?address={ADDRESS}"
    assert scan.headers["TRON-PRO-API-KEY"] == "scan-credential"
    assert grid.url.host == "example.test"
    assert grid.headers["TRON-PRO-API-KEY"] == "grid-credential"


@pytest.mark.parametrize("status", [401, 429, 500])
def test_tronscan_http_errors_fail_open_and_log_safely(make_scan_client, caplog, status):
    client = make_scan_client(lambda _: httpx.Response(status))
    assert asyncio.run(is_flagged_on_tronscan(client, ADDRESS)) is False
    assert "TronScan check failed" in caplog.text
    assert "scan-credential" not in caplog.text
    assert "grid-credential" not in caplog.text


def test_tronscan_network_error_fails_open_without_logging_exception_contents(make_scan_client, caplog):
    def handler(request):
        raise httpx.ConnectError("secret scan-credential", request=request)

    assert asyncio.run(is_flagged_on_tronscan(make_scan_client(handler), ADDRESS)) is False
    assert "ConnectError" in caplog.text
    assert "scan-credential" not in caplog.text


@pytest.mark.parametrize("body", [b"not-json", b"[]"])
def test_tronscan_malformed_response_fails_open_and_logs(make_scan_client, caplog, body):
    client = make_scan_client(lambda _: httpx.Response(200, content=body))
    assert asyncio.run(is_flagged_on_tronscan(client, ADDRESS)) is False
    assert "TronScan check failed" in caplog.text


def test_tronscan_redirect_does_not_forward_credentials(make_scan_client):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(302, headers={"Location": "https://other.test/account"})

    client = make_scan_client(handler)
    assert asyncio.run(is_flagged_on_tronscan(client, ADDRESS)) is False
    assert len(requests) == 1


def test_tronscan_cancellation_propagates(make_scan_client):
    async def handler(request):
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(is_flagged_on_tronscan(make_scan_client(handler), ADDRESS))
