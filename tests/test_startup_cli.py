from argparse import Namespace
from contextlib import asynccontextmanager
from dataclasses import replace
import os
import subprocess
import sys
from unittest.mock import AsyncMock, Mock

import pytest

from pipeline import cli
from pipeline.config import Settings


@pytest.fixture
def startup(tmp_path, monkeypatch):
    settings = Settings(csv_dir=tmp_path / "data", api_keys=("test-key",), sync_every=0)
    client = Mock(_request=AsyncMock(return_value={"blockID": "current-block"}), aclose=AsyncMock())

    @asynccontextmanager
    async def open_client(settings):
        try:
            yield client
        finally:
            await client.aclose()

    monkeypatch.setattr(cli, "_client", open_client)
    runner = Mock(run_once=AsyncMock(), run_forever=AsyncMock())
    monkeypatch.setattr(cli, "Runner", Mock(return_value=runner))
    monkeypatch.setattr(cli, "fetch_licit_seeds", AsyncMock(return_value=[]))
    monkeypatch.setattr(cli, "check_dependencies", Mock())
    return settings, client, runner


@pytest.mark.parametrize("command", ["run", "fetch-seeds"])
@pytest.mark.parametrize("keys", [(), ("duplicate", "duplicate")])
def test_invalid_config_is_refused_before_storage_or_network(startup, monkeypatch, command, keys):
    settings, client, runner = startup
    monkeypatch.setattr(cli, "_settings", lambda args: replace(settings, api_keys=keys))
    storage = Mock(side_effect=AssertionError("invalid configuration must not open storage"))
    monkeypatch.setattr(cli, "CSVStore", storage)
    assert cli.main([command]) == 1
    storage.assert_not_called()
    client._request.assert_not_awaited()
    cli.check_dependencies.assert_not_called()


@pytest.mark.parametrize("command", ["run", "fetch-seeds"])
def test_api_failure_stops_work_and_closes_client(startup, command):
    settings, client, runner = startup
    client._request.side_effect = RuntimeError("unreachable")
    handler = cli.cmd_run if command == "run" else cli.cmd_fetch_seeds
    with pytest.raises(RuntimeError, match="startup probe failed after 3 attempts"):
        handler(Namespace(settings=settings, step=True))
    assert client._request.await_count == 3
    client.aclose.assert_awaited_once()
    runner.run_once.assert_not_awaited()
    runner.run_forever.assert_not_awaited()
    cli.fetch_licit_seeds.assert_not_awaited()


@pytest.mark.parametrize("step,sync_every,expect_drive", [(True, 10, False), (False, 0, False), (False, 10, True)])
def test_run_probes_api_and_only_checks_drive_for_long_syncing_runs(startup, step, sync_every, expect_drive):
    settings, client, runner = startup
    settings = replace(settings, sync_every=sync_every, drive_remote="archive" if expect_drive else "")
    assert cli.cmd_run(Namespace(settings=settings, step=step)) == 0
    client._request.assert_awaited_once_with("/wallet/getnowblock", method="POST", json={})
    client.aclose.assert_awaited_once()
    if expect_drive:
        cli.check_dependencies.assert_called_once_with(settings)
    else:
        cli.check_dependencies.assert_not_called()


def test_bad_drive_aborts_before_opening_dataset_or_client(startup):
    settings, client, runner = startup
    cli.check_dependencies.side_effect = RuntimeError("Drive unreachable")
    settings = replace(settings, sync_every=10, drive_remote="archive")
    with pytest.raises(RuntimeError, match="Drive unreachable"):
        cli.cmd_run(Namespace(settings=settings, step=False))
    client._request.assert_not_awaited()
    runner.run_forever.assert_not_awaited()
    assert not settings.csv_dir.exists() or not list(settings.csv_dir.glob("*.csv"))


def test_fetch_checks_api_without_drive_and_inserts_seeds(startup):
    settings, client, runner = startup
    assert cli.cmd_fetch_seeds(Namespace(settings=replace(settings, sync_every=10))) == 0
    client._request.assert_awaited_once()
    cli.fetch_licit_seeds.assert_awaited_once_with(client, settings.seed_count)
    cli.check_dependencies.assert_not_called()
    client.aclose.assert_awaited_once()


def test_module_refuses_missing_keys_with_nonzero_status(tmp_path):
    environment = dict(os.environ, TRONGRID_API_KEYS="", SYNC_EVERY="0", PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run(
        [sys.executable, "-B", "-m", "pipeline.cli", "run", "--step", "--csv-dir", str(tmp_path / "data")],
        env=environment, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 1
    assert "TRONGRID_API_KEYS requires at least one API key" in result.stderr
    assert not list(tmp_path.rglob("*.csv"))
