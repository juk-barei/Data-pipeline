from argparse import Namespace
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from pipeline import cli
from pipeline.config import Settings
from pipeline.storage.csv_store import CSVStore


COMMANDS = ("fetch-seeds", "run", "monitor", "reset", "compact", "sync")


def test_help_lists_all_commands_without_loading_settings(capsys):
    with pytest.raises(SystemExit) as result:
        cli.build_parser().parse_args(["--help"])
    assert result.value.code == 0
    output = capsys.readouterr().out
    assert all(command in output for command in COMMANDS)


@pytest.mark.parametrize("command", COMMANDS)
def test_parser_accepts_command_paths(command):
    args = cli.build_parser().parse_args([command, "--csv-dir", "dataset", "--env-file", "custom.env"])
    assert args.command == command
    assert args.csv_dir == Path("dataset")
    assert args.env_file == Path("custom.env")


def test_parser_flags():
    parser = cli.build_parser()
    assert parser.parse_args(["run", "--step"]).step
    assert parser.parse_args(["reset", "--yes"]).yes
    args = parser.parse_args(["sync", "--compress", "--dry-run", "--delete-local"])
    assert args.compress and args.dry_run and args.delete_local


@pytest.mark.parametrize("arguments", [[], ["unknown"], ["monitor", "--step"]])
def test_parser_rejects_missing_unknown_or_inapplicable_commands(arguments, capsys):
    with pytest.raises(SystemExit) as result:
        cli.build_parser().parse_args(arguments)
    assert result.value.code == 2
    assert "usage:" in capsys.readouterr().err


ADDRESS = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"


@pytest.fixture
def fake_clients(monkeypatch):
    clients = []

    class Client:
        def __init__(self, pool, base_url, timeout):
            self.pool = pool
            self.base_url = base_url
            self.timeout = timeout
            self.aclose = AsyncMock()
            clients.append(self)

    monkeypatch.setattr(cli, "TronGridClient", Client)
    return clients


def test_fetch_seeds_rerun_is_idempotent(tmp_path, monkeypatch, fake_clients):
    settings = Settings(csv_dir=tmp_path, seed_count=1, api_keys=("first", "second"))
    fetch = AsyncMock(return_value=[{"wallet_address": ADDRESS, "created_at": "2026-01-01"}])
    monkeypatch.setattr(cli, "fetch_licit_seeds", fetch)
    args = Namespace(settings=settings)
    assert cli.cmd_fetch_seeds(args) == 0
    before = {path.name: path.read_bytes() for path in tmp_path.glob("*.csv")}
    assert cli.cmd_fetch_seeds(args) == 0
    assert before == {path.name: path.read_bytes() for path in tmp_path.glob("*.csv")}
    store = CSVStore(str(tmp_path))
    assert list(store.seeds) == [ADDRESS]
    assert store.crawl_state[ADDRESS]["hop_level"] == 0
    assert len(fake_clients) == 2
    for client in fake_clients:
        assert client.pool.keys == ["first", "second"]
        client.aclose.assert_awaited_once()
        assert client.base_url == "https://api.trongrid.io"
    assert [call.args[1] for call in fetch.await_args_list] == [1, 1]


def test_fetch_failure_closes_client_and_leaves_csvs_intact(tmp_path, monkeypatch, fake_clients):
    store = CSVStore(str(tmp_path))
    before = {name: path.read_bytes() for name, path in store.paths.items()}
    monkeypatch.setattr(cli, "fetch_licit_seeds", AsyncMock(side_effect=RuntimeError("failed")))
    with pytest.raises(RuntimeError, match="failed"):
        cli.cmd_fetch_seeds(Namespace(settings=Settings(csv_dir=tmp_path)))
    fake_clients[0].aclose.assert_awaited_once()
    assert before == {name: path.read_bytes() for name, path in store.paths.items()}


def test_command_loads_env_and_applies_directory_override(tmp_path, monkeypatch, fake_clients):
    env = tmp_path / "custom.env"
    env.write_text("SEED_COUNT=1\nCSV_DIR=unused\n", encoding="utf-8")
    # Record both keys so monkeypatch also undoes dotenv modifications.
    for key in ("SEED_COUNT", "CSV_DIR"):
        monkeypatch.setenv(key, "temporary")
        monkeypatch.delenv(key)
    fetch = AsyncMock(return_value=[])
    monkeypatch.setattr(cli, "fetch_licit_seeds", fetch)
    output = tmp_path / "override"
    args = cli.build_parser().parse_args(["fetch-seeds", "--env-file", str(env), "--csv-dir", str(output)])
    assert cli.cmd_fetch_seeds(args) == 0
    assert (output / "seed_wallets.csv").is_file()
    assert fetch.await_args.args[1] == 1
