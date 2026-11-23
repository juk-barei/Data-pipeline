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


@pytest.mark.parametrize("step", [True, False])
def test_run_dispatches_to_requested_mode(tmp_path, monkeypatch, fake_clients, step):
    from unittest.mock import Mock

    runner = Mock(run_once=AsyncMock(), run_forever=AsyncMock())
    factory = Mock(return_value=runner)
    monkeypatch.setattr(cli, "Runner", factory)
    settings = Settings(csv_dir=tmp_path, sync_every=0)
    assert cli.cmd_run(Namespace(settings=settings, step=step)) == 0
    (runner.run_once if step else runner.run_forever).assert_awaited_once()
    (runner.run_forever if step else runner.run_once).assert_not_awaited()
    store, client, actual_settings = factory.call_args.args
    assert store.csv_dir == tmp_path
    assert client is fake_clients[0]
    assert actual_settings is settings
    client.aclose.assert_awaited_once()


def test_run_real_idle_step_requires_no_network(tmp_path, fake_clients):
    assert cli.cmd_run(Namespace(settings=Settings(csv_dir=tmp_path), step=True)) == 0
    fake_clients[0].aclose.assert_awaited_once()
    assert len(list(tmp_path.glob("*.csv"))) == 5


@pytest.mark.parametrize("error", [KeyboardInterrupt(), RuntimeError("runner failed")])
def test_run_failure_closes_client_and_preserves_csvs(tmp_path, monkeypatch, fake_clients, error):
    from unittest.mock import Mock

    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    before = {name: path.read_bytes() for name, path in store.paths.items()}
    runner = Mock(run_forever=AsyncMock(side_effect=error))
    monkeypatch.setattr(cli, "Runner", Mock(return_value=runner))
    args = Namespace(settings=Settings(csv_dir=tmp_path), step=False)
    if isinstance(error, KeyboardInterrupt):
        assert cli.cmd_run(args) == 130
    else:
        with pytest.raises(RuntimeError, match="runner failed"):
            cli.cmd_run(args)
    fake_clients[0].aclose.assert_awaited_once()
    assert before == {name: path.read_bytes() for name, path in store.paths.items()}
    assert ADDRESS in CSVStore(str(tmp_path)).seeds


def test_monitor_reads_existing_dataset_without_network(tmp_path, monkeypatch, capsys):
    from unittest.mock import Mock

    store = CSVStore(str(tmp_path))
    store.insert_seeds([{"wallet_address": ADDRESS}])
    before = {name: path.read_bytes() for name, path in store.paths.items()}
    network = Mock(side_effect=AssertionError("monitor must stay offline"))
    monkeypatch.setattr(cli, "TronGridClient", network)
    monkeypatch.setattr(cli, "MultiKeyPool", network)
    assert cli.cmd_monitor(Namespace(settings=Settings(csv_dir=tmp_path, api_keys=()))) == 0
    output = capsys.readouterr().out
    assert "Pipeline health" in output
    assert "Hop 0: 1 wallets" in output
    assert "Hop 5: 0 wallets" in output
    assert "Pending crawls: 1" in output
    assert "Complete: no" in output
    network.assert_not_called()
    assert before == {name: path.read_bytes() for name, path in store.paths.items()}
