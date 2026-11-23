from pathlib import Path

import pytest

from pipeline import cli


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
