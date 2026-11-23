import subprocess
from unittest.mock import Mock

import pytest

from pipeline.config import Settings
from pipeline.sync import drive


@pytest.fixture
def startup(monkeypatch):
    monkeypatch.setattr(drive, "load_env", Mock())
    monkeypatch.setattr(drive, "load_settings", lambda: Settings(drive_remote="archive", drive_path="dataset"))
    monkeypatch.setattr(drive, "ensure_rclone", lambda: "/usr/bin/rclone")
    run = Mock(return_value=subprocess.CompletedProcess([], 0, "", ""))
    monkeypatch.setattr(drive.subprocess, "run", run)
    return run


def test_dependency_check_is_read_only_and_bounded(startup):
    assert drive.check_dependencies() is None
    command = startup.call_args.args[0]
    assert command[:2] == ["/usr/bin/rclone", "lsf"]
    assert command[-2:] == ["--", "archive:dataset"]
    assert command[command.index("--retries") + 1] == "1"
    assert startup.call_args.kwargs["timeout"] == 30
    assert startup.call_args.kwargs["stdout"] == subprocess.DEVNULL
    assert startup.call_args.kwargs["stderr"] == subprocess.DEVNULL
    assert "text" not in startup.call_args.kwargs
    drive.load_env.assert_called_once_with()


def test_unreachable_remote_has_actionable_error_without_stderr(startup):
    startup.return_value = subprocess.CompletedProcess([], 1, "", "secret-token")
    with pytest.raises(RuntimeError, match="rclone config") as caught:
        drive.check_dependencies()
    assert "secret-token" not in str(caught.value)


@pytest.mark.parametrize("error", [OSError("cannot execute"), subprocess.TimeoutExpired("rclone", 30)])
def test_dependency_execution_failure_is_actionable(startup, error):
    startup.side_effect = error
    with pytest.raises(RuntimeError, match="network access"):
        drive.check_dependencies()


def test_missing_remote_fails_before_subprocess(startup, monkeypatch):
    monkeypatch.setattr(drive, "load_settings", lambda: Settings())
    with pytest.raises(ValueError, match="remote"):
        drive.check_dependencies()
    startup.assert_not_called()


def test_missing_binary_keeps_install_hint(startup, monkeypatch):
    monkeypatch.setattr(drive, "ensure_rclone", Mock(side_effect=FileNotFoundError("install rclone")))
    with pytest.raises(FileNotFoundError, match="install"):
        drive.check_dependencies()
    startup.assert_not_called()


def test_dependency_check_uses_resolved_settings_instead_of_environment(startup):
    drive.check_dependencies(Settings(drive_remote="override", drive_path="actual/path"))
    assert startup.call_args.args[0][-1] == "override:actual/path"
    drive.load_env.assert_not_called()
