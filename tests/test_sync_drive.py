import pytest

from pipeline.sync import drive


def test_ensure_rclone_returns_resolved_executable_without_running_it(monkeypatch):
    requested = []

    def locate(name):
        requested.append(name)
        return "C:/tools/rclone.exe"

    monkeypatch.setattr(drive.shutil, "which", locate)
    assert drive.ensure_rclone() == "C:/tools/rclone.exe"
    assert requested == ["rclone"]


def test_ensure_rclone_missing_has_install_hint(monkeypatch):
    monkeypatch.setattr(drive.shutil, "which", lambda name: None)
    with pytest.raises(FileNotFoundError, match="rclone.org/install/.*rclone config"):
        drive.ensure_rclone()
