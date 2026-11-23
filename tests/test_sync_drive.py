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


@pytest.mark.parametrize("remote,path,expected", [
    ("drive", "", "drive:"),
    ("drive:", "/", "drive:"),
    (" drive: ", "/pipeline/data/", "drive:pipeline/data"),
    ("my drive", "folder with spaces/", "my drive:folder with spaces"),
])
def test_build_dest_normalizes_root_and_trailing_slashes(remote, path, expected):
    assert drive.build_dest(remote, path) == expected


@pytest.mark.parametrize("remote", ["", " ", ":", "drive::", "a/b", "a\\b", "-flag", "a\nb"])
def test_build_dest_rejects_invalid_remote_names(remote):
    with pytest.raises(ValueError, match="remote"):
        drive.build_dest(remote, "folder")


def test_build_dest_rejects_null_path():
    with pytest.raises(ValueError, match="null"):
        drive.build_dest("drive", "bad\x00path")
