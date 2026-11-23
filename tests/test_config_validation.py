from dataclasses import replace
import logging
from unittest.mock import Mock

import pytest

from pipeline.config import Settings, validate_config


@pytest.fixture
def valid(tmp_path):
    return Settings(api_keys=("first", "second"), csv_dir=tmp_path / "dataset", drive_remote="drive")


def test_valid_config_probes_directory_without_leaving_files(valid, caplog):
    with caplog.at_level(logging.INFO):
        assert validate_config(valid) == []
    assert valid.csv_dir.is_dir()
    assert list(valid.csv_dir.iterdir()) == []
    assert "29 requests/s across 2 unique API keys" in caplog.text
    assert "first" not in caplog.text


def test_duplicate_keys_are_rejected_and_counted_once(valid, caplog):
    settings = replace(valid, api_keys=("secret", "secret"))
    with caplog.at_level(logging.INFO):
        errors = validate_config(settings)
    assert any("duplicate" in error for error in errors)
    assert "14.5 requests/s across 1 unique API keys" in caplog.text
    assert "secret" not in caplog.text + str(errors)
    assert settings.api_keys == ("secret", "secret")


@pytest.mark.parametrize("keys", [(), ("",), (" ",), (" padded",), (None,)])
def test_missing_or_malformed_keys(valid, keys):
    assert any("TRONGRID_API_KEYS" in error for error in validate_config(replace(valid, api_keys=keys)))


@pytest.mark.parametrize("rate", [0, -1, float("nan"), float("inf"), True, "fast"])
def test_invalid_rate(valid, rate):
    assert any("RATE_LIMIT" in error for error in validate_config(replace(valid, rate_limit_per_key=rate)))


@pytest.mark.parametrize("remote", ["", "drive::", "a/b", "-flag", "bad\x00name"])
def test_invalid_remote_with_enabled_sync(valid, remote):
    assert any("DRIVE_REMOTE" in error for error in validate_config(replace(valid, drive_remote=remote)))


def test_disabled_sync_needs_no_remote(valid):
    assert validate_config(replace(valid, drive_remote="", sync_every=0)) == []


@pytest.mark.parametrize("field,value", [("sync_every", -1), ("max_concurrency", 0), ("batch_size", True)])
def test_invalid_integer_settings(valid, field, value):
    assert validate_config(replace(valid, **{field: value}))


def test_csv_directory_cannot_be_a_file(valid):
    valid.csv_dir.write_text("existing user content", encoding="utf-8")
    assert "CSV_DIR must be a writable directory" in validate_config(valid)
    assert valid.csv_dir.read_text() == "existing user content"


def test_unwritable_csv_directory_is_reported(valid, monkeypatch):
    monkeypatch.setattr("pipeline.config.tempfile.TemporaryFile", Mock(side_effect=PermissionError("denied")))
    assert "CSV_DIR must be a writable directory" in validate_config(valid)
