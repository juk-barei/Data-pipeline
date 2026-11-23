import logging
import os

from pipeline.config import load_env, load_settings, setup_logging
import pytest


def test_load_env_missing_file_is_safe(tmp_path):
    load_env(tmp_path / "missing.env")


def test_load_env_reads_values(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("PHASE_ZERO_TEST=loaded\n", encoding="utf-8")
    monkeypatch.delenv("PHASE_ZERO_TEST", raising=False)

    load_env(env_file)

    assert os.getenv("PHASE_ZERO_TEST") == "loaded"


def test_load_settings_defaults_and_safe_parsing(monkeypatch):
    monkeypatch.setenv("TRONGRID_API_KEYS", "one, two, ,three")
    monkeypatch.setenv("RATE_LIMIT_PER_KEY", "invalid")
    monkeypatch.setenv("MAX_CONCURRENCY", "0")

    settings = load_settings()

    assert settings.api_keys == ("one", "two", "three")
    assert settings.rate_limit_per_key == 14.5
    assert settings.max_concurrency == 20


def test_setup_logging_is_idempotent():
    root = logging.getLogger()
    before = sum(bool(getattr(h, "_tron_pipeline", False)) for h in root.handlers)

    setup_logging()
    setup_logging()

    after = sum(bool(getattr(h, "_tron_pipeline", False)) for h in root.handlers)
    assert after == max(1, before)


@pytest.mark.parametrize("value,expected", [("0", 0), ("3", 3), ("-1", 10), ("invalid", 10)])
def test_sync_every_can_disable_auto_sync(monkeypatch, value, expected):
    monkeypatch.setenv("SYNC_EVERY", value)
    assert load_settings().sync_every == expected
