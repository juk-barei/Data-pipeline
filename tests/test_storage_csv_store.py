import csv

import pytest

from pipeline.storage.csv_store import CSVStore


def bare_store():
    return CSVStore.__new__(CSVStore)


def test_init_file_creates_parent_and_header(tmp_path):
    path = tmp_path / "nested" / "accounts.csv"

    bare_store()._init_file(path, ["address", "created_at"])

    with path.open(encoding="utf-8", newline="") as handle:
        assert list(csv.reader(handle)) == [["address", "created_at"]]


def test_init_file_populates_existing_empty_file(tmp_path):
    path = tmp_path / "accounts.csv"
    path.touch()

    bare_store()._init_file(path, ["address"])

    assert path.read_text(encoding="utf-8").strip() == "address"


def test_init_file_never_changes_nonempty_file(tmp_path):
    path = tmp_path / "accounts.csv"
    original = b"old,header\r\nkeep,this\r\n"
    path.write_bytes(original)

    bare_store()._init_file(path, ["new", "header"])

    assert path.read_bytes() == original


def test_row_to_list_uses_header_order_and_fills_missing_cells():
    row = {"created_at": "2026-01-01", "address": "T123", "ignored": "value"}

    result = bare_store()._row_to_list(row, ["address", "hex_address", "created_at"])

    assert result == ["T123", "", "2026-01-01"]


def test_append_rows_preserves_existing_rows_and_updates_counter(tmp_path, monkeypatch):
    path = tmp_path / "accounts.csv"
    path.write_text("address,created_at\nT1,old\n", encoding="utf-8")
    store = bare_store()
    store._appended_since_compact = 4
    fsync_calls = []
    monkeypatch.setattr("pipeline.storage.csv_store.os.fsync", fsync_calls.append)

    store._append_rows(
        path,
        ["address", "created_at"],
        ({"created_at": "new", "address": "T2"},),
    )

    with path.open(encoding="utf-8", newline="") as handle:
        assert list(csv.reader(handle)) == [
            ["address", "created_at"],
            ["T1", "old"],
            ["T2", "new"],
        ]
    assert store._appended_since_compact == 5
    assert len(fsync_calls) == 1


def test_append_rows_writes_header_for_new_file_and_empty_batch_is_noop(tmp_path):
    path = tmp_path / "transactions.csv"
    store = bare_store()

    store._append_rows(path, ["tx_hash", "amount"], [{"tx_hash": "abc"}])
    before = path.read_bytes()
    store._append_rows(path, ["tx_hash", "amount"], [])

    with path.open(encoding="utf-8", newline="") as handle:
        assert list(csv.reader(handle)) == [["tx_hash", "amount"], ["abc", ""]]
    assert path.read_bytes() == before
    assert store._appended_since_compact == 1


def test_write_snapshot_replaces_file_with_complete_csv(tmp_path, monkeypatch):
    path = tmp_path / "accounts.csv"
    path.write_text("old,data\n", encoding="utf-8")
    fsync_calls = []
    monkeypatch.setattr("pipeline.storage.csv_store.os.fsync", fsync_calls.append)

    bare_store()._write_snapshot(
        path,
        ["address", "created_at"],
        [{"address": "T1", "created_at": "now"}],
    )

    with path.open(encoding="utf-8", newline="") as handle:
        assert list(csv.reader(handle)) == [
            ["address", "created_at"],
            ["T1", "now"],
        ]
    assert len(fsync_calls) == 1
    assert not (tmp_path / "accounts.csv.tmp").exists()


def test_write_snapshot_replace_failure_preserves_old_file(tmp_path, monkeypatch):
    path = tmp_path / "accounts.csv"
    original = b"address,created_at\r\nT-old,before\r\n"
    path.write_bytes(original)

    def fail_replace(source, destination):
        assert source == tmp_path / "accounts.csv.tmp"
        assert destination == path
        raise OSError("synthetic replace failure")

    monkeypatch.setattr("pipeline.storage.csv_store.os.replace", fail_replace)

    with pytest.raises(OSError, match="synthetic replace failure"):
        bare_store()._write_snapshot(path, ["address", "created_at"], [{"address": "T-new"}])

    assert path.read_bytes() == original
    with (tmp_path / "accounts.csv.tmp").open(encoding="utf-8", newline="") as handle:
        assert list(csv.reader(handle)) == [
            ["address", "created_at"],
            ["T-new", ""],
        ]


def test_iter_rows_missing_file_is_empty(tmp_path):
    assert list(bare_store()._iter_rows(tmp_path / "missing.csv")) == []


def test_iter_rows_streams_multiline_and_complete_record_without_final_newline(tmp_path):
    path = tmp_path / "accounts.csv"
    path.write_bytes(
        b'address,created_at,source\r\nT1,now,"line one\r\nline two"\r\nT2,later,complete'
    )
    store = bare_store()

    rows = list(store._iter_rows(path))

    assert rows == [
        {"address": "T1", "created_at": "now", "source": "line one\r\nline two"},
        {"address": "T2", "created_at": "later", "source": "complete"},
    ]
    assert store._current_row_offset == path.read_bytes().index(b"T2,later")


@pytest.mark.parametrize("tail", [b'T2,later,"unterminated', b"T2,later"])
def test_iter_rows_skips_and_removes_torn_final_record_before_append(tmp_path, tail):
    path = tmp_path / "accounts.csv"
    complete = b"address,created_at,source\r\nT1,now,ok\r\n"
    path.write_bytes(complete + tail)
    store = bare_store()

    assert list(store._iter_rows(path)) == [
        {"address": "T1", "created_at": "now", "source": "ok"}
    ]
    assert path.read_bytes() == complete

    store._append_rows(
        path,
        ["address", "created_at", "source"],
        [{"address": "T3", "created_at": "after", "source": "repair"}],
    )
    assert list(store._iter_rows(path))[-1]["address"] == "T3"


def test_iter_rows_does_not_hide_complete_malformed_record(tmp_path):
    path = tmp_path / "accounts.csv"
    path.write_bytes(b"address,created_at\r\nT1\r\n")

    with pytest.raises(csv.Error, match="expected 2"):
        list(bare_store()._iter_rows(path))


def test_append_after_complete_record_without_newline_keeps_both_records(tmp_path):
    path = tmp_path / "accounts.csv"
    path.write_bytes(b"address,created_at\r\nT1,before")
    store = bare_store()
    assert [row["address"] for row in store._iter_rows(path)] == ["T1"]

    store._append_rows(
        path,
        ["address", "created_at"],
        [{"address": "T2", "created_at": "after"}],
    )

    assert [row["address"] for row in store._iter_rows(path)] == ["T1", "T2"]


def test_iter_rows_preserves_and_rejects_extra_column_at_eof(tmp_path):
    path = tmp_path / "accounts.csv"
    malformed = b"address,created_at\r\nT1,now,extra"
    path.write_bytes(malformed)

    with pytest.raises(csv.Error, match="expected 2"):
        list(bare_store()._iter_rows(path))

    assert path.read_bytes() == malformed
