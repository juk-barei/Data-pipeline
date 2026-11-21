import csv

import pytest

from pipeline.storage.csv_store import CSVStore
from pipeline.storage.schema import build_headers


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


def test_load_seeds_is_last_row_wins_then_filters_latest_label(tmp_path):
    path = tmp_path / "seed_wallets.csv"
    header = ["wallet_address", "hex_address", "transaction_id", "source_name", "label", "created_at"]
    store = bare_store()
    store.seed_wallets_path = path
    store._write_snapshot(
        path,
        header,
        [
            {"wallet_address": "T1", "label": "0", "source_name": "old"},
            {"wallet_address": "T2", "label": "1"},
            {"wallet_address": "T1", "label": "1", "source_name": "latest"},
            {"wallet_address": "T3", "label": "0"},
        ],
    )

    store._load_seeds()

    assert set(store.seeds) == {"T3"}


def test_load_seeds_can_retain_all_latest_rows(tmp_path):
    path = tmp_path / "seed_wallets.csv"
    store = bare_store()
    store.seed_wallets_path = path
    store.retain_all_seeds = True
    store._write_snapshot(
        path,
        ["wallet_address", "label"],
        [
            {"wallet_address": "T1", "label": "0"},
            {"wallet_address": "T1", "label": "1"},
            {"wallet_address": "T2", "label": "0"},
        ],
    )

    store._load_seeds()

    assert store.seeds["T1"]["label"] == "1"
    assert set(store.seeds) == {"T1", "T2"}


def test_load_accounts_keeps_latest_row_for_each_nonempty_address(tmp_path):
    path = tmp_path / "accounts.csv"
    store = bare_store()
    store.accounts_path = path
    store._write_snapshot(
        path,
        ["address", "last_seen_timestamp", "created_at"],
        [
            {"address": "T1", "last_seen_timestamp": "old"},
            {"address": "T2", "last_seen_timestamp": "only"},
            {"address": "", "last_seen_timestamp": "invalid"},
            {"address": "T1", "last_seen_timestamp": "latest"},
        ],
    )

    store._load_accounts()

    assert set(store.accounts) == {"T1", "T2"}
    assert store.accounts["T1"]["last_seen_timestamp"] == "latest"


def test_load_transactions_builds_compact_first_wins_indexes(tmp_path):
    path = tmp_path / "transactions.csv"
    store = bare_store()
    store.transactions_path = path
    store._write_snapshot(
        path,
        ["tx_hash", "from_address", "to_address", "amount"],
        [
            {"tx_hash": "h1", "from_address": "T1", "to_address": "T2", "amount": "1"},
            {"tx_hash": "h2", "from_address": "T2", "to_address": "T2", "amount": "2"},
            {"tx_hash": "h1", "from_address": "BAD", "to_address": "T3", "amount": "changed"},
            {"tx_hash": "", "from_address": "T4", "to_address": "T5"},
        ],
    )

    store._load_transactions()

    assert store.tx_hashes == {"h1", "h2"}
    assert store.wallet_tx_hashes == {"T1": {"h1"}, "T2": {"h1", "h2"}}
    assert set(store.tx_offsets) == {"h1", "h2"}
    raw = path.read_bytes()
    assert raw[store.tx_offsets["h1"] :].startswith(b"h1,T1,T2,1")
    assert raw[store.tx_offsets["h2"] :].startswith(b"h2,T2,T2,2")
    assert not hasattr(store, "transactions")


def test_load_hops_deduplicates_edges_and_tracks_all_observed_ids(tmp_path):
    path = tmp_path / "wallet_hops.csv"
    store = bare_store()
    store.wallet_hops_path = path
    store._write_snapshot(
        path,
        ["hop_id", "wallet_address", "seed_address", "hop_level", "direction"],
        [
            {"hop_id": "2", "wallet_address": "T1", "seed_address": "S1", "hop_level": "1", "direction": "out"},
            {"hop_id": "99", "wallet_address": "T1", "seed_address": "S1", "hop_level": "1", "direction": "duplicate"},
            {"hop_id": "3", "wallet_address": "T1", "seed_address": "S2", "hop_level": "1", "direction": "out"},
            {"hop_id": "4", "wallet_address": "T1", "seed_address": "S1", "hop_level": "2", "direction": "out"},
        ],
    )

    store._load_hops()

    assert len(store.hops) == 3
    assert store.hop_keys == {("T1", "S1", 1), ("T1", "S2", 1), ("T1", "S1", 2)}
    assert store.max_hop_id == 99
    assert store.hops[0]["direction"] == "out"


def test_load_hops_rejects_invalid_numeric_state(tmp_path):
    path = tmp_path / "wallet_hops.csv"
    store = bare_store()
    store.wallet_hops_path = path
    store._write_snapshot(
        path,
        ["hop_id", "wallet_address", "seed_address", "hop_level"],
        [{"hop_id": "bad", "wallet_address": "T1", "seed_address": "S1", "hop_level": "0"}],
    )

    with pytest.raises(ValueError, match="must be integers"):
        store._load_hops()


def test_load_crawl_state_is_last_wins_with_typed_values(tmp_path):
    path = tmp_path / "pipeline_crawl_state.csv"
    store = bare_store()
    store.crawl_state_path = path
    header = ["address", "seed_address", "hop_level", "synced", "expanded", "is_supernode", "updated_at"]
    store._write_snapshot(
        path,
        header,
        [
            {"address": "T1", "hop_level": "1", "synced": "FALSE", "expanded": "FALSE", "is_supernode": "FALSE", "updated_at": "old"},
            {"address": "T2", "hop_level": "0", "synced": "true", "expanded": "false", "is_supernode": "false"},
            {"address": "T1", "hop_level": "2", "synced": "TRUE", "expanded": "FALSE", "is_supernode": "TRUE", "updated_at": "latest"},
        ],
    )

    store._load_crawl_state()

    assert store.crawl_state["T1"] == {
        "address": "T1", "seed_address": "", "hop_level": 2,
        "synced": True, "expanded": False, "is_supernode": True, "updated_at": "latest",
    }
    assert store.crawl_state["T2"]["synced"] is True


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [("synced", "yes", "synced"), ("hop_level", "1.5", "hop_level")],
)
def test_load_crawl_state_rejects_ambiguous_values(tmp_path, field, value, message):
    path = tmp_path / "pipeline_crawl_state.csv"
    store = bare_store()
    store.crawl_state_path = path
    row = {"address": "T1", "hop_level": "0", "synced": "FALSE", "expanded": "FALSE", "is_supernode": "FALSE"}
    row[field] = value
    store._write_snapshot(path, list(row), [row])

    with pytest.raises(ValueError, match=message):
        store._load_crawl_state()


def test_load_orchestrates_dependency_order_and_logs_counts(monkeypatch, caplog):
    store = bare_store()
    calls = []

    def loader(name, attribute, value):
        def run():
            calls.append(name)
            setattr(store, attribute, value)
        return run

    monkeypatch.setattr(store, "_load_seeds", loader("seeds", "seeds", {"S": {}}))
    monkeypatch.setattr(store, "_load_accounts", loader("accounts", "accounts", {"A": {}}))
    monkeypatch.setattr(store, "_load_transactions", loader("transactions", "tx_hashes", {"H"}))
    monkeypatch.setattr(store, "_load_hops", loader("hops", "hops", [{}]))
    monkeypatch.setattr(store, "_load_crawl_state", loader("crawl_state", "crawl_state", {"A": {}}))
    caplog.set_level("INFO", logger="pipeline.storage.csv_store")

    store.load()

    assert calls == ["seeds", "accounts", "transactions", "hops", "crawl_state"]
    assert "seeds=1 accounts=1 transactions=1 hops=1 crawl_state=1" in caplog.text


def test_constructor_creates_canonical_files_and_reopen_loads_all_state(tmp_path):
    store = CSVStore(str(tmp_path / "data"))
    assert store.csv_dir.is_absolute()
    assert set(store.paths) == set(build_headers())
    for name, header in build_headers().items():
        with store.paths[name].open(encoding="utf-8", newline="") as handle:
            assert next(csv.reader(handle)) == header

    store._append_rows(store.seed_wallets_path, store.headers["seed_wallets.csv"], [
        {"wallet_address": "S1", "label": "0", "created_at": "now"}
    ])
    store._append_rows(store.accounts_path, store.headers["accounts.csv"], [
        {"address": "T1", "created_at": "now"}
    ])
    store._append_rows(store.transactions_path, store.headers["transactions.csv"], [
        {"tx_hash": "h1", "from_address": "T1", "to_address": "T2"}
    ])
    store._append_rows(store.wallet_hops_path, store.headers["wallet_hops.csv"], [
        {"hop_id": "7", "wallet_address": "T1", "seed_address": "S1", "hop_level": "1"}
    ])
    store._append_rows(store.crawl_state_path, store.headers["pipeline_crawl_state.csv"], [
        {"address": "T1", "seed_address": "S1", "hop_level": "1", "synced": "TRUE", "expanded": "FALSE", "is_supernode": "FALSE"}
    ])

    reopened = CSVStore(str(store.csv_dir))

    assert set(reopened.seeds) == {"S1"}
    assert set(reopened.accounts) == {"T1"}
    assert reopened.tx_hashes == {"h1"}
    assert reopened.max_hop_id == 7
    assert reopened.crawl_state["T1"]["synced"] is True
    reopened.accounts["STALE"] = {}
    reopened.load()
    assert "STALE" not in reopened.accounts
    assert reopened._appended_since_compact == 0


def test_append_account_rows_writes_current_known_rows_once(tmp_path):
    store = CSVStore(str(tmp_path))
    store.accounts = {
        "T1": {"address": "T1", "last_seen_timestamp": "latest", "created_at": "now"},
        "T2": {"address": "T2", "last_seen_timestamp": "only", "created_at": "now"},
    }

    store._append_account_rows(["T1", "UNKNOWN", "T1", "T2"])

    rows = list(store._iter_rows(store.accounts_path))
    assert [row["address"] for row in rows] == ["T1", "T2"]
    assert rows[0]["last_seen_timestamp"] == "latest"
    assert store._appended_since_compact == 2


def test_append_crawl_state_rows_encodes_booleans_without_mutating_memory(tmp_path):
    store = CSVStore(str(tmp_path))
    state = {
        "address": "T1", "seed_address": "S1", "hop_level": 2,
        "synced": True, "expanded": False, "is_supernode": False,
        "created_at": "before", "updated_at": "now",
    }
    store.crawl_state = {"T1": state}

    store._append_crawl_state_rows(["UNKNOWN", "T1", "T1"])

    assert state["synced"] is True
    assert state["expanded"] is False
    rows = list(store._iter_rows(store.crawl_state_path))
    assert len(rows) == 1
    assert rows[0]["hop_level"] == "2"
    assert (rows[0]["synced"], rows[0]["expanded"], rows[0]["is_supernode"]) == (
        "TRUE", "FALSE", "FALSE"
    )


def test_append_crawl_state_rows_rejects_string_booleans(tmp_path):
    store = CSVStore(str(tmp_path))
    store.crawl_state["T1"] = {
        "address": "T1", "hop_level": 0,
        "synced": "FALSE", "expanded": False, "is_supernode": False,
    }

    with pytest.raises(ValueError, match="synced must be bool"):
        store._append_crawl_state_rows(["T1"])

    assert list(store._iter_rows(store.crawl_state_path)) == []


def test_insert_seeds_creates_dependencies_and_is_byte_idempotent(tmp_path):
    store = CSVStore(str(tmp_path))
    address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    seed = {
        "wallet_address": address,
        "transaction_id": "origin-tx",
        "source_name": "fixture",
        "label": 0,
        "created_at": "2026-01-02T03:04:05+00:00",
    }

    store.insert_seeds([seed])
    sizes = {name: path.stat().st_size for name, path in store.paths.items()}
    store.insert_seeds([seed])

    assert {name: path.stat().st_size for name, path in store.paths.items()} == sizes
    assert store.seeds[address]["source_name"] == "fixture"
    assert store.accounts[address]["address"] == address
    assert store.accounts[address]["created_date"] == "2026-01-02"
    assert store.crawl_state[address]["seed_address"] == address
    assert store.crawl_state[address]["hop_level"] == 0
    assert store.crawl_state[address]["synced"] is False


def test_insert_seeds_repairs_missing_dependencies_without_duplicate_seed(tmp_path):
    store = CSVStore(str(tmp_path))
    address = "TUoHaVjx7n5xz8LwPRDckgFrDWhMhuSuJM"
    existing = {
        "wallet_address": address, "hex_address": "", "transaction_id": "",
        "source_name": "existing", "label": "0", "created_at": "2026-01-01T00:00:00+00:00",
    }
    store._append_rows(store.seed_wallets_path, store.headers["seed_wallets.csv"], [existing])
    store.seeds[address] = existing

    store.insert_seeds([existing])

    assert len(list(store._iter_rows(store.seed_wallets_path))) == 1
    assert address in store.accounts
    assert address in store.crawl_state
    assert len(list(store._iter_rows(store.accounts_path))) == 1
    assert len(list(store._iter_rows(store.crawl_state_path))) == 1


def test_insert_seeds_retries_after_dependency_write_failure(tmp_path, monkeypatch):
    store = CSVStore(str(tmp_path))
    address = "TUoHaVjx7n5xz8LwPRDckgFrDWhMhuSuJM"
    seed = {
        "wallet_address": address,
        "label": 0,
        "created_at": "2026-01-01T00:00:00+00:00",
    }
    real_append_state = store._append_crawl_state_rows
    attempts = 0

    def fail_once(addresses):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("synthetic crawl-state write failure")
        real_append_state(addresses)

    monkeypatch.setattr(store, "_append_crawl_state_rows", fail_once)
    with pytest.raises(OSError, match="synthetic crawl-state write failure"):
        store.insert_seeds([seed])

    assert address not in store.accounts
    assert address not in store.crawl_state
    assert address not in store.seeds
    store.insert_seeds([seed])

    reopened = CSVStore(str(tmp_path))
    assert set(reopened.seeds) == {address}
    assert set(reopened.accounts) == {address}
    assert set(reopened.crawl_state) == {address}
    assert reopened.crawl_state[address]["hop_level"] == 0


def test_upsert_crawled_data_deduplicates_transactions_and_updates_indexes(tmp_path):
    store = CSVStore(str(tmp_path))
    address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    store.insert_seeds([{"wallet_address": address, "label": 0}])
    original_hex = store.accounts[address]["hex_address"]
    original_created_at = store.accounts[address]["created_at"]
    huge_amount = "9007199254740993.000000"
    tx = {
        "tx_hash": "h1", "from_address": address, "to_address": "T-destination",
        "amount": huge_amount, "token_symbol": "USDT",
    }

    store.upsert_crawled_data(
        [{"address": address, "last_seen_timestamp": "latest"}],
        [tx, dict(tx)],
        [address],
        [address],
    )
    store.upsert_crawled_data([], [dict(tx)], [address], [address])

    assert list(store._iter_rows(store.transactions_path)) == [
        {**{column: "" for column in store.headers["transactions.csv"]}, **tx}
    ]
    assert store.tx_hashes == {"h1"}
    assert store.wallet_tx_hashes[address] == {"h1"}
    assert store.wallet_tx_hashes["T-destination"] == {"h1"}
    assert store.transactions_path.read_bytes()[store.tx_offsets["h1"] :].startswith(b"h1,")
    assert store.crawl_state[address]["synced"] is True
    assert store.crawl_state[address]["is_supernode"] is True
    assert store.accounts[address]["hex_address"] == original_hex
    assert store.accounts[address]["created_at"] == original_created_at

    reopened = CSVStore(str(tmp_path))
    assert reopened.tx_hashes == {"h1"}
    assert list(reopened._iter_rows(reopened.transactions_path))[0]["amount"] == huge_amount
    assert reopened.accounts[address]["hex_address"] == original_hex
    assert reopened.accounts[address]["created_at"] == original_created_at


def test_upsert_crawled_data_failure_reloads_unsynced_state_and_can_retry(tmp_path, monkeypatch):
    store = CSVStore(str(tmp_path))
    address = "TUoHaVjx7n5xz8LwPRDckgFrDWhMhuSuJM"
    store.insert_seeds([{"wallet_address": address, "label": 0}])
    real_append = store._append_rows
    failed = False

    def fail_transaction_once(path, header, rows):
        nonlocal failed
        if path == store.transactions_path and not failed:
            failed = True
            raise OSError("synthetic transaction append failure")
        real_append(path, header, rows)

    monkeypatch.setattr(store, "_append_rows", fail_transaction_once)
    tx = {"tx_hash": "retry-hash", "from_address": address, "to_address": "T2", "amount": "42"}
    with pytest.raises(OSError, match="synthetic transaction append failure"):
        store.upsert_crawled_data(
            [{"address": address, "last_seen_timestamp": "durable-account"}],
            [tx],
            [address],
            [],
        )

    assert store.crawl_state[address]["synced"] is False
    assert "retry-hash" not in store.tx_hashes
    store.upsert_crawled_data([], [tx], [address], [])
    assert store.crawl_state[address]["synced"] is True
    assert store.tx_hashes == {"retry-hash"}
