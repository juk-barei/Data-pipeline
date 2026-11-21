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


def test_claim_crawl_batch_orders_frontier_without_mutating_state(tmp_path):
    store = CSVStore(str(tmp_path))
    store.crawl_state = {
        "T3": {"address": "T3", "hop_level": 2, "created_at": "2026-01-01T00:00:00+00:00", "synced": False},
        "T2": {"address": "T2", "hop_level": 1, "created_at": "2026-01-02T00:00:00+00:00", "synced": False},
        "T1": {"address": "T1", "hop_level": 1, "created_at": "2026-01-01T00:00:00+00:00", "synced": False},
        "DONE": {"address": "DONE", "hop_level": 0, "created_at": "2025-01-01T00:00:00+00:00", "synced": True},
    }

    claimed = store.claim_crawl_batch(2)

    assert [row["address"] for row in claimed] == ["T1", "T2"]
    claimed[0]["synced"] = True
    assert store.crawl_state["T1"]["synced"] is False
    assert store.claim_crawl_batch(0) == []


@pytest.mark.parametrize("size", [-1, True, 1.5, "1"])
def test_claim_crawl_batch_rejects_invalid_sizes(tmp_path, size):
    with pytest.raises(ValueError, match="non-negative integer"):
        CSVStore(str(tmp_path)).claim_crawl_batch(size)


def test_claim_expand_batch_includes_supernodes_and_hop_five_for_later_pruning(tmp_path):
    store = CSVStore(str(tmp_path))
    store.crawl_state = {
        "PENDING": {"address": "PENDING", "hop_level": 0, "created_at": "2026-01-01", "synced": False, "expanded": False, "is_supernode": False},
        "DONE": {"address": "DONE", "hop_level": 0, "created_at": "2026-01-01", "synced": True, "expanded": True, "is_supernode": False},
        "HOP5": {"address": "HOP5", "hop_level": 5, "created_at": "2026-01-01", "synced": True, "expanded": False, "is_supernode": False},
        "SUPER": {"address": "SUPER", "hop_level": 1, "created_at": "2026-01-02", "synced": True, "expanded": False, "is_supernode": True},
        "FIRST": {"address": "FIRST", "hop_level": 1, "created_at": "2026-01-01", "synced": True, "expanded": False, "is_supernode": False},
    }

    claimed = store.claim_expand_batch(3)

    assert [row["address"] for row in claimed] == ["FIRST", "SUPER", "HOP5"]
    claimed[0]["expanded"] = True
    assert store.crawl_state["FIRST"]["expanded"] is False


@pytest.mark.parametrize("size", [-1, False, 1.5, "2"])
def test_claim_expand_batch_rejects_invalid_sizes(tmp_path, size):
    with pytest.raises(ValueError, match="non-negative integer"):
        CSVStore(str(tmp_path)).claim_expand_batch(size)


def test_mark_supernode_persists_flags_visible_after_reload(tmp_path):
    store = CSVStore(str(tmp_path))
    address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    store.insert_seeds([{"wallet_address": address, "label": 0}])
    before = store.crawl_state[address]["updated_at"]

    store.mark_supernode(address)

    assert store.crawl_state[address]["is_supernode"] is True
    assert store.crawl_state[address]["expanded"] is True
    assert store.crawl_state[address]["updated_at"] >= before
    row_count = len(list(store._iter_rows(store.crawl_state_path)))
    store.mark_supernode(address)
    assert len(list(store._iter_rows(store.crawl_state_path))) == row_count
    reopened = CSVStore(str(tmp_path))
    assert reopened.crawl_state[address]["is_supernode"] is True
    assert reopened.crawl_state[address]["expanded"] is True


def test_mark_supernode_write_failure_reloads_nonphantom_state(tmp_path, monkeypatch):
    store = CSVStore(str(tmp_path))
    address = "TUoHaVjx7n5xz8LwPRDckgFrDWhMhuSuJM"
    store.insert_seeds([{"wallet_address": address, "label": 0}])

    def fail(_addresses):
        raise OSError("synthetic state write failure")

    monkeypatch.setattr(store, "_append_crawl_state_rows", fail)
    with pytest.raises(OSError, match="synthetic state write failure"):
        store.mark_supernode(address)

    assert store.crawl_state[address]["is_supernode"] is False
    assert store.crawl_state[address]["expanded"] is False


def test_get_wallet_transactions_seeks_offsets_and_supports_multiline_fields(tmp_path):
    store = CSVStore(str(tmp_path))
    store.upsert_crawled_data(
        [],
        [
            {"tx_hash": "h1", "from_address": "T1", "to_address": "T2", "token_symbol": "line one\nline two", "amount": "1"},
            {"tx_hash": "h2", "from_address": "T3", "to_address": "T1", "token_symbol": "TRX", "amount": "2"},
            {"tx_hash": "h3", "from_address": "T9", "to_address": "T8", "amount": "3"},
        ],
        [],
        [],
    )
    reopened = CSVStore(str(tmp_path))

    rows = reopened.get_wallet_transactions("T1")

    assert [row["tx_hash"] for row in rows] == ["h1", "h2"]
    assert rows[0]["token_symbol"] == "line one\nline two"
    assert [row["amount"] for row in rows] == ["1", "2"]


def test_get_wallet_transactions_unknown_wallet_does_not_touch_disk(tmp_path):
    store = CSVStore(str(tmp_path))
    store.transactions_path.unlink()

    assert store.get_wallet_transactions("UNKNOWN") == []


def test_get_wallet_transactions_detects_stale_offset(tmp_path):
    store = CSVStore(str(tmp_path))
    store.upsert_crawled_data(
        [],
        [
            {"tx_hash": "h1", "from_address": "T1", "to_address": "T2"},
            {"tx_hash": "h2", "from_address": "T3", "to_address": "T4"},
        ],
        [],
        [],
    )
    store.tx_offsets["h1"] = store.tx_offsets["h2"]

    with pytest.raises(RuntimeError, match="stale transaction offset"):
        store.get_wallet_transactions("T1")


def test_add_expansion_results_persists_edges_frontier_then_parent_completion(tmp_path):
    store = CSVStore(str(tmp_path))
    parent = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    child = "TUoHaVjx7n5xz8LwPRDckgFrDWhMhuSuJM"
    store.insert_seeds([{"wallet_address": parent, "label": 0}])
    store.upsert_crawled_data([], [], [parent], [])
    account = {"address": child, "first_seen_timestamp": "first"}
    crawl = {"address": child, "seed_address": parent, "hop_level": 2}
    hop = {
        "wallet_address": child, "seed_address": parent, "parent_address": parent,
        "via_tx_hash": "h1", "hop_level": 2, "direction": "out",
    }

    store.add_expansion_results([account], [crawl], [hop, dict(hop)], [parent])
    store.add_expansion_results([], [{**crawl, "hop_level": 3}], [hop], [parent])

    assert store.crawl_state[parent]["expanded"] is True
    assert store.crawl_state[child]["hop_level"] == 2
    assert len(store.hops) == 1
    assert store.hops[0]["hop_id"] == 1
    reopened = CSVStore(str(tmp_path))
    assert reopened.crawl_state[parent]["expanded"] is True
    assert reopened.crawl_state[child]["hop_level"] == 2
    assert reopened.hop_keys == {(child, parent, 2)}


def test_add_expansion_failure_does_not_mark_parent_expanded_and_retries(tmp_path, monkeypatch):
    store = CSVStore(str(tmp_path))
    parent = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    child = "TUoHaVjx7n5xz8LwPRDckgFrDWhMhuSuJM"
    store.insert_seeds([{"wallet_address": parent, "label": 0}])
    real_append = store._append_rows
    failed = False

    def fail_hops_once(path, header, rows):
        nonlocal failed
        if path == store.wallet_hops_path and not failed:
            failed = True
            raise OSError("synthetic hop write failure")
        real_append(path, header, rows)

    monkeypatch.setattr(store, "_append_rows", fail_hops_once)
    account = {"address": child}
    crawl = {"address": child, "seed_address": parent, "hop_level": 1}
    hop = {"wallet_address": child, "seed_address": parent, "hop_level": 1}
    with pytest.raises(OSError, match="synthetic hop write failure"):
        store.add_expansion_results([account], [crawl], [hop], [parent])

    assert store.crawl_state[parent]["expanded"] is False
    assert child not in store.crawl_state
    assert store.hops == []
    store.add_expansion_results([account], [crawl], [hop], [parent])
    assert store.crawl_state[parent]["expanded"] is True
    assert child in store.crawl_state


@pytest.mark.parametrize("invalid_level", [-1, 1.5, "1.5", True])
def test_add_expansion_rejects_invalid_frontier_before_any_write(tmp_path, invalid_level):
    store = CSVStore(str(tmp_path))
    parent = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    child = "TUoHaVjx7n5xz8LwPRDckgFrDWhMhuSuJM"
    store.insert_seeds([{"wallet_address": parent, "label": 0}])
    before = {name: path.read_bytes() for name, path in store.paths.items()}

    with pytest.raises(ValueError, match="non-negative integer"):
        store.add_expansion_results(
            [{"address": child}],
            [{"address": child, "seed_address": parent, "hop_level": invalid_level}],
            [],
            [parent],
        )

    assert {name: path.read_bytes() for name, path in store.paths.items()} == before
    assert store.crawl_state[parent]["expanded"] is False
    assert child not in store.accounts


def test_add_expansion_requires_frontier_seed_before_any_write(tmp_path):
    store = CSVStore(str(tmp_path))
    parent = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    child = "TUoHaVjx7n5xz8LwPRDckgFrDWhMhuSuJM"
    store.insert_seeds([{"wallet_address": parent, "label": 0}])
    before = {name: path.read_bytes() for name, path in store.paths.items()}

    with pytest.raises(ValueError, match="seed_address"):
        store.add_expansion_results(
            [{"address": child}],
            [{"address": child, "hop_level": 1}],
            [],
            [parent],
        )

    assert {name: path.read_bytes() for name, path in store.paths.items()} == before


def test_compact_rewrites_latest_logs_and_leaves_other_files_and_indexes_untouched(tmp_path):
    store = CSVStore(str(tmp_path))
    address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    store.insert_seeds([{"wallet_address": address, "label": 0}])
    store.upsert_crawled_data(
        [{"address": address, "last_seen_timestamp": "one"}],
        [{"tx_hash": "h1", "from_address": address, "to_address": "T2"}],
        [address],
        [],
    )
    store.upsert_crawled_data(
        [{"address": address, "last_seen_timestamp": "two"}], [], [address], []
    )
    untouched_before = {
        name: store.paths[name].read_bytes()
        for name in ("seed_wallets.csv", "transactions.csv", "wallet_hops.csv")
    }
    offsets_before = dict(store.tx_offsets)
    assert len(list(store._iter_rows(store.accounts_path))) > len(store.accounts)
    assert len(list(store._iter_rows(store.crawl_state_path))) > len(store.crawl_state)

    store.compact()

    assert len(list(store._iter_rows(store.accounts_path))) == len(store.accounts)
    assert len(list(store._iter_rows(store.crawl_state_path))) == len(store.crawl_state)
    assert {
        name: store.paths[name].read_bytes() for name in untouched_before
    } == untouched_before
    assert store.tx_offsets == offsets_before
    assert store._appended_since_compact == 0
    reopened = CSVStore(str(tmp_path))
    assert reopened.accounts[address]["last_seen_timestamp"] == "two"
    assert reopened.crawl_state[address]["synced"] is True


def test_compact_second_snapshot_failure_keeps_counter_and_coherent_first_file(tmp_path, monkeypatch):
    store = CSVStore(str(tmp_path))
    address = "TUoHaVjx7n5xz8LwPRDckgFrDWhMhuSuJM"
    store.insert_seeds([{"wallet_address": address, "label": 0}])
    store.accounts[address]["last_seen_timestamp"] = "latest"
    store._append_account_rows([address])
    counter_before = store._appended_since_compact
    real_write = store._write_snapshot

    def fail_crawl(path, header, rows):
        if path == store.crawl_state_path:
            raise OSError("synthetic crawl snapshot failure")
        real_write(path, header, rows)

    monkeypatch.setattr(store, "_write_snapshot", fail_crawl)
    with pytest.raises(OSError, match="synthetic crawl snapshot failure"):
        store.compact()

    assert store._appended_since_compact == counter_before
    assert list(store._iter_rows(store.accounts_path)) == [store.accounts[address]]


def test_compact_if_needed_uses_strict_live_row_threshold(tmp_path, monkeypatch):
    store = CSVStore(str(tmp_path))
    store.accounts = {"A": {}}
    store.crawl_state = {"A": {}}
    calls = []
    monkeypatch.setattr(store, "compact", lambda: calls.append("compact"))

    store._appended_since_compact = 6
    store.compact_if_needed(factor=3)
    assert calls == []

    store._appended_since_compact = 7
    store.compact_if_needed(factor=3)
    assert calls == ["compact"]


@pytest.mark.parametrize("factor", [0, -1, True, 1.5, "10"])
def test_compact_if_needed_rejects_invalid_factor(tmp_path, factor):
    with pytest.raises(ValueError, match="positive integer"):
        CSVStore(str(tmp_path)).compact_if_needed(factor)


def test_compact_if_needed_bounds_repeated_mutable_log_growth(tmp_path):
    store = CSVStore(str(tmp_path))
    address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    store.insert_seeds([{"wallet_address": address, "label": 0}])
    for index in range(8):
        store.accounts[address]["last_seen_timestamp"] = str(index)
        store._append_account_rows([address])
        store.crawl_state[address]["updated_at"] = str(index)
        store._append_crawl_state_rows([address])

    store.compact_if_needed(factor=2)

    assert store._appended_since_compact == 0
    assert len(list(store._iter_rows(store.accounts_path))) == 1
    assert len(list(store._iter_rows(store.crawl_state_path))) == 1


def test_reset_to_seeds_preserves_seed_source_and_clears_all_derived_state(tmp_path):
    store = CSVStore(str(tmp_path))
    seed1 = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    seed2 = "TUoHaVjx7n5xz8LwPRDckgFrDWhMhuSuJM"
    store.insert_seeds([
        {"wallet_address": seed1, "label": 0},
        {"wallet_address": seed2, "label": 0},
    ])
    store.upsert_crawled_data(
        [],
        [{"tx_hash": "h1", "from_address": seed1, "to_address": seed2}],
        [seed1, seed2],
        [],
    )
    child = "T-child"
    store.add_expansion_results(
        [{"address": child}],
        [{"address": child, "seed_address": seed1, "hop_level": 1}],
        [{"wallet_address": child, "seed_address": seed1, "hop_level": 1}],
        [seed1],
    )
    seed_bytes = store.seed_wallets_path.read_bytes()

    store.reset_to_seeds()

    assert store.seed_wallets_path.read_bytes() == seed_bytes
    assert set(store.accounts) == {seed1, seed2}
    assert store.tx_hashes == set()
    assert store.wallet_tx_hashes == {}
    assert store.tx_offsets == {}
    assert store.hops == []
    assert store.hop_keys == set()
    assert store.max_hop_id == 0
    assert set(store.crawl_state) == {seed1, seed2}
    assert all(
        row["hop_level"] == 0
        and row["synced"] is False
        and row["expanded"] is False
        and row["is_supernode"] is False
        for row in store.crawl_state.values()
    )
    assert list(store._iter_rows(store.transactions_path)) == []
    assert list(store._iter_rows(store.wallet_hops_path)) == []
    assert store._appended_since_compact == 0
    assert not store._reset_marker_path.exists()


def test_reset_to_seeds_restart_rolls_forward_after_mid_reset_failure(tmp_path, monkeypatch):
    store = CSVStore(str(tmp_path))
    seed = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    store.insert_seeds([{"wallet_address": seed, "label": 0}])
    store.upsert_crawled_data(
        [], [{"tx_hash": "h1", "from_address": seed, "to_address": "T2"}], [seed], []
    )
    seed_bytes = store.seed_wallets_path.read_bytes()
    real_write = store._write_snapshot

    def fail_hops(path, header, rows):
        if path == store.wallet_hops_path:
            raise OSError("synthetic mid-reset failure")
        real_write(path, header, rows)

    monkeypatch.setattr(store, "_write_snapshot", fail_hops)
    with pytest.raises(OSError, match="synthetic mid-reset failure"):
        store.reset_to_seeds()
    assert store._reset_marker_path.exists()

    recovered = CSVStore(str(tmp_path))

    assert recovered.seed_wallets_path.read_bytes() == seed_bytes
    assert set(recovered.accounts) == {seed}
    assert recovered.tx_hashes == set()
    assert recovered.hops == []
    assert recovered.crawl_state[seed]["hop_level"] == 0
    assert recovered.crawl_state[seed]["synced"] is False
    assert not recovered._reset_marker_path.exists()


def test_get_metrics_empty_dataset_is_incomplete_and_uses_no_disk(tmp_path, monkeypatch):
    store = CSVStore(str(tmp_path))
    monkeypatch.setattr(
        store,
        "_iter_rows",
        lambda _path: (_ for _ in ()).throw(AssertionError("disk read")),
    )

    metrics = store.get_metrics()

    assert metrics == {
        "total_seeds": 0,
        "total_accounts": 0,
        "total_transactions": 0,
        "total_hops": 0,
        "total_crawl_states": 0,
        "supernodes": 0,
        "pending_crawls": 0,
        "pending_expands": 0,
        "per_hop": {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0},
        "finished_seeds": 0,
        "is_complete": False,
    }


def test_get_metrics_counts_all_seed_descendants_before_finished(tmp_path):
    store = CSVStore(str(tmp_path))
    store.seeds = {"S1": {}, "S2": {}}
    store.accounts = {"S1": {}, "S2": {}, "C1": {}, "C2": {}, "C3": {}}
    store.tx_hashes = {"h1", "h2"}
    store.hops = [{}, {}]
    store.crawl_state = {
        "S1": {"address": "S1", "seed_address": "S1", "hop_level": 0, "synced": True, "expanded": True, "is_supernode": False},
        "C1": {"address": "C1", "seed_address": "S1", "hop_level": 1, "synced": True, "expanded": True, "is_supernode": False},
        "S2": {"address": "S2", "seed_address": "S2", "hop_level": 0, "synced": True, "expanded": True, "is_supernode": False},
        "C2": {"address": "C2", "seed_address": "S2", "hop_level": 5, "synced": True, "expanded": False, "is_supernode": False},
        "C3": {"address": "C3", "seed_address": "S2", "hop_level": 2, "synced": True, "expanded": False, "is_supernode": True},
    }

    metrics = store.get_metrics()

    assert metrics["total_accounts"] == 5
    assert metrics["total_transactions"] == 2
    assert metrics["total_hops"] == 2
    assert metrics["pending_crawls"] == 0
    assert metrics["pending_expands"] == 2
    assert metrics["per_hop"] == {0: 2, 1: 1, 2: 1, 3: 0, 4: 0, 5: 1}
    assert metrics["supernodes"] == 1
    assert metrics["finished_seeds"] == 1
    assert metrics["is_complete"] is False

    store.crawl_state["C2"]["expanded"] = True
    store.crawl_state["C3"]["expanded"] = True
    completed = store.get_metrics()
    assert completed["finished_seeds"] == 2
    assert completed["is_complete"] is True
