import json
from pathlib import Path
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

from pipeline.storage.csv_store import CSVStore
from pipeline.storage.schema import build_headers
from pipeline.sync.drive import compress_gzip, csv_to_parquet


@pytest.fixture
def notebook():
    pytest.importorskip("pandas")
    pytest.importorskip("networkx")
    namespace = {}
    path = Path(__file__).resolve().parents[1] / "colab" / "analysis.ipynb"
    for cell in json.loads(path.read_text(encoding="utf-8"))["cells"]:
        if cell["cell_type"] == "code" and "definitions" in cell["metadata"].get("tags", []):
            exec(compile("".join(cell["source"]), str(path), "exec"), namespace)
    return namespace


@pytest.fixture
def dataset(tmp_path):
    store = CSVStore(str(tmp_path))
    store._append_rows(store.accounts_path, store.headers["accounts.csv"], [
        {"address": "NA", "hex_address": "000123"},
    ])
    store._append_rows(store.transactions_path, store.headers["transactions.csv"], [
        {"tx_hash": "001", "amount": "123456789012345678901234567890", "token_symbol": "NULL"},
    ])
    return store


@pytest.mark.parametrize("mode", ["csv", "gzip", "parquet", "mixed"])
def test_notebook_loads_all_formats_without_type_loss(notebook, dataset, mode):
    if mode in ("parquet", "mixed"):
        pytest.importorskip("pyarrow")
    for index, path in enumerate(dataset.paths.values()):
        if mode == "gzip" or (mode == "mixed" and index % 2):
            compress_gzip(path)
        elif mode in ("parquet", "mixed"):
            csv_to_parquet(path)
        if mode != "csv":
            path.unlink()
    dfs = notebook["colab_load_dataset"](dataset.csv_dir, compressed=mode != "csv")
    assert {name + ".csv": list(frame.columns) for name, frame in dfs.items()} == build_headers()
    assert dfs["accounts"].iloc[0]["address"] == "NA"
    assert dfs["accounts"].iloc[0]["hex_address"] == "000123"
    assert dfs["accounts"].iloc[0]["created_at"] == ""
    assert dfs["transactions"].iloc[0]["amount"] == "123456789012345678901234567890"
    assert dfs["transactions"].iloc[0]["token_symbol"] == "NULL"
    assert dfs["seed_wallets"].empty


def test_notebook_missing_table_and_bad_schema_are_clear(notebook, dataset):
    dataset.accounts_path.write_text("wrong,columns\n", encoding="utf-8")
    with pytest.raises(ValueError, match="schema.*accounts"):
        notebook["colab_load_dataset"](dataset.csv_dir)
    dataset.accounts_path.unlink()
    with pytest.raises(FileNotFoundError, match="accounts"):
        notebook["colab_load_dataset"](dataset.csv_dir)


def test_notebook_compressed_does_not_silently_load_stale_raw(notebook, dataset):
    with pytest.raises(FileNotFoundError, match="csv.gz.*parquet"):
        notebook["colab_load_dataset"](dataset.csv_dir, compressed=True)
    with pytest.raises(ValueError, match="boolean"):
        notebook["colab_load_dataset"](dataset.csv_dir, compressed="gzip")


def test_notebook_mounts_drive_only_for_colab_path(notebook, monkeypatch, dataset):
    module = ModuleType("google.colab")
    module.drive = Mock()
    monkeypatch.setitem(sys.modules, "google.colab", module)
    real_relative = Path.is_relative_to
    monkeypatch.setattr(Path, "is_relative_to", lambda path, other: True)
    real_is_dir = Path.is_dir
    monkeypatch.setattr(Path, "is_dir", lambda path: False if path == Path("/content/drive/MyDrive") else real_is_dir(path))
    notebook["colab_load_dataset"](dataset.csv_dir)
    module.drive.mount.assert_called_once_with(str(Path("/content/drive")))
    module.drive.mount.reset_mock()
    monkeypatch.setattr(Path, "is_relative_to", real_relative)
    notebook["colab_load_dataset"](dataset.csv_dir)
    module.drive.mount.assert_not_called()


@pytest.mark.parametrize("populated", [False, True])
def test_analysis_metrics_match_csvstore_append_log_semantics(notebook, tmp_path, populated):
    store = CSVStore(str(tmp_path))
    if populated:
        records = {
            "seed_wallets.csv": [
                {"wallet_address": "A", "label": "0"},
                {"wallet_address": "B", "label": "0"},
                {"wallet_address": "C", "label": "0"},
                {"wallet_address": "C", "label": "1"},
            ],
            "accounts.csv": [{"address": "A"}, {"address": "B"}, {"address": "A", "hex_address": "latest"}],
            "transactions.csv": [
                {"tx_hash": "001", "from_address": "A", "to_address": "B", "amount": "1"},
                {"tx_hash": "001", "from_address": "B", "to_address": "C", "amount": "2"},
                {"tx_hash": "002", "from_address": "A", "to_address": "B"},
                {"tx_hash": "003", "from_address": "B", "to_address": "B"},
                {"tx_hash": "", "from_address": "ignored", "to_address": "row"},
            ],
            "wallet_hops.csv": [
                {"hop_id": "1", "wallet_address": "B", "seed_address": "A", "hop_level": "1"},
                {"hop_id": "2", "wallet_address": "B", "seed_address": "A", "hop_level": "01"},
            ],
            "pipeline_crawl_state.csv": [
                {"address": "A", "seed_address": "A", "hop_level": "0", "synced": "FALSE", "expanded": "FALSE", "is_supernode": "FALSE"},
                {"address": "A", "seed_address": "A", "hop_level": "0", "synced": "TRUE", "expanded": "FALSE", "is_supernode": "FALSE"},
                {"address": "B", "seed_address": "B", "hop_level": "1", "synced": "FALSE", "expanded": "FALSE", "is_supernode": "FALSE"},
                {"address": "C", "seed_address": "A", "hop_level": "5", "synced": "TRUE", "expanded": "TRUE", "is_supernode": "TRUE"},
            ],
        }
        for filename, rows in records.items():
            store._append_rows(store.paths[filename], store.headers[filename], rows)
    store.load()
    dfs = notebook["colab_load_dataset"](tmp_path)
    before = {key: frame.copy(deep=True) for key, frame in dfs.items()}
    result = notebook["colab_run_analysis"](dfs)
    assert result["metrics"] == store.get_metrics()
    for key, frame in dfs.items():
        notebook["pd"].testing.assert_frame_equal(frame, before[key])
    if populated:
        assert result["summary"] == {"nodes": 3, "edges": 3, "weak_components": 2}
        assert result["graph"]["A"]["B"]["001"]["amount"] == "1"
        assert result["graph"].nodes["C"]["hop_level"] == 5
    else:
        assert result["summary"] == {"nodes": 0, "edges": 0, "weak_components": 0}


@pytest.mark.parametrize("field,value", [("synced", "yes"), ("hop_level", "-1"), ("address", "")])
def test_analysis_rejects_corrupt_state(notebook, dataset, field, value):
    row = {"address": "A", "seed_address": "A", "hop_level": "0", "synced": "TRUE", "expanded": "TRUE", "is_supernode": "FALSE"}
    row[field] = value
    dataset._append_rows(dataset.crawl_state_path, dataset.headers["pipeline_crawl_state.csv"], [row])
    dfs = notebook["colab_load_dataset"](dataset.csv_dir)
    with pytest.raises(ValueError):
        notebook["colab_run_analysis"](dfs)


def test_analysis_rejects_missing_table(notebook, dataset):
    dfs = notebook["colab_load_dataset"](dataset.csv_dir)
    del dfs["wallet_hops"]
    with pytest.raises(ValueError, match="wallet_hops"):
        notebook["colab_run_analysis"](dfs)


def test_analysis_completed_frontier_matches_store(notebook, dataset):
    dataset._append_rows(dataset.seed_wallets_path, dataset.headers["seed_wallets.csv"], [{"wallet_address": "A", "label": "0"}])
    dataset._append_rows(dataset.crawl_state_path, dataset.headers["pipeline_crawl_state.csv"], [{"address": "A", "seed_address": "A", "hop_level": "0", "synced": "TRUE", "expanded": "TRUE", "is_supernode": "FALSE"}])
    dataset.load()
    result = notebook["colab_run_analysis"](notebook["colab_load_dataset"](dataset.csv_dir))
    assert result["metrics"] == dataset.get_metrics()
    assert result["metrics"]["finished_seeds"] == 1
    assert result["metrics"]["is_complete"] is True
