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
