import gzip
import builtins

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


def test_gzip_streams_compressible_csv_and_preserves_source(tmp_path, monkeypatch):
    source = tmp_path / "transactions.csv"
    content = b"address,amount\n" + b"wallet,123456789\n" * 100_000
    source.write_bytes(content)
    original_copy = drive.shutil.copyfileobj
    chunks = []

    def copy_bounded(reader, writer, length):
        chunks.append(length)
        return original_copy(reader, writer, length)

    monkeypatch.setattr(drive.shutil, "copyfileobj", copy_bounded)
    result = drive.compress_gzip(source)
    assert result == source.with_suffix(".csv.gz")
    assert gzip.decompress(result.read_bytes()) == content
    assert result.stat().st_size < len(content) / 10
    assert source.read_bytes() == content
    assert chunks == [1024 * 1024]
    previous = result.read_bytes()
    assert drive.compress_gzip(source).read_bytes() == previous


def test_gzip_failure_preserves_existing_export_and_cleans_temp(tmp_path, monkeypatch):
    source = tmp_path / "accounts.csv"
    source.write_text("address\nwallet\n", encoding="utf-8")
    destination = source.with_suffix(".csv.gz")
    destination.write_bytes(b"previous export")

    def fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(drive.shutil, "copyfileobj", fail)
    with pytest.raises(OSError, match="disk full"):
        drive.compress_gzip(source)
    assert destination.read_bytes() == b"previous export"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["accounts.csv", "accounts.csv.gz"]


def test_gzip_empty_input_roundtrips(tmp_path):
    source = tmp_path / "empty.csv"
    source.write_bytes(b"")
    assert gzip.decompress(drive.compress_gzip(source).read_bytes()) == b""


def test_parquet_roundtrip_preserves_strings_and_uses_bounded_batches(tmp_path):
    parquet = pytest.importorskip("pyarrow.parquet")
    source = tmp_path / "transactions.csv"
    row = '000123,99999999999999999999999999,"line one\nline two",\n'
    source.write_text("address,amount,note,empty\n" + row * 30_000, encoding="utf-8", newline="")
    result = drive.csv_to_parquet(source)
    metadata = parquet.ParquetFile(result)
    assert metadata.metadata.num_row_groups > 1
    assert metadata.metadata.row_group(0).column(0).compression == "ZSTD"
    values = metadata.read().to_pydict()
    assert values["address"] == ["000123"] * 30_000
    assert values["amount"] == ["99999999999999999999999999"] * 30_000
    assert values["note"] == ["line one\nline two"] * 30_000
    assert values["empty"] == [""] * 30_000
    pandas = pytest.importorskip("pandas")
    assert pandas.read_parquet(result).iloc[0].to_dict() == {
        "address": "000123", "amount": "99999999999999999999999999",
        "note": "line one\nline two", "empty": "",
    }


def test_parquet_header_only_has_readable_schema(tmp_path):
    parquet = pytest.importorskip("pyarrow.parquet")
    source = tmp_path / "accounts.csv"
    source.write_text("address,created_at\n", encoding="utf-8")
    result = parquet.read_table(drive.csv_to_parquet(source))
    assert result.num_rows == 0
    assert result.column_names == ["address", "created_at"]


def test_parquet_invalid_input_preserves_existing_export_and_cleans_temp(tmp_path):
    arrow = pytest.importorskip("pyarrow")
    source = tmp_path / "accounts.csv"
    source.write_text("a,b\none,two,three\n", encoding="utf-8")
    destination = source.with_suffix(".parquet")
    destination.write_bytes(b"previous export")
    with pytest.raises(arrow.ArrowInvalid, match="CSV parse error"):
        drive.csv_to_parquet(source)
    assert destination.read_bytes() == b"previous export"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["accounts.csv", "accounts.parquet"]


@pytest.mark.parametrize("content", ["", "a,a\n", "a,\n"])
def test_parquet_requires_unique_nonempty_header(tmp_path, content):
    pytest.importorskip("pyarrow")
    source = tmp_path / "invalid.csv"
    source.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError, match="header"):
        drive.csv_to_parquet(source)


def test_parquet_missing_optional_dependency_has_install_hint(tmp_path, monkeypatch):
    original_import = builtins.__import__

    def without_arrow(name, *args, **kwargs):
        if name == "pyarrow":
            raise ImportError("not installed")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_arrow)
    with pytest.raises(ImportError, match=r"tron-data-pipeline\[parquet\]"):
        drive.csv_to_parquet(tmp_path / "unused.csv")
