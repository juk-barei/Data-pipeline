import csv

import pytest

from pipeline.seeds.blacklist import load_local_blacklist
from pipeline.utils.address import to_hex


ADDRESS = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
OTHER = "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb"


def test_multiple_files_normalize_and_deduplicate_without_changing_sources(tmp_path):
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    first.write_text(
        f"wallet_address,label\n {ADDRESS} ,scam\n{to_hex(ADDRESS)},phishing\n",
        encoding="utf-8",
    )
    second.write_text(f"address\n{OTHER}\n{ADDRESS}\n", encoding="utf-8")
    originals = [path.read_bytes() for path in (first, second)]

    assert load_local_blacklist([str(first), str(second), str(first)]) == {ADDRESS, OTHER}
    assert [path.read_bytes() for path in (first, second)] == originals


def test_missing_and_empty_files_are_ignored(tmp_path):
    empty = tmp_path / "empty.csv"
    empty.touch()
    assert load_local_blacklist([str(tmp_path / "absent.csv"), str(empty)]) == set()
    assert load_local_blacklist([]) == set()


def test_bom_case_insensitive_headers_and_multiple_address_columns(tmp_path):
    path = tmp_path / "export.csv"
    path.write_text(
        f" Wallet_Address , HEX_ADDRESS ,notes\n{ADDRESS},{to_hex(OTHER)},{OTHER}\n",
        encoding="utf-8-sig",
    )
    assert load_local_blacklist([str(path)]) == {ADDRESS, OTHER}


def test_invalid_blank_and_short_rows_are_skipped_and_metadata_is_not_an_address(tmp_path):
    path = tmp_path / "invalid.csv"
    path.write_text(
        f"label,address\n{OTHER},invalid\nblank,\nshort\n\nvalid,{ADDRESS}\n",
        encoding="utf-8",
    )
    assert load_local_blacklist([str(path)]) == {ADDRESS}


def test_quoted_newlines_in_metadata_do_not_break_address_rows(tmp_path):
    path = tmp_path / "quoted.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["address", "notes"])
        writer.writerow([ADDRESS, 'first line\nsecond, "quoted" line'])
    assert load_local_blacklist([str(path)]) == {ADDRESS}


def test_nonempty_file_without_address_column_is_reported(tmp_path):
    path = tmp_path / "wrong.csv"
    path.write_text("label\nscam\n", encoding="utf-8")
    with pytest.raises(ValueError, match="no address column"):
        load_local_blacklist([str(path)])


def test_malformed_csv_is_reported(tmp_path):
    path = tmp_path / "broken.csv"
    path.write_text('address\n"unterminated', encoding="utf-8")
    with pytest.raises(csv.Error):
        load_local_blacklist([str(path)])


def test_permission_errors_are_not_treated_as_missing_files(monkeypatch):
    def deny_open(*args, **kwargs):
        raise PermissionError("blacklist is unreadable")

    monkeypatch.setattr("builtins.open", deny_open)
    with pytest.raises(PermissionError, match="unreadable"):
        load_local_blacklist(["restricted.csv"])
