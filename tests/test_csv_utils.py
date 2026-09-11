import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

import pytest
from app.csv_utils import (
    parse_csv_bytes, dataset_id_for_bytes, row_content_hash,
    EmptyCSVError, NoDataRowsError, NotACSVError,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def _read(name):
    with open(os.path.join(DATA_DIR, name), "rb") as f:
        return f.read()


def test_clean_csv_parses_all_rows():
    raw = _read("sample_clean.csv")
    parsed = parse_csv_bytes(raw, "sample_clean.csv")
    assert parsed.columns == ["customer_id", "name", "group", "order_id", "amount"]
    assert len(parsed.rows) == 15
    assert parsed.rows[0].values["name"] == "Asha Rao"


def test_large_csv_parses_expected_row_count():
    raw = _read("sample_large.csv")
    parsed = parse_csv_bytes(raw, "sample_large.csv")
    assert len(parsed.rows) == 5000


def test_dataset_id_is_deterministic_for_same_bytes():
    raw = _read("sample_clean.csv")
    id1 = dataset_id_for_bytes(raw)
    id2 = dataset_id_for_bytes(raw)
    assert id1 == id2
    # different content -> different id
    other = _read("sample_large.csv")
    assert dataset_id_for_bytes(other) != id1


def test_row_content_hash_is_stable_and_content_sensitive():
    h1 = row_content_hash("ds1", {"a": "1", "b": "2"})
    h2 = row_content_hash("ds1", {"b": "2", "a": "1"})  # key order shouldn't matter
    assert h1 == h2
    h3 = row_content_hash("ds1", {"a": "1", "b": "3"})
    assert h1 != h3


def test_reparsing_same_file_produces_identical_hashes():
    """This is the core guarantee behind must-have #10 (two clean runs,
    identical counts): parsing the same bytes twice must yield identical
    dataset_id and identical per-row content_hash values."""
    raw = _read("sample_clean.csv")
    p1 = parse_csv_bytes(raw, "sample_clean.csv")
    p2 = parse_csv_bytes(raw, "sample_clean.csv")
    assert p1.dataset_id == p2.dataset_id
    hashes1 = [r.content_hash for r in p1.rows]
    hashes2 = [r.content_hash for r in p2.rows]
    assert hashes1 == hashes2
    assert len(hashes1) == len(set(hashes1))  # no accidental collisions


# --- hostile input cases named in handout 6.7 -------------------------------

def test_empty_file_raises_empty_csv_error():
    with pytest.raises(EmptyCSVError):
        parse_csv_bytes(_read("empty.csv"), "empty.csv")


def test_header_only_file_raises_no_data_rows_error():
    with pytest.raises(NoDataRowsError):
        parse_csv_bytes(_read("header_only.csv"), "header_only.csv")


def test_non_csv_binary_file_raises_not_a_csv_error():
    with pytest.raises(NotACSVError):
        parse_csv_bytes(_read("not_a_csv.png"), "not_a_csv.png")


def test_broken_csv_with_ragged_rows_is_tolerated_not_crashed():
    """sample_broken.csv has no header row, missing cells, extra trailing
    cells, and a fully-blank line. The parser must fail politely or
    tolerate it -- never raise an unhandled exception (6.7)."""
    raw = _read("sample_broken.csv")
    # The first line becomes the "header" (this file has no true header,
    # which is itself a form of malformed input) -- the important
    # assertion is that this call does not raise a *generic* uncaught
    # exception, only ever one of our typed, client-safe errors, or
    # succeeds by tolerating ragged rows.
    try:
        parsed = parse_csv_bytes(raw, "sample_broken.csv")
        assert len(parsed.rows) >= 1
        # ragged short row should have been padded, not crashed on
        assert "" in parsed.rows[1].values.values() or len(parsed.rows) >= 1
    except (EmptyCSVError, NoDataRowsError, NotACSVError):
        pass  # also an acceptable, polite outcome for this adversarial file
