"""Tests for the valkey_rdb Python module."""

import pyarrow as pa
import pytest

import valkey_rdb


# --- Happy path ---


def test_read_basic(basic_rdb):
    """read() returns a dict of pyarrow.Table keyed by type name."""
    tables = valkey_rdb.read(basic_rdb)
    assert isinstance(tables, dict)
    assert len(tables) > 0
    for name, table in tables.items():
        assert isinstance(name, str)
        assert isinstance(table, pa.Table)
        assert table.num_rows > 0


def test_read_batches(basic_rdb):
    """read_batches() yields (type_name, RecordBatch) tuples lazily."""
    reader = valkey_rdb.read_batches(basic_rdb)
    results = list(reader)
    assert len(results) > 0
    for name, batch in results:
        assert isinstance(name, str)
        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows > 0


def test_to_parquet(basic_rdb, tmp_path):
    """to_parquet() creates .parquet files in the output directory."""
    output_dir = str(tmp_path / "parquet_out")
    valkey_rdb.to_parquet(basic_rdb, output_dir)
    files = list(tmp_path.joinpath("parquet_out").glob("*.parquet"))
    assert len(files) > 0

    for f in files:
        table = pa.parquet.read_table(str(f))
        assert table.num_rows > 0


def test_to_parquet_records_rdb_metadata(basic_rdb, tmp_path):
    """Every Parquet file written by Python must carry the same
    ``rdb.exported_by`` and ``rdb.heuristics`` tags the CLI writes. Without
    these the ``validate`` command cannot reproduce the heuristic set that
    produced the export, so Python-generated trees would silently fail
    validation against anything other than the default "all" policy.

    This pins the FFI seam: a prior regression (Python forgot to insert
    ``rdb.heuristics``) went undetected because no test ever read the
    metadata back from pyarrow. It also cross-validates the heuristics
    string with what the Rust side's ``Heuristic::format_set`` produces.
    """
    output_dir = str(tmp_path / "parquet_out")
    valkey_rdb.to_parquet(basic_rdb, output_dir, heuristic="all")
    files = list(tmp_path.joinpath("parquet_out").glob("*.parquet"))
    assert len(files) > 0, "to_parquet() must emit at least one .parquet file"

    for f in files:
        pq_meta = pa.parquet.read_metadata(str(f))
        schema_meta = pq_meta.schema.to_arrow_schema().metadata or {}
        # pyarrow keys/values are bytes.
        assert b"rdb.exported_by" in schema_meta, (
            f"{f.name} missing rdb.exported_by metadata tag"
        )
        assert schema_meta[b"rdb.exported_by"] == b"valkey-rdb-tools"
        assert b"rdb.heuristics" in schema_meta, (
            f"{f.name} missing rdb.heuristics metadata tag"
        )
        # `heuristic="all"` must include "geo" today, but additional
        # built-in heuristics may be added later and would also be
        # listed here (format_set joins sorted names with commas).
        # Assert membership rather than exact equality so a new
        # heuristic doesn't silently break this test.
        heuristics_value = schema_meta[b"rdb.heuristics"].decode("ascii")
        assert heuristics_value != "none"
        names = [n.strip() for n in heuristics_value.split(",")]
        assert "geo" in names, f"expected 'geo' in {heuristics_value!r}"


def test_to_parquet_records_none_heuristics(basic_rdb, tmp_path):
    """When heuristics are disabled, the recorded tag value is "none",
    matching Rust ``Heuristic::format_set`` behavior. ``validate`` reparses
    this via ``Heuristic::parse_set`` so the exact string matters."""
    output_dir = str(tmp_path / "parquet_none")
    valkey_rdb.to_parquet(basic_rdb, output_dir, heuristic="none")
    files = list(tmp_path.joinpath("parquet_none").glob("*.parquet"))
    assert len(files) > 0

    for f in files:
        schema_meta = pa.parquet.read_metadata(str(f)).schema.to_arrow_schema().metadata or {}
        assert schema_meta[b"rdb.heuristics"] == b"none"


def test_inspect(basic_rdb):
    """inspect() returns a dict with expected keys."""
    info = valkey_rdb.inspect(basic_rdb)
    assert isinstance(info, dict)
    assert "magic" in info
    assert "rdb_version" in info
    assert "total_keys" in info
    assert "dbs" in info
    assert info["total_keys"] > 0
    assert isinstance(info["dbs"], list)
    if len(info["dbs"]) > 0:
        db_entry = info["dbs"][0]
        assert "db" in db_entry
        assert "keys" in db_entry
        assert "types" in db_entry


# --- Error handling ---


def test_read_nonexistent_file():
    """read() raises OSError for missing files."""
    with pytest.raises(OSError):
        valkey_rdb.read("/nonexistent/dump.rdb")


def test_read_batches_nonexistent_file():
    """read_batches() raises OSError for missing files."""
    with pytest.raises(OSError):
        valkey_rdb.read_batches("/nonexistent/dump.rdb")


def test_inspect_nonexistent_file():
    """inspect() raises OSError for missing files."""
    with pytest.raises(OSError):
        valkey_rdb.inspect("/nonexistent/dump.rdb")


def test_to_parquet_nonexistent_file(tmp_path):
    """to_parquet() raises OSError for missing files."""
    with pytest.raises(OSError):
        valkey_rdb.to_parquet("/nonexistent/dump.rdb", str(tmp_path))


def test_read_invalid_rdb(tmp_path):
    """read() raises on a file that isn't valid RDB."""
    bad = tmp_path / "bad.rdb"
    bad.write_bytes(b"not an rdb file at all")
    with pytest.raises((ValueError, RuntimeError)):
        valkey_rdb.read(str(bad))


def test_read_batch_size_zero():
    """batch_size=0 raises ValueError."""
    with pytest.raises(ValueError, match="batch_size must be > 0"):
        valkey_rdb.read("/dev/null", batch_size=0)


def test_to_parquet_bad_compression(tmp_path):
    """Unknown compression raises ValueError."""
    with pytest.raises(ValueError, match="unknown compression"):
        valkey_rdb.to_parquet("/dev/null", str(tmp_path), compression="brotli")
