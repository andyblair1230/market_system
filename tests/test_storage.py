from pathlib import Path

import pytest


def _has_pyarrow() -> bool:
    try:
        import pyarrow  # noqa: F401
        import pyarrow.parquet  # noqa: F401
    except Exception:
        return False
    return True


@pytest.mark.skipif(not _has_pyarrow(), reason="pyarrow not installed")
def test_parquet_writer_roundtrip(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from market_system.storage.writer import ParquetWriter

    data = {
        "ts": [1, 2, 3],
        "price": [100.0, 101.5, 99.9],
        "qty": [1, 2, 1],
    }
    tbl = pa.table(data)

    out_file = tmp_path / "trades.parquet"
    ParquetWriter().write(tbl, out_file)

    assert out_file.exists(), "parquet file not written"

    read_back = pq.read_table(out_file)
    assert read_back.num_rows == tbl.num_rows
    assert read_back.column_names == tbl.column_names


@pytest.mark.skipif(not _has_pyarrow(), reason="pyarrow not installed")
def test_parquet_writer_partitioned_dataset(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from market_system.storage.writer import ParquetWriter

    data = {
        "date": ["2025-08-27", "2025-08-27", "2025-08-28"],
        "symbol": ["ES", "NQ", "ES"],
        "price": [1.0, 2.0, 3.0],
    }
    tbl = pa.table(data)

    root = tmp_path / "dataset"
    ParquetWriter().write(tbl, root, partitioning={"by": ["date", "symbol"]})

    # Validate partition folders exist and at least one file was produced
    assert (root / "date=2025-08-27").exists()
    # Collect all .parquet files and ensure we can read the dataset
    files = [str(p) for p in root.rglob("*.parquet")]
    assert files, "no parquet files written in partitioned dataset"

    # Read back as a dataset (Arrow will discover partitions)
    ds = pq.ParquetDataset(str(root))
    back = ds.read()
    assert back.num_rows == tbl.num_rows
    assert set(back.column_names) >= set(tbl.column_names)
