from __future__ import annotations

import struct
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from market_system.ingestion.reader import ScidIngestor, IngestConfig

_HDR_FMT = "<4sI I H H I 36s"
_REC_FMT = "<q ffff I I I I"

_HDR_SIZE = struct.calcsize(_HDR_FMT)
_REC_SIZE = struct.calcsize(_REC_FMT)

_SC_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_SC_TO_UNIX_US = int((_UNIX_EPOCH - _SC_EPOCH).total_seconds() * 1_000_000)


def _unix_to_sc_us(dt: datetime) -> int:
    unix_us = int(dt.timestamp() * 1_000_000)
    return unix_us + _SC_TO_UNIX_US


def _write_scid(path: Path, rows: list[tuple]) -> None:
    # header
    hdr = struct.pack(_HDR_FMT, b"SCID", _HDR_SIZE, _REC_SIZE, 1, 0, 0, b"\x00" * 36)
    with open(path, "wb") as f:
        f.write(hdr)
        for r in rows:
            f.write(struct.pack(_REC_FMT, *r))


def test_scid_to_parquet_daily(tmp_path: Path) -> None:
    sym = "ESU25_FUT_CME"
    scid = tmp_path / f"{sym}.scid"

    # two days, three ticks
    d0 = datetime(2025, 8, 26, 23, 59, 59, tzinfo=timezone.utc)
    d1 = d0 + timedelta(seconds=2)  # next day UTC
    rows = [
        # sc_us, open, high(ask), low(bid), close(price), numTrades, totVol, bidVol, askVol
        (_unix_to_sc_us(d0), 0.0, 100.25, 100.00, 100.10, 1, 1, 0, 1),
        (_unix_to_sc_us(d0), 0.0, 100.30, 100.05, 100.20, 1, 2, 2, 0),
        (_unix_to_sc_us(d1), 0.0, 100.40, 100.10, 100.30, 1, 1, 0, 1),
    ]
    _write_scid(scid, rows)

    out_root = tmp_path / "out"
    ScidIngestor().ingest(
        IngestConfig(source=scid, out_root=out_root, symbol=sym, overwrite=True)
    )

    root = out_root / "trades"
    # partition dirs exist
    assert (root / f"symbol={sym}").exists()
    # both dates present
    dates = sorted([p.name for p in (root / f"symbol={sym}").glob("date=*")])
    assert len(dates) == 2

    # Read dataset
    ds = pq.ParquetDataset(str(root))
    tbl = ds.read()
    assert tbl.num_rows == 3
    assert set(tbl.column_names) >= {"ts", "price", "qty", "symbol", "date"}
