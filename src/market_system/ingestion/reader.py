# SCID + Depth -> Parquet (daily partitions) for ES
from __future__ import annotations

import re
import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.parquet as pq


# ---------------- Sierra time conversion ----------------

_SC_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_SC_TO_UNIX_US: int = int((_UNIX_EPOCH - _SC_EPOCH).total_seconds() * 1_000_000)


def _sc_us_to_unix_us(sc_us: int) -> int:
    # microseconds since 1899-12-30 → microseconds since 1970-01-01
    return sc_us - _SC_TO_UNIX_US


# ---------------- Contracts & filenames ----------------

MONTH_CODES = "FGHJKMNQUVXZ"
MONTH_ORDER = {m: i for i, m in enumerate(MONTH_CODES)}
# For ES we expect quarterly months only:
ES_QUARTERLY = set("HMUZ")


_CONTRACT_RE = re.compile(
    r"^(?P<root>[A-Z]+)(?P<mon>[FGHJKMNQUVXZ])(?P<yy>\d{2}).*?\.scid$", re.IGNORECASE
)


def _month_letter_for_es(mon: str) -> bool:
    return mon.upper() in ES_QUARTERLY


def _parse_contract_from_scid_name(name: str) -> Tuple[str, str, str]:
    """
    Extract (root, month_letter, yy) from a SCID filename (stem or full name).
    Example: ESU25_FUT_CME.scid -> ('ES', 'U', '25')
    """
    m = _CONTRACT_RE.match(name.upper())
    if not m:
        raise ValueError(f"Cannot parse contract from SCID name: {name}")
    return m.group("root"), m.group("mon"), m.group("yy")


def _choose_latest_contract(scid_files: List[Path], root: str) -> Path:
    """
    From a list of matching SCID files, choose the latest by (yy, month-order).
    For ES: only consider HMUZ (quarterlies).
    """
    scored: List[Tuple[int, int, Path]] = []
    for p in scid_files:
        try:
            r, mon, yy = _parse_contract_from_scid_name(p.name)
            if r != root.upper():
                continue
            if root.upper() == "ES" and not _month_letter_for_es(mon):
                continue
            scored.append((int(yy), MONTH_ORDER[mon.upper()], p))
        except Exception:
            continue

    if not scored:
        raise FileNotFoundError(
            f"No SCID files found for root={root} (quarterlies for ES)"
        )

    scored.sort()  # ascending by (yy, month)
    return scored[-1][2]


# ---------------- Binary formats (SCID & Depth) ----------------

# SCID header/record (docs: header=56 bytes, record=40 bytes)
_SCID_HDR_FMT = "<4sI I H H I 36s"  # 56 bytes
_SCID_HDR_SIZE = struct.calcsize(_SCID_HDR_FMT)
_SCID_REC_FMT = "<q f f f f I I I I"  # 40 bytes
_SCID_REC_SIZE = struct.calcsize(_SCID_REC_FMT)

# Depth header/record (docs: header=64 bytes, record=24 bytes)
_DEPTH_MAGIC = 0x44444353  # "SCDD"
_DEPTH_HDR_FMT = "<IIII48s"  # 64 bytes
_DEPTH_HDR_SIZE = struct.calcsize(_DEPTH_HDR_FMT)
_DEPTH_REC_FMT = "<q B B H f I I"  # 24 bytes
_DEPTH_REC_SIZE = struct.calcsize(_DEPTH_REC_FMT)


# ---------------- Config ----------------


@dataclass(frozen=True)
class IngestConfig:
    source: Path  # Data dir or a .scid file
    out_root: Path  # output root: e.g., ./data
    symbol: str = "ES"  # root like "ES"
    start: Optional[str] = None  # YYYY-MM-DD (UTC)
    end: Optional[str] = None  # YYYY-MM-DD (UTC)
    overwrite: bool = False
    # batch sizes / logging
    flush_rows: int = 500_000
    progress_every: int = 1_000_000


# ---------------- Ingestor ----------------


class ScidIngestor:
    """
    Ingest ES trades (.scid) and matching Depth files into Parquet.
    Layout:
      out_root/ES/<YY>/<M>/<YYYY-MM-DD>/{trades,depth}/ES<M><YY>.<YYYY-MM-DD>.parquet
    Only writes trades for days that have a corresponding depth file.
    """

    def ingest(self, cfg: IngestConfig) -> None:
        root = cfg.symbol.upper()
        scid_path, base_stem = self._resolve_scid(cfg.source, root)
        r, mon_letter, yy = _parse_contract_from_scid_name(scid_path.name)
        if r != root:
            # If you pointed at a specific file directly, we still use its parsed root.
            root = r

        # Discover depth files/dates for this contract
        depth_dir = self._resolve_depth_dir(cfg.source)
        depth_map = self._discover_depth_files(depth_dir, base_stem)
        depth_days: Set[str] = set(depth_map.keys())

        if cfg.start:
            start_d = datetime.fromisoformat(cfg.start).date()
            depth_days = {
                d for d in depth_days if datetime.fromisoformat(d).date() >= start_d
            }
        if cfg.end:
            end_d = datetime.fromisoformat(cfg.end).date()
            depth_days = {
                d for d in depth_days if datetime.fromisoformat(d).date() <= end_d
            }

        print(
            f"[ingest] contract={root}{mon_letter}{yy}  depth-days={len(depth_days)}  base={base_stem}"
        )

        # Optional: clear output for this contract if requested
        if cfg.overwrite:
            self._wipe_contract_output(cfg.out_root, root, yy, mon_letter)

        # 1) Ingest Depth files first (so their days exist)
        self._ingest_depth_files(
            depth_map=depth_map,
            allowed_days=depth_days,
            out_root=cfg.out_root,
            root=root,
            yy=yy,
            mon=mon_letter,
            progress=True,
        )

        # 2) Ingest Trades from SCID, writing ONLY for days in depth_days
        self._ingest_scid_trades(
            scid_path=scid_path,
            out_root=cfg.out_root,
            root=root,
            yy=yy,
            mon=mon_letter,
            allowed_days=depth_days,
            start=cfg.start,
            end=cfg.end,
            flush_rows=cfg.flush_rows,
            progress_every=cfg.progress_every,
        )

        print(f"[ingest] DONE  contract={root}{mon_letter}{yy}  out={cfg.out_root}")

    # --------- SCID Trades ---------

    def _ingest_scid_trades(
        self,
        *,
        scid_path: Path,
        out_root: Path,
        root: str,
        yy: str,
        mon: str,
        allowed_days: Set[str],
        start: Optional[str],
        end: Optional[str],
        flush_rows: int,
        progress_every: int,
    ) -> None:
        if not allowed_days:
            print("[trades] skipped (no depth days present)")
            return

        start_date = datetime.fromisoformat(start).date() if start else None
        end_date = datetime.fromisoformat(end).date() if end else None

        with open(scid_path, "rb") as f:
            self._read_scid_header(f)

            # buffers keyed per-day to write a single Parquet file per day
            day_bufs: Dict[str, Dict[str, list]] = {}
            total = 0
            written_days = 0

            block_sz = 65536 * _SCID_REC_SIZE  # bigger blocks for speed
            while True:
                chunk = f.read(block_sz)
                if not chunk:
                    break
                # trim any trailing partial record
                rem = len(chunk) % _SCID_REC_SIZE
                if rem:
                    chunk = chunk[: len(chunk) - rem]

                for rec in struct.iter_unpack(_SCID_REC_FMT, chunk):
                    (
                        sc_us,
                        open_p,
                        high_p,
                        low_p,
                        close_p,
                        num_trades,
                        total_vol,
                        bid_vol,
                        ask_vol,
                    ) = rec

                    unix_us = _sc_us_to_unix_us(sc_us)
                    dts = datetime.utcfromtimestamp(unix_us / 1_000_000)
                    d = dts.date()

                    if start_date and d < start_date:
                        continue
                    if end_date and d > end_date:
                        continue

                    date_str = d.isoformat()
                    if date_str not in allowed_days:
                        continue  # ONLY write days that have depth

                    buf = day_bufs.get(date_str)
                    if buf is None:
                        buf = {
                            "ts": [],
                            "open": [],
                            "high": [],
                            "low": [],
                            "close": [],
                            "num_trades": [],
                            "total_volume": [],
                            "bid_volume": [],
                            "ask_volume": [],
                        }
                        day_bufs[date_str] = buf

                    buf["ts"].append(unix_us)
                    buf["open"].append(float(open_p))
                    buf["high"].append(float(high_p))
                    buf["low"].append(float(low_p))
                    buf["close"].append(float(close_p))
                    buf["num_trades"].append(int(num_trades))
                    buf["total_volume"].append(int(total_vol))
                    buf["bid_volume"].append(int(bid_vol))
                    buf["ask_volume"].append(int(ask_vol))

                    total += 1
                    if total % progress_every == 0:
                        print(f"[trades] read {total:,} records ... last={date_str}")

                    # opportunistic flush if buffers get large
                    if sum(len(v["ts"]) for v in day_bufs.values()) >= flush_rows:
                        written_days += self._flush_trade_days(
                            out_root, root, yy, mon, day_bufs
                        )

            # final flush
            written_days += self._flush_trade_days(out_root, root, yy, mon, day_bufs)

        print(f"[trades] wrote {written_days} day files (records read: {total:,})")

    def _flush_trade_days(
        self,
        out_root: Path,
        root: str,
        yy: str,
        mon: str,
        day_bufs: Dict[str, Dict[str, list]],
    ) -> int:
        written = 0
        for date_str, cols in list(day_bufs.items()):
            if not cols["ts"]:
                continue
            tbl = pa.table(
                {
                    "ts": pa.array(cols["ts"], type=pa.timestamp("us")),
                    "open": pa.array(cols["open"], type=pa.float32()),
                    "high": pa.array(cols["high"], type=pa.float32()),
                    "low": pa.array(cols["low"], type=pa.float32()),
                    "close": pa.array(cols["close"], type=pa.float32()),
                    "num_trades": pa.array(cols["num_trades"], type=pa.uint32()),
                    "total_volume": pa.array(cols["total_volume"], type=pa.uint32()),
                    "bid_volume": pa.array(cols["bid_volume"], type=pa.uint32()),
                    "ask_volume": pa.array(cols["ask_volume"], type=pa.uint32()),
                }
            )
            out_dir = out_root / root / yy / mon / date_str / "trades"
            out_dir.mkdir(parents=True, exist_ok=True)
            fname = f"{root}{mon}{yy}.{date_str}.parquet"
            pq.write_table(
                tbl,
                out_dir / fname,
                compression="zstd",
                coerce_timestamps="us",
            )
            written += 1
            del day_bufs[date_str]  # free
            print(f"[trades] wrote {yy}/{mon}/{date_str} ({len(cols['ts']):,} rows)")
        return written

    # --------- Depth ---------

    def _ingest_depth_files(
        self,
        *,
        depth_map: Dict[str, Path],
        allowed_days: Set[str],
        out_root: Path,
        root: str,
        yy: str,
        mon: str,
        progress: bool = True,
    ) -> None:
        if not depth_map:
            print("[depth] none found — skipping")
            return

        # Only process allowed_days subset
        days = sorted(d for d in depth_map.keys() if d in allowed_days)
        if not days:
            print("[depth] no matching days in selected range — skipping")
            return

        total_files = len(days)
        for i, day in enumerate(days, 1):
            path = depth_map[day]
            rows = self._read_write_depth_day(path, out_root, root, yy, mon, day)
            if progress:
                print(f"[depth] ({i}/{total_files}) {yy}/{mon}/{day} -> {rows:,} rows")

    def _read_write_depth_day(
        self,
        path: Path,
        out_root: Path,
        root: str,
        yy: str,
        mon: str,
        date_str: str,
    ) -> int:
        with open(path, "rb") as f:
            self._read_depth_header(f)

            # read whole file by blocks; but most depth files are per-day
            block_sz = 65536 * _DEPTH_REC_SIZE
            col_ts: List[int] = []
            col_cmd: List[int] = []
            col_flags: List[int] = []
            col_orders: List[int] = []
            col_price: List[float] = []
            col_qty: List[int] = []

            while True:
                chunk = f.read(block_sz)
                if not chunk:
                    break
                rem = len(chunk) % _DEPTH_REC_SIZE
                if rem:
                    chunk = chunk[: len(chunk) - rem]
                for rec in struct.iter_unpack(_DEPTH_REC_FMT, chunk):
                    sc_us, cmd, flags, num_orders, price, qty, _reserved = rec
                    unix_us = _sc_us_to_unix_us(sc_us)
                    # We trust filename date → still keep all records; no filtering here
                    col_ts.append(unix_us)
                    col_cmd.append(int(cmd))
                    col_flags.append(int(flags))
                    col_orders.append(int(num_orders))
                    col_price.append(float(price))
                    col_qty.append(int(qty))

        tbl = pa.table(
            {
                "ts": pa.array(col_ts, type=pa.timestamp("us")),
                "command": pa.array(col_cmd, type=pa.uint8()),
                "flags": pa.array(col_flags, type=pa.uint8()),
                "num_orders": pa.array(col_orders, type=pa.uint16()),
                "price": pa.array(col_price, type=pa.float32()),
                "quantity": pa.array(col_qty, type=pa.uint32()),
            }
        )
        out_dir = out_root / root / yy / mon / date_str / "depth"
        out_dir.mkdir(parents=True, exist_ok=True)
        fname = f"{root}{mon}{yy}.{date_str}.parquet"
        pq.write_table(
            tbl,
            out_dir / fname,
            compression="zstd",
            coerce_timestamps="us",
        )
        return tbl.num_rows

    # --------- Discovery & I/O helpers ---------

    def _resolve_scid(self, source: Path, root: str) -> Tuple[Path, str]:
        """
        Return (scid_path, base_stem). If source is a file, we use it.
        If source is a dir, we search for the latest contract for the root.
        """
        if source.is_file() and source.suffix.lower() == ".scid":
            return source, source.stem

        if not source.is_dir():
            raise FileNotFoundError(f"Source not found: {source}")

        # search for SCID files for this root
        # We allow any suffix after the contract code, e.g. ESU25_FUT_CME.scid
        candidates = list(source.glob(f"{root.upper()}[A-Z][0-9][0-9]*.scid"))
        if not candidates:
            raise FileNotFoundError(f"No SCID files for root={root} under {source}")

        chosen = _choose_latest_contract(candidates, root.upper())
        return chosen, chosen.stem

    def _resolve_depth_dir(self, source: Path) -> Path:
        if source.is_file():
            # Common layout: SCID file is in Data/, depth under Data/MarketDepthData
            depth_dir = source.parent / "MarketDepthData"
        else:
            depth_dir = source / "MarketDepthData"
        return depth_dir

    def _discover_depth_files(self, depth_dir: Path, base_stem: str) -> Dict[str, Path]:
        """
        Find depth day files for this contract. We accept any filename
        that contains the contract stem and a YYYY-MM-DD date. If the date
        is not in the name, we fall back to peeking the first record.
        Returns: { 'YYYY-MM-DD': Path }
        """
        if not depth_dir.exists():
            return {}

        depth_files = [
            p for p in depth_dir.iterdir() if p.is_file() and base_stem in p.name
        ]
        day_map: Dict[str, Path] = {}

        date_re = re.compile(r"(\d{4}-\d{2}-\d{2})")
        for p in depth_files:
            m = date_re.search(p.name)
            if m:
                day_map[m.group(1)] = p
                continue

            # Fallback: read header + first record to infer day
            try:
                with open(p, "rb") as f:
                    self._read_depth_header(f)
                    first = f.read(_DEPTH_REC_SIZE)
                    if len(first) == _DEPTH_REC_SIZE:
                        (sc_us, *_rest) = struct.unpack(_DEPTH_REC_FMT, first)
                        unix_us = _sc_us_to_unix_us(sc_us)
                        d = (
                            datetime.utcfromtimestamp(unix_us / 1_000_000)
                            .date()
                            .isoformat()
                        )
                        day_map[d] = p
            except Exception:
                # Ignore non-depth or unreadable files
                pass

        return day_map

    def _wipe_contract_output(
        self, out_root: Path, root: str, yy: str, mon: str
    ) -> None:
        base = out_root / root / yy / mon
        if not base.exists():
            return
        # Remove **only** this contract-month tree
        for p in sorted(base.rglob("*"), reverse=True):
            try:
                if p.is_file():
                    p.unlink()
                else:
                    p.rmdir()
            except OSError:
                pass
        try:
            base.rmdir()
        except OSError:
            pass

    # ---- Header validators ----

    def _read_scid_header(self, f) -> None:
        hdr = f.read(_SCID_HDR_SIZE)
        if len(hdr) != _SCID_HDR_SIZE:
            raise ValueError("SCID header too short")
        file_id, hdr_size, rec_size, version, _unused, _utc_idx, _reserve = (
            struct.unpack(_SCID_HDR_FMT, hdr)
        )
        if file_id != b"SCID":
            raise ValueError("Not a SCID file (missing 'SCID' magic)")
        if hdr_size != _SCID_HDR_SIZE:
            raise ValueError(f"Unexpected SCID header size: {hdr_size}")
        if rec_size != _SCID_REC_SIZE:
            raise ValueError(f"Unexpected SCID record size: {rec_size} (expected 40)")
        # version currently 1; accept any

    def _read_depth_header(self, f) -> None:
        hdr = f.read(_DEPTH_HDR_SIZE)
        if len(hdr) < 16:
            raise ValueError("Depth header too short")
        (magic, hdr_size, rec_size, version, _reserve) = struct.unpack(
            _DEPTH_HDR_FMT, hdr
        )
        if magic != _DEPTH_MAGIC:
            raise ValueError("Not a Depth file (missing 'SCDD' magic)")
        if hdr_size != _DEPTH_HDR_SIZE:
            raise ValueError(f"Unexpected Depth header size: {hdr_size}")
        if rec_size != _DEPTH_REC_SIZE:
            raise ValueError(f"Unexpected Depth record size: {rec_size} (expected 24)")
        # version 1 per docs; accept any
