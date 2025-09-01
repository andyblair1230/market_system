import argparse
import sys
import shutil
import platform
from pathlib import Path

from market_system.ingestion.reader import ScidIngestor, IngestConfig


def cmd_ingest(args: argparse.Namespace) -> int:
    cfg = IngestConfig(
        source=Path(args.source),
        out_root=Path(args.out or "data"),
        symbol=args.symbol,  # root like "ES"
        start=args.start,
        end=args.end,
        overwrite=args.overwrite,
    )
    ScidIngestor().ingest(cfg)
    return 0


def cmd_align(args: argparse.Namespace) -> int:
    print(f"[align] trades={args.trades} depth={args.depth} out={args.out}")
    return 0


def cmd_store(args: argparse.Namespace) -> int:
    print(f"[store] table={args.table} parquet={args.parquet}")
    return 0


def cmd_replay(args: argparse.Namespace) -> int:
    print(f"[replay] aligned={args.aligned}")
    return 0


def cmd_viewer(args: argparse.Namespace) -> int:
    from market_system.viewer import ViewerApp

    app = ViewerApp()
    app.start()
    return 0


def _run(cmd: list[str]) -> tuple[int, str]:
    import subprocess as sp

    try:
        out = sp.check_output(cmd, stderr=sp.STDOUT, text=True)
        return 0, out.strip()
    except (OSError, sp.CalledProcessError) as e:
        msg = e.output.strip() if isinstance(e, sp.CalledProcessError) else str(e)
        return 1, msg


def cmd_doctor(_: argparse.Namespace) -> int:
    print("=== market_system environment check ===")
    print(f"OS: {platform.system()} {platform.release()} ({platform.version()})")
    print(f"Python: {sys.executable}")

    for name in ["pre-commit", "black", "ruff", "mypy", "pytest"]:
        path = shutil.which(name)
        print(f"{name:>10}: {'OK ' + path if path else 'NOT FOUND'}")

    cmake_path = shutil.which("cmake")
    if cmake_path:
        code, out = _run(["cmake", "--version"])
        print(f"{'cmake':>10}: OK {cmake_path}")
        if code == 0:
            first = out.splitlines()[0]
            print(f"{'':>12}{first}")
        else:
            print(f"{'':>12}Problem running cmake: {out}")
    else:
        print(f"{'cmake':>10}: NOT FOUND")

    if platform.system() == "Windows":
        cl_path = shutil.which("cl")
        if cl_path:
            code, out = _run(["cl"])
            banner = out.splitlines()[0] if out else "OK"
            print(f"{'cl':>10}: OK {cl_path}")
            print(f"{'':>12}{banner}")
        else:
            print(
                f"{'cl':>10}: NOT FOUND (open x64 Native Tools for VS 2022 or add MSVC to PATH)"
            )

    try:
        import PySide6  # noqa: F401
        import pyqtgraph  # noqa: F401

        print(f"{'viewer':>10}: PySide6 + pyqtgraph AVAILABLE")
    except Exception as e:  # noqa: BLE001
        print(f"{'viewer':>10}: MISSING ({e})")

    return 0


def cmd_exec(args: argparse.Namespace) -> int:
    print(f"[exec] strategy={args.strategy} dry_run={args.dry_run}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="market-system")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser(
        "ingest",
        help="Convert ES SCID + Depth to Parquet (daily partitions, only days with depth)",
    )
    sp.add_argument("source", help="Path to SierraChart Data dir or a .scid file")
    sp.add_argument(
        "--symbol",
        default="ES",
        help="Root symbol (e.g., ES). We auto-detect the active contract from filenames.",
    )
    sp.add_argument("--out", help="Output root for dataset (default: ./data)")
    sp.add_argument("--start", help="Start date (YYYY-MM-DD, UTC)")
    sp.add_argument("--end", help="End date (YYYY-MM-DD, UTC)")
    sp.add_argument(
        "--overwrite",
        action="store_true",
        help="Clear existing output for this symbol/contract before writing",
    )
    sp.set_defaults(func=cmd_ingest)

    sp = sub.add_parser("align", help="align trades and depth")
    sp.add_argument("trades", help="path to trades table (e.g., parquet)")
    sp.add_argument("depth", help="path to depth table (e.g., parquet)")
    sp.add_argument(
        "--out", required=False, help="output path for aligned table (parquet)"
    )
    sp.set_defaults(func=cmd_align)

    sp = sub.add_parser("store", help="persist a table to parquet")
    sp.add_argument("table", help="input table path")
    sp.add_argument("--parquet", required=True, help="destination parquet path")
    sp.set_defaults(func=cmd_store)

    sp = sub.add_parser("replay", help="replay an aligned dataset")
    sp.add_argument("aligned", help="path to aligned table (parquet)")
    sp.set_defaults(func=cmd_replay)

    sp = sub.add_parser("viewer", help="launch desktop viewer")
    sp.set_defaults(func=cmd_viewer)

    sp = sub.add_parser("doctor", help="environment sanity checks")
    sp.set_defaults(func=cmd_doctor)

    sp = sub.add_parser(
        "exec", help="connect execution adapter (Sierra) and run strategy"
    )
    sp.add_argument("strategy", help="strategy name or path")
    sp.add_argument("--dry-run", action="store_true")
    sp.set_defaults(func=cmd_exec)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    rc = args.func(args)
    sys.exit(rc)
