import argparse
import sys


def cmd_ingest(args: argparse.Namespace) -> int:
    print(f"[ingest] source={args.source}")
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


def cmd_exec(args: argparse.Namespace) -> int:
    print(f"[exec] strategy={args.strategy} dry_run={args.dry_run}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="market-system")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("ingest", help="ingest raw sources (SCID/depth/etc.)")
    sp.add_argument("source", help="path to raw source (file or dir)")
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
