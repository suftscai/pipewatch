"""CLI sub-command: pipewatch congestion."""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.export.history import load_history
from pipewatch.export.congestion import compute_congestion, format_congestion


def add_congestion_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    p = subparsers.add_parser(
        "congestion",
        help="Detect pipelines with sustained high event volume.",
    )
    p.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        help="Path to history file (default: pipewatch_history.jsonl).",
    )
    p.add_argument(
        "--window",
        type=int,
        default=24,
        help="Number of recent history entries to analyse (default: 24).",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=100.0,
        help="Average events-per-entry above which a pipeline is congested (default: 100).",
    )
    p.add_argument(
        "--min-entries",
        type=int,
        default=3,
        help="Minimum entries required before flagging a pipeline (default: 3).",
    )
    p.set_defaults(func=run_congestion_cmd)


def run_congestion_cmd(args: argparse.Namespace) -> None:
    history_path = Path(args.history)
    history = load_history(history_path) if history_path.exists() else []
    report = compute_congestion(
        history,
        window=args.window,
        threshold=args.threshold,
        min_entries=args.min_entries,
    )
    print(format_congestion(report))
    if report.has_congestion():
        print(f"  {len(report.congested())} congested pipeline(s) detected.")
