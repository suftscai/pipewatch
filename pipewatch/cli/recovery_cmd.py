"""CLI subcommand: recovery — show pipeline recovery rates from history."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipewatch.export.history import load_history
from pipewatch.export.recovery import compute_recovery, format_recovery


def add_recovery_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "recovery",
        help="Show pipeline recovery rates from historical data.",
    )
    parser.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        metavar="FILE",
        help="Path to history file (default: pipewatch_history.jsonl).",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=50,
        metavar="N",
        help="Number of most-recent history entries to analyse (default: 50).",
    )
    parser.set_defaults(func=run_recovery_cmd)


def run_recovery_cmd(args: argparse.Namespace) -> None:
    history_path = Path(args.history)
    if not history_path.exists():
        print(f"[pipewatch] history file not found: {history_path}", file=sys.stderr)
        sys.exit(1)

    entries = load_history(history_path)
    report = compute_recovery(entries, window=args.window)
    print(format_recovery(report))
