"""CLI subcommand: quota — report pipelines exceeding their error quota."""
from __future__ import annotations

import argparse
import sys

from pipewatch.export.history import load_history
from pipewatch.export.quota import compute_quota, format_quota


def add_quota_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser(
        "quota",
        help="Show pipelines that have exceeded their allowed error quota.",
    )
    p.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        help="Path to history file (default: pipewatch_history.jsonl).",
    )
    p.add_argument(
        "--quota",
        type=int,
        default=100,
        help="Maximum allowed errors per pipeline (default: 100).",
    )
    p.add_argument(
        "--window",
        type=int,
        default=24,
        help="Number of history entries to consider (default: 24).",
    )


def run_quota_cmd(args: argparse.Namespace) -> None:
    history = load_history(args.history)
    report = compute_quota(history, quota=args.quota, window=args.window)
    print(format_quota(report))
    if not report.compliant:
        sys.exit(1)
