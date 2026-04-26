"""CLI subcommand: heartbeat — show pipelines that have gone silent."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pipewatch.export.history import load_history
from pipewatch.export.heartbeat import compute_heartbeat, format_heartbeat


def add_heartbeat_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "heartbeat",
        help="Detect pipelines that have stopped emitting events.",
    )
    p.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        help="Path to history file (default: pipewatch_history.jsonl).",
    )
    p.add_argument(
        "--interval",
        type=float,
        default=300.0,
        help="Expected heartbeat interval in seconds (default: 300).",
    )
    p.add_argument(
        "--window",
        type=int,
        default=50,
        help="Number of recent history entries to consider (default: 50).",
    )


def run_heartbeat_cmd(args: argparse.Namespace) -> None:
    entries = load_history(args.history)
    now = datetime.now(timezone.utc)
    report = compute_heartbeat(
        entries,
        expected_interval_s=args.interval,
        window=args.window,
        now=now,
    )
    print(format_heartbeat(report))
    if report.has_flatlines():
        print(f"\n  {len(report.flatlines())} pipeline(s) appear to have flatlined.")
