"""CLI subcommand: watchdog — report silent pipelines."""
from __future__ import annotations

import argparse
from datetime import datetime

from pipewatch.export.history import load_history
from pipewatch.export.watchdog import detect_silent, format_watchdog


def add_watchdog_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("watchdog", help="Detect pipelines that have gone silent")
    p.add_argument("--history", default="pipewatch_history.jsonl", help="Path to history file")
    p.add_argument(
        "--threshold",
        type=float,
        default=300.0,
        help="Seconds of silence before a pipeline is flagged (default: 300)",
    )
    p.add_argument(
        "--window",
        type=int,
        default=100,
        help="Number of recent history entries to consider (default: 100)",
    )


def run_watchdog_cmd(args: argparse.Namespace) -> None:
    entries = load_history(args.history)
    recent = entries[-args.window:] if len(entries) > args.window else entries
    report = detect_silent(recent, threshold_seconds=args.threshold, now=datetime.utcnow())
    print(format_watchdog(report))
    if report.has_silent:
        raise SystemExit(1)
