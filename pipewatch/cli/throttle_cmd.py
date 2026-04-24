"""CLI sub-command: throttle — detect pipelines emitting events too rapidly."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipewatch.export.history import load_history
from pipewatch.export.throttle import compute_throttle, format_throttle


def add_throttle_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    parser = subparsers.add_parser(
        "throttle",
        help="Detect pipelines whose event rate exceeds a threshold.",
    )
    parser.add_argument(
        "history_file",
        help="Path to the pipewatch history JSON-lines file.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=500.0,
        metavar="N",
        help="Maximum events per hour before a pipeline is flagged (default: 500).",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=24,
        metavar="W",
        help="Number of most-recent history entries to consider (default: 24).",
    )
    parser.add_argument(
        "--only-throttled",
        action="store_true",
        help="Only print pipelines that are currently throttled.",
    )
    parser.set_defaults(func=run_throttle_cmd)


def run_throttle_cmd(args: argparse.Namespace) -> None:
    path = Path(args.history_file)
    if not path.exists():
        print(f"[throttle] history file not found: {path}", file=sys.stderr)
        sys.exit(1)

    history = load_history(path)
    report = compute_throttle(history, threshold=args.threshold, window=args.window)

    if args.only_throttled:
        from pipewatch.export.throttle import ThrottleReport

        report = ThrottleReport(results=report.throttled())

    print(format_throttle(report))

    if report.has_throttled():
        sys.exit(2)
