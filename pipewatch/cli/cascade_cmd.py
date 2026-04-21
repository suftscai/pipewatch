"""CLI subcommand: detect cascade failures across pipelines."""
from __future__ import annotations

import argparse

from pipewatch.export.history import load_history
from pipewatch.export.cascade import compute_cascade, format_cascade


def add_cascade_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser(
        "cascade",
        help="Detect cascade failures: multiple pipelines failing in the same time window.",
    )
    p.add_argument("history_file", help="Path to the history JSONL file.")
    p.add_argument(
        "--window",
        type=int,
        default=5,
        metavar="MINUTES",
        help="Time window in minutes to consider simultaneous (default: 5).",
    )
    p.add_argument(
        "--min-pipelines",
        type=int,
        default=2,
        metavar="N",
        help="Minimum distinct pipelines to flag a cascade (default: 2).",
    )
    p.add_argument(
        "--recent",
        type=int,
        default=50,
        metavar="N",
        help="Number of most-recent history entries to consider (default: 50).",
    )
    p.set_defaults(func=run_cascade_cmd)


def run_cascade_cmd(args: argparse.Namespace) -> None:
    history = load_history(args.history_file)
    report = compute_cascade(
        history,
        window_minutes=args.window,
        min_pipelines=args.min_pipelines,
        recent=args.recent,
    )
    print(format_cascade(report))
