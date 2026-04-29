"""CLI sub-command: pipewatch regression."""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.export.history import load_history
from pipewatch.export.regression import compute_regression, format_regression


def add_regression_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    p = subparsers.add_parser(
        "regression",
        help="Detect failure-rate regressions between baseline and recent history.",
    )
    p.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        help="Path to history file (default: pipewatch_history.jsonl).",
    )
    p.add_argument(
        "--baseline-window",
        type=int,
        default=10,
        dest="baseline_window",
        help="Number of history entries used as baseline (default: 10).",
    )
    p.add_argument(
        "--current-window",
        type=int,
        default=5,
        dest="current_window",
        help="Number of recent history entries to compare (default: 5).",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=0.10,
        help="Minimum failure-rate delta to flag as regression (default: 0.10).",
    )
    p.set_defaults(func=run_regression_cmd)


def run_regression_cmd(args: argparse.Namespace) -> None:
    history = load_history(Path(args.history))
    if not history:
        print("No history data found.")
        return

    report = compute_regression(
        history,
        baseline_window=args.baseline_window,
        current_window=args.current_window,
        threshold=args.threshold,
    )
    print(format_regression(report))
