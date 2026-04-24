"""CLI sub-command: pipewatch drift"""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.export.drift import compute_drift, format_drift
from pipewatch.export.history import load_history


def add_drift_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    p = subparsers.add_parser(
        "drift",
        help="Detect pipelines whose failure rate is drifting upward.",
    )
    p.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        help="Path to history file (default: pipewatch_history.jsonl)",
    )
    p.add_argument(
        "--baseline-window",
        type=int,
        default=7,
        dest="baseline_window",
        help="Number of historical entries used as baseline (default: 7)",
    )
    p.add_argument(
        "--recent-window",
        type=int,
        default=3,
        dest="recent_window",
        help="Number of recent entries to compare against baseline (default: 3)",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=0.10,
        help="Minimum failure-rate delta to flag as drifting (default: 0.10)",
    )
    p.set_defaults(func=run_drift_cmd)


def run_drift_cmd(args: argparse.Namespace) -> None:
    history = load_history(Path(args.history))
    report = compute_drift(
        history,
        baseline_window=args.baseline_window,
        recent_window=args.recent_window,
        threshold=args.threshold,
    )
    print(format_drift(report))
