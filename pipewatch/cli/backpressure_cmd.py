"""CLI subcommand: pipewatch backpressure

Detects pipelines whose failure rate is consistently rising,
indicating upstream backpressure.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.export.backpressure import compute_backpressure, format_backpressure
from pipewatch.export.history import load_history


def add_backpressure_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    p = subparsers.add_parser(
        "backpressure",
        help="Detect pipelines with a consistently rising failure rate.",
    )
    p.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        metavar="FILE",
        help="Path to the history JSONL file (default: pipewatch_history.jsonl).",
    )
    p.add_argument(
        "--window",
        type=int,
        default=10,
        metavar="N",
        help="Number of recent history entries to analyse (default: 10).",
    )
    p.add_argument(
        "--min-slope",
        type=float,
        default=0.02,
        metavar="SLOPE",
        help="Minimum per-period failure-rate increase to flag (default: 0.02).",
    )
    p.add_argument(
        "--min-periods",
        type=int,
        default=3,
        metavar="N",
        help="Minimum periods required to evaluate a pipeline (default: 3).",
    )
    p.set_defaults(func=run_backpressure_cmd)


def run_backpressure_cmd(args: argparse.Namespace) -> None:
    history_path = Path(args.history)
    history = load_history(history_path) if history_path.exists() else []
    report = compute_backpressure(
        history,
        window=args.window,
        min_slope=args.min_slope,
        min_periods=args.min_periods,
    )
    print(format_backpressure(report))
