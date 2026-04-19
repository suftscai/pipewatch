"""CLI subcommand: pipewatch correlation."""
from __future__ import annotations
import argparse
from pipewatch.export.history import load_history
from pipewatch.export.correlation import compute_correlation, format_correlation


def add_correlation_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("correlation", help="Show failure-rate correlations between pipelines")
    p.add_argument("log", help="Path to pipeline log file")
    p.add_argument("--history", default="pipewatch_history.jsonl", help="History file path")
    p.add_argument("--window", type=int, default=50, help="Number of recent history entries to use")
    p.add_argument("--min-strength", choices=["none", "weak", "moderate", "strong"],
                   default="none", help="Minimum correlation strength to display")


_STRENGTH_ORDER = {"none": 0, "weak": 1, "moderate": 2, "strong": 3}


def run_correlation_cmd(args: argparse.Namespace) -> None:
    entries = load_history(args.history)
    report = compute_correlation(entries, window=args.window)

    min_level = _STRENGTH_ORDER[args.min_strength]
    report.pairs = [
        p for p in report.pairs
        if _STRENGTH_ORDER[p.strength] >= min_level
    ]

    print(format_correlation(report))
