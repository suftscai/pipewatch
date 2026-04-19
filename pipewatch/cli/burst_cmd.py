"""CLI subcommand: detect error bursts in pipeline history."""
from __future__ import annotations
import argparse
from pipewatch.export.history import load_history
from pipewatch.export.burst import compute_burst, format_burst


def add_burst_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("burst", help="Detect error burst windows in history")
    p.add_argument("history_file", help="Path to history JSON file")
    p.add_argument(
        "--window", type=int, default=24, help="Number of recent entries to consider"
    )
    p.add_argument(
        "--min-rate", type=float, default=0.5, help="Minimum error rate to flag (0-1)"
    )
    p.add_argument(
        "--min-errors", type=int, default=3, help="Minimum absolute errors to flag"
    )


def run_burst_cmd(args: argparse.Namespace) -> None:
    history = load_history(args.history_file)
    report = compute_burst(
        history,
        window=args.window,
        min_rate=args.min_rate,
        min_errors=args.min_errors,
    )
    print(format_burst(report))
