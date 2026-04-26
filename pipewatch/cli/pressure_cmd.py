"""CLI subcommand: pipewatch pressure — show pipelines under sustained error pressure."""
from __future__ import annotations

import argparse

from pipewatch.export.history import load_history
from pipewatch.export.pressure import compute_pressure, format_pressure


def add_pressure_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "pressure",
        help="Detect pipelines under sustained high error pressure.",
    )
    parser.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        help="Path to history file (default: pipewatch_history.jsonl)",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=24,
        help="Number of recent history entries to consider (default: 24)",
    )
    parser.add_argument(
        "--rate-threshold",
        type=float,
        default=0.3,
        help="Failure rate threshold per period (default: 0.30)",
    )
    parser.add_argument(
        "--pressure-ratio",
        type=float,
        default=0.5,
        help="Fraction of periods above threshold to flag as pressure (default: 0.50)",
    )
    parser.add_argument(
        "--min-periods",
        type=int,
        default=3,
        help="Minimum periods required to evaluate a pipeline (default: 3)",
    )
    parser.set_defaults(func=run_pressure_cmd)


def run_pressure_cmd(args: argparse.Namespace) -> None:
    history = load_history(args.history)
    report = compute_pressure(
        history,
        window=args.window,
        rate_threshold=args.rate_threshold,
        min_periods=args.min_periods,
        pressure_ratio=args.pressure_ratio,
    )
    print(format_pressure(report))
    if report.has_pressure():
        print(f"\n{len(report.pressured())} pipeline(s) under pressure.")
    else:
        print("\nAll pipelines within acceptable pressure levels.")
