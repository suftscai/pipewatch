"""CLI subcommand: pipewatch saturation — report pipelines nearing error capacity."""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.export.history import load_history
from pipewatch.export.saturation import compute_saturation, format_saturation


def add_saturation_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    parser = subparsers.add_parser(
        "saturation",
        help="Detect pipelines whose error volume is near or above a high-water mark.",
    )
    parser.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        help="Path to history file (default: pipewatch_history.jsonl).",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=10,
        help="Number of recent history entries to consider (default: 10).",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=50,
        help="Absolute error count high-water mark (default: 50).",
    )
    parser.add_argument(
        "--saturation-rate",
        type=float,
        default=0.8,
        dest="saturation_rate",
        help="Fraction of threshold that triggers saturation (default: 0.8).",
    )
    parser.set_defaults(func=run_saturation_cmd)


def run_saturation_cmd(args: argparse.Namespace) -> None:
    history_path = Path(args.history)
    if not history_path.exists():
        print("No history file found.")
        return

    history = load_history(history_path)
    report = compute_saturation(
        history,
        window=args.window,
        threshold=args.threshold,
        saturation_rate=args.saturation_rate,
    )
    print(format_saturation(report))
