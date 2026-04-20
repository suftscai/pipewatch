"""CLI subcommand: bottleneck — detect persistently failing pipelines."""
from __future__ import annotations

import argparse

from pipewatch.export.bottleneck import compute_bottleneck, format_bottleneck
from pipewatch.export.history import load_history


def add_bottleneck_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "bottleneck",
        help="Detect pipelines with persistently high failure rates.",
    )
    parser.add_argument(
        "history_file",
        help="Path to the JSON history file produced by pipewatch.",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=10,
        help="Number of recent history entries to analyse (default: 10).",
    )
    parser.add_argument(
        "--min-occurrences",
        type=int,
        default=2,
        dest="min_occurrences",
        help="Minimum windows with errors to flag a pipeline (default: 2).",
    )
    parser.add_argument(
        "--min-rate",
        type=float,
        default=0.1,
        dest="min_rate",
        help="Minimum failure rate (0–1) to flag a pipeline (default: 0.1).",
    )


def run_bottleneck_cmd(args: argparse.Namespace) -> None:
    history = load_history(args.history_file)
    report = compute_bottleneck(
        history,
        window=args.window,
        min_occurrences=args.min_occurrences,
        min_failure_rate=args.min_rate,
    )
    print(format_bottleneck(report))
