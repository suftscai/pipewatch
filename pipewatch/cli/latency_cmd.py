"""CLI subcommand: latency — show inter-event latency per pipeline."""
from __future__ import annotations

import argparse

from pipewatch.export.history import load_history
from pipewatch.export.latency import compute_latency, format_latency


def add_latency_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "latency",
        help="Show average time between events per pipeline.",
    )
    parser.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        metavar="FILE",
        help="Path to history file (default: pipewatch_history.jsonl).",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=50,
        metavar="N",
        help="Number of most-recent history entries to consider (default: 50).",
    )
    parser.set_defaults(func=run_latency_cmd)


def run_latency_cmd(args: argparse.Namespace) -> None:
    entries = load_history(args.history)
    report = compute_latency(entries, window=args.window)
    print(format_latency(report))
