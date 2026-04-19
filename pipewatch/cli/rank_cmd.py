"""CLI subcommand: rank pipelines by failure rate."""
from __future__ import annotations
import argparse
from pipewatch.export.history import load_history
from pipewatch.export.pipeline_rank import compute_rank, format_rank


def add_rank_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "rank",
        help="Rank pipelines by failure rate over recent history.",
    )
    p.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        help="Path to history file (default: pipewatch_history.jsonl)",
    )
    p.add_argument(
        "--window",
        type=int,
        default=20,
        help="Number of recent history entries to consider (default: 20)",
    )


def run_rank_cmd(args: argparse.Namespace) -> None:
    history = load_history(args.history)
    report = compute_rank(history, window=args.window)
    print(format_rank(report))
