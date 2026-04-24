"""CLI subcommand: pipewatch deadletter — show persistently failing pipelines."""
from __future__ import annotations

import argparse

from pipewatch.export.history import load_history
from pipewatch.export.deadletter import compute_deadletter, format_deadletter


def add_deadletter_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    parser = subparsers.add_parser(
        "deadletter",
        help="Identify pipelines stuck in a persistent failure streak.",
    )
    parser.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        help="Path to history file (default: pipewatch_history.jsonl).",
    )
    parser.add_argument(
        "--min-consecutive",
        type=int,
        default=3,
        dest="min_consecutive",
        help="Minimum consecutive failing entries to flag (default: 3).",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=20,
        help="Number of recent history entries to consider (default: 20).",
    )
    parser.set_defaults(func=run_deadletter_cmd)


def run_deadletter_cmd(args: argparse.Namespace) -> None:
    history = load_history(args.history)
    if not history:
        print("No history data found.")
        return

    report = compute_deadletter(
        history,
        min_consecutive=args.min_consecutive,
        window=args.window,
    )
    print(format_deadletter(report))

    if report.has_dead_letters():
        flagged = report.flagged()
        print(f"\n{len(flagged)} pipeline(s) flagged as dead-letter.")
    else:
        print("\nNo dead-letter pipelines detected.")
