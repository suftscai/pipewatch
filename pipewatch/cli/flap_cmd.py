"""CLI subcommand: pipewatch flap — detect pipelines flapping between states."""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.export.flap import compute_flap, format_flap
from pipewatch.export.history import load_history


def add_flap_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    parser = subparsers.add_parser(
        "flap",
        help="Detect pipelines that repeatedly flip between healthy and failing.",
    )
    parser.add_argument(
        "--history",
        default="pipewatch_history.jsonl",
        help="Path to history file (default: pipewatch_history.jsonl)",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=20,
        help="Number of recent history entries to consider (default: 20)",
    )
    parser.add_argument(
        "--min-transitions",
        type=int,
        default=3,
        dest="min_transitions",
        help="Minimum state transitions to flag a pipeline (default: 3)",
    )
    parser.add_argument(
        "--min-flap-rate",
        type=float,
        default=0.4,
        dest="min_flap_rate",
        help="Minimum flap rate (transitions / periods-1) to flag (default: 0.4)",
    )
    parser.set_defaults(func=run_flap_cmd)


def run_flap_cmd(args: argparse.Namespace) -> None:
    history_path = Path(args.history)
    if not history_path.exists():
        print("No history file found.")
        return

    entries = load_history(history_path)
    report = compute_flap(
        entries,
        window=args.window,
        min_transitions=args.min_transitions,
        min_flap_rate=args.min_flap_rate,
    )
    print(format_flap(report))
