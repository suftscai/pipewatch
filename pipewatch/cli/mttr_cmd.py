"""CLI subcommand for MTTR report."""
from __future__ import annotations
import argparse
from pipewatch.export.history import load_history
from pipewatch.export.mttr import compute_mttr, format_mttr


def add_mttr_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("mttr", help="Show mean time to recovery per pipeline")
    p.add_argument("history_file", help="Path to history JSONL file")
    p.add_argument(
        "--window",
        type=int,
        default=24,
        help="Number of recent snapshots to analyse (default: 24)",
    )


def run_mttr_cmd(args: argparse.Namespace) -> None:
    history = load_history(args.history_file)
    report = compute_mttr(history, window=args.window)
    print(format_mttr(report))
