"""CLI subcommand for error message frequency map."""
from __future__ import annotations
import argparse
from pipewatch.export.history import load_history
from pipewatch.export.errormap import compute_errormap, format_errormap


def add_errormap_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("errormap", help="Show error message frequency map")
    p.add_argument("--history", default="pipewatch_history.json", help="History file path")
    p.add_argument("--window", type=int, default=50, help="Number of recent entries to consider")
    p.add_argument("--top", type=int, default=10, help="Number of top errors to display")


def run_errormap_cmd(args: argparse.Namespace) -> None:
    history = load_history(args.history)
    report = compute_errormap(history, window=args.window, top_n=args.top)
    print(format_errormap(report))
