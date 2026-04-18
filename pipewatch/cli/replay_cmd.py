"""CLI subcommand: replay — show historical pipeline event replay."""
from __future__ import annotations

import argparse

from pipewatch.export.replay import compute_replay, format_replay


def add_replay_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("replay", help="Replay historical pipeline snapshots")
    p.add_argument("history", help="Path to history JSONL file")
    p.add_argument(
        "--window",
        type=int,
        default=10,
        help="Number of recent entries to replay (default: 10)",
    )


def run_replay_cmd(args: argparse.Namespace) -> None:
    report = compute_replay(args.history, window=args.window)
    print(format_replay(report))
