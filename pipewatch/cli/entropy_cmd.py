"""CLI subcommand: pipewatch entropy — show failure pattern entropy per pipeline."""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.export.history import load_history
from pipewatch.export.entropy import compute_entropy, format_entropy


def add_entropy_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    parser = subparsers.add_parser(
        "entropy",
        help="Measure unpredictability of failure patterns per pipeline.",
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
        default=24,
        metavar="N",
        help="Number of recent history entries to analyse (default: 24).",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=2.5,
        metavar="FLOAT",
        help="Shannon entropy threshold above which a pipeline is flagged (default: 2.5).",
    )
    parser.add_argument(
        "--min-periods",
        type=int,
        default=4,
        dest="min_periods",
        metavar="N",
        help="Minimum number of periods required to evaluate a pipeline (default: 4).",
    )
    parser.set_defaults(func=run_entropy_cmd)


def run_entropy_cmd(args: argparse.Namespace) -> None:
    history_path = Path(args.history)
    if not history_path.exists():
        print("No history file found. Run pipewatch first to collect data.")
        return

    history = load_history(history_path)
    report = compute_entropy(
        history,
        window=args.window,
        threshold=args.threshold,
        min_periods=args.min_periods,
    )
    print(format_entropy(report))
    if report.has_chaos():
        chaotic = report.chaotic()
        print(f"\n  {len(chaotic)} pipeline(s) show chaotic failure patterns.")
    else:
        print("\n  All pipelines show stable or predictable failure patterns.")
