"""CLI subcommand: pipewatch fatigue — detect alert-fatiguing pipelines."""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.export.fatigue import compute_fatigue, format_fatigue
from pipewatch.export.history import load_history


def add_fatigue_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    parser = subparsers.add_parser(
        "fatigue",
        help="Detect pipelines generating excessive low-severity noise.",
    )
    parser.add_argument(
        "history_file",
        type=Path,
        help="Path to the JSON history file produced by pipewatch.",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=20,
        metavar="N",
        help="Number of most-recent history entries to analyse (default: 20).",
    )
    parser.add_argument(
        "--noise-threshold",
        type=float,
        default=0.4,
        metavar="T",
        help="Noise score threshold above which a pipeline is flagged (default: 0.40).",
    )
    parser.add_argument(
        "--min-events",
        type=int,
        default=5,
        metavar="M",
        help="Minimum total events required to include a pipeline (default: 5).",
    )
    parser.set_defaults(func=run_fatigue_cmd)


def run_fatigue_cmd(args: argparse.Namespace) -> None:
    history = load_history(args.history_file)
    report = compute_fatigue(
        history,
        window=args.window,
        noise_threshold=args.noise_threshold,
        min_events=args.min_events,
    )
    print(format_fatigue(report))
    if report.fatiguing():
        noisy = ", ".join(r.pipeline for r in report.fatiguing())
        print(f"\nWarning: {len(report.fatiguing())} noisy pipeline(s) detected: {noisy}")
