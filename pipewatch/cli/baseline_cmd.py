"""CLI helpers for baseline save/compare sub-commands."""
from __future__ import annotations

import argparse
import sys

from pipewatch.export.history import load_history
from pipewatch.export.baseline import (
    save_baseline,
    load_baseline,
    compare_to_baseline,
    format_baseline,
)


def add_baseline_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("baseline", help="Manage pipeline baselines")
    sub = p.add_subparsers(dest="baseline_cmd", required=True)

    save_p = sub.add_parser("save", help="Save current history as baseline")
    save_p.add_argument("history", help="Path to history JSONL file")
    save_p.add_argument("--output", default="baseline.json", help="Output baseline JSON path")

    cmp_p = sub.add_parser("compare", help="Compare history against baseline")
    cmp_p.add_argument("history", help="Path to history JSONL file")
    cmp_p.add_argument("--baseline", default="baseline.json", help="Baseline JSON path")
    cmp_p.add_argument("--threshold", type=float, default=0.05, help="Regression delta threshold")


def run_baseline_cmd(args: argparse.Namespace) -> int:
    if args.baseline_cmd == "save":
        entries = load_history(args.history)
        save_baseline(entries, args.output)
        print(f"Baseline saved to {args.output} ({len(entries)} entries).")
        return 0

    if args.baseline_cmd == "compare":
        entries = load_history(args.history)
        if not entries:
            print("No history entries found.", file=sys.stderr)
            return 1
        last = entries[-1]
        total = last.total_events
        current_rates = {
            pipeline: count / total if total else 0.0
            for pipeline, count in last.top_failing
        }
        baseline = load_baseline(args.baseline)
        reports = compare_to_baseline(baseline, current_rates, threshold=args.threshold)
        print(format_baseline(reports))
        return 0

    return 1
