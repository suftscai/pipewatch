"""CLI subcommand: sla — show SLA compliance report."""
from __future__ import annotations
import argparse
from pipewatch.export.history import load_history
from pipewatch.export.sla import compute_sla, format_sla


def add_sla_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("sla", help="Show SLA compliance per pipeline")
    p.add_argument("--history", default="pipewatch_history.jsonl", help="History file")
    p.add_argument(
        "--threshold",
        type=float,
        default=0.05,
        help="Max allowed failure rate (default 0.05)",
    )
    p.add_argument(
        "--window",
        type=int,
        default=20,
        help="Number of recent history entries to consider",
    )
    p.set_defaults(func=run_sla_cmd)


def run_sla_cmd(args: argparse.Namespace) -> None:
    history = load_history(args.history)
    report = compute_sla(history, threshold=args.threshold, window=args.window)
    print(format_sla(report))
    if report.violations:
        print(f"\n{len(report.violations)} pipeline(s) violating SLA.")
    else:
        print("\nAll pipelines within SLA.")
