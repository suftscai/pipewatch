"""CLI subcommand for capturing and diffing pipeline snapshots."""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.cli.runner import run_once
from pipewatch.export.snapshot import (
    capture, save_snapshot, load_snapshot, diff_snapshots, format_snapshot_diff
)


def add_snapshot_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("snapshot", help="Capture or diff pipeline snapshots")
    p.add_argument("logfile", help="Path to pipeline log file")
    p.add_argument("--save", metavar="FILE", help="Save snapshot to file")
    p.add_argument("--diff", metavar="FILE", help="Diff current state against saved snapshot")
    p.add_argument("--label", default="", help="Label for this snapshot")
    p.add_argument("--tail", type=int, default=200, help="Lines to read (default 200)")


def run_snapshot_cmd(args: argparse.Namespace) -> None:
    summary, alerts = run_once(args.logfile, tail_lines=args.tail)
    snap = capture(summary, alerts, label=args.label)

    if args.diff:
        old = load_snapshot(Path(args.diff))
        if old is None:
            print(f"No snapshot found at {args.diff}")
        else:
            diff = diff_snapshots(old, snap)
            print(format_snapshot_diff(diff))

    if args.save:
        save_snapshot(snap, Path(args.save))
        print(f"Snapshot saved to {args.save}")

    if not args.save and not args.diff:
        import json
        print(json.dumps(snap.summary, indent=2))
