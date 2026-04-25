"""Tests for pipewatch.cli.congestion_cmd."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.cli.congestion_cmd import add_congestion_subparser, run_congestion_cmd
from pipewatch.export.history import HistoryEntry


def _entry(pipeline: str, total: int, errors: int = 0) -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_events": total,
        "total_errors": errors,
        "failure_rate": errors / total if total else 0.0,
        "top_failing": [],
        "per_pipeline": {pipeline: {"total": total, "errors": errors}},
    }


def _write_history(path: Path, entries: list) -> None:
    with path.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


def _build_args(history_path: Path, **kwargs) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    add_congestion_subparser(sub)
    argv = ["congestion", "--history", str(history_path)]
    for k, v in kwargs.items():
        argv += [f"--{k.replace('_', '-')}", str(v)]
    return parser.parse_args(argv)


def test_add_congestion_subparser_registers():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    add_congestion_subparser(sub)
    args = parser.parse_args(["congestion"])
    assert hasattr(args, "func")
    assert args.func is run_congestion_cmd


def test_run_congestion_cmd_no_history(tmp_path, capsys):
    missing = tmp_path / "nope.jsonl"
    args = _build_args(missing)
    run_congestion_cmd(args)
    out = capsys.readouterr().out
    assert "Congestion" in out


def test_run_congestion_cmd_no_congestion(tmp_path, capsys):
    path = tmp_path / "hist.jsonl"
    entries = [_entry("pipe_ok", 5) for _ in range(5)]
    _write_history(path, entries)
    args = _build_args(path, threshold=100.0, min_entries=3)
    run_congestion_cmd(args)
    out = capsys.readouterr().out
    assert "[CONGESTED]" not in out


def test_run_congestion_cmd_with_congestion(tmp_path, capsys):
    path = tmp_path / "hist.jsonl"
    entries = [_entry("pipe_busy", 500) for _ in range(5)]
    _write_history(path, entries)
    args = _build_args(path, threshold=100.0, min_entries=3)
    run_congestion_cmd(args)
    out = capsys.readouterr().out
    assert "[CONGESTED]" in out
    assert "pipe_busy" in out
    assert "congested pipeline" in out
