"""Tests for pipewatch.cli.regression_cmd."""
from __future__ import annotations

import json
import argparse
from pathlib import Path

import pytest

from pipewatch.cli.regression_cmd import add_regression_subparser, run_regression_cmd
from pipewatch.export.history import HistoryEntry


def _entry(pipeline: str, errors: int, total: int) -> dict:
    return {
        "timestamp": "2024-01-01T00:00:00+00:00",
        "total_events": total,
        "total_failures": errors,
        "failure_rate": errors / total if total else 0.0,
        "top_failing": [],
        "failure_counts": {pipeline: errors},
        "event_counts": {pipeline: total},
    }


def _write_history(path: Path, entries: list[dict]) -> None:
    with path.open("w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")


def _build_args(tmp_path: Path, **kwargs) -> argparse.Namespace:
    history_file = tmp_path / "history.jsonl"
    defaults = dict(
        history=str(history_file),
        baseline_window=10,
        current_window=5,
        threshold=0.10,
        func=run_regression_cmd,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_add_regression_subparser_registers():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    add_regression_subparser(subs)
    args = parser.parse_args(["regression", "--threshold", "0.05"])
    assert args.threshold == 0.05


def test_run_regression_cmd_no_history(tmp_path: Path, capsys):
    args = _build_args(tmp_path)
    run_regression_cmd(args)
    out = capsys.readouterr().out
    assert "No history data found" in out


def test_run_regression_cmd_no_regressions(tmp_path: Path, capsys):
    history_file = tmp_path / "history.jsonl"
    entries = [_entry("pipe_a", 1, 100)] * 15
    _write_history(history_file, entries)
    args = _build_args(tmp_path, history=str(history_file))
    run_regression_cmd(args)
    out = capsys.readouterr().out
    assert "Regression Report" in out
    assert "REGRESSED" not in out


def test_run_regression_cmd_detects_regression(tmp_path: Path, capsys):
    history_file = tmp_path / "history.jsonl"
    baseline = [_entry("pipe_b", 1, 100)] * 10
    current = [_entry("pipe_b", 40, 100)] * 5
    _write_history(history_file, baseline + current)
    args = _build_args(tmp_path, history=str(history_file))
    run_regression_cmd(args)
    out = capsys.readouterr().out
    assert "REGRESSED" in out
    assert "pipe_b" in out
