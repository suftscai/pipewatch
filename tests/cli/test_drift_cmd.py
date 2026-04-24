"""Tests for pipewatch.cli.drift_cmd."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from pipewatch.cli.drift_cmd import add_drift_subparser, run_drift_cmd
from pipewatch.export.history import HistoryEntry


def _entry(pipeline: str, total: int, errors: int) -> dict:
    return {
        "timestamp": "2024-01-01T00:00:00+00:00",
        "total_events": total,
        "total_errors": errors,
        "failure_rate": errors / total if total else 0.0,
        "top_failing": [],
        "pipeline_totals": {pipeline: total},
        "pipeline_errors": {pipeline: errors},
    }


def _write_history(path: Path, entries: list[dict]) -> None:
    with path.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


def _build_args(history_path: Path, **kwargs) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    add_drift_subparser(sub)
    args_list = ["drift", "--history", str(history_path)]
    for k, v in kwargs.items():
        args_list += [f"--{k.replace('_', '-')}", str(v)]
    return parser.parse_args(args_list)


def test_add_drift_subparser_registers():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    add_drift_subparser(sub)
    args = parser.parse_args(["drift", "--history", "x.jsonl"])
    assert hasattr(args, "func")


def test_run_drift_cmd_no_history(tmp_path, capsys):
    history_file = tmp_path / "empty.jsonl"
    history_file.write_text("")
    args = _build_args(history_file)
    run_drift_cmd(args)
    out = capsys.readouterr().out
    assert "Drift Report" in out


def test_run_drift_cmd_prints_output(tmp_path, capsys):
    history_file = tmp_path / "history.jsonl"
    baseline = [_entry("etl", 100, 5)] * 7
    recent = [_entry("etl", 100, 40)] * 3
    _write_history(history_file, baseline + recent)
    args = _build_args(history_file, threshold=0.10)
    run_drift_cmd(args)
    out = capsys.readouterr().out
    assert "etl" in out
    assert "[DRIFTING]" in out


def test_run_drift_cmd_respects_threshold(tmp_path, capsys):
    history_file = tmp_path / "history.jsonl"
    baseline = [_entry("etl", 100, 5)] * 7
    recent = [_entry("etl", 100, 12)] * 3  # ~7% delta — below 0.20 threshold
    _write_history(history_file, baseline + recent)
    args = _build_args(history_file, threshold=0.20)
    run_drift_cmd(args)
    out = capsys.readouterr().out
    assert "[DRIFTING]" not in out
