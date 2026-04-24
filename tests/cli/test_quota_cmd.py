"""Tests for pipewatch.cli.quota_cmd."""
from __future__ import annotations

import argparse
import json
import os
from typing import Dict

import pytest

from pipewatch.cli.quota_cmd import add_quota_subparser, run_quota_cmd
from pipewatch.export.history import HistoryEntry


def _entry(error_counts: Dict[str, int]) -> dict:
    return {
        "timestamp": "2024-01-01T00:00:00+00:00",
        "total_events": sum(error_counts.values()) + 10,
        "total_errors": sum(error_counts.values()),
        "error_counts": error_counts,
        "top_failing": list(error_counts.keys())[:3],
    }


def _write_history(path: str, entries: list) -> None:
    with open(path, "w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")


def _build_args(history: str, quota: int = 100, window: int = 24) -> argparse.Namespace:
    return argparse.Namespace(history=history, quota=quota, window=window)


def test_add_quota_subparser_registers(tmp_path):
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    add_quota_subparser(subs)
    args = parser.parse_args(["quota", "--quota", "50"])
    assert args.quota == 50


def test_run_quota_cmd_compliant(tmp_path, capsys):
    hist = str(tmp_path / "h.jsonl")
    _write_history(hist, [_entry({"etl_a": 10})])
    run_quota_cmd(_build_args(hist, quota=100))
    out = capsys.readouterr().out
    assert "OK" in out


def test_run_quota_cmd_violation_exits_nonzero(tmp_path, capsys):
    hist = str(tmp_path / "h.jsonl")
    _write_history(hist, [_entry({"etl_a": 200})])
    with pytest.raises(SystemExit) as exc:
        run_quota_cmd(_build_args(hist, quota=100))
    assert exc.value.code == 1


def test_run_quota_cmd_prints_pipeline_name(tmp_path, capsys):
    hist = str(tmp_path / "h.jsonl")
    _write_history(hist, [_entry({"etl_critical": 500})])
    with pytest.raises(SystemExit):
        run_quota_cmd(_build_args(hist, quota=100))
    out = capsys.readouterr().out
    assert "etl_critical" in out


def test_run_quota_cmd_empty_history(tmp_path, capsys):
    hist = str(tmp_path / "h.jsonl")
    _write_history(hist, [])
    run_quota_cmd(_build_args(hist))
    out = capsys.readouterr().out
    assert "No data" in out
