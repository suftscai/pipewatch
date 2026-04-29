"""Tests for pipewatch.cli.backpressure_cmd."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from pipewatch.cli.backpressure_cmd import add_backpressure_subparser, run_backpressure_cmd
from pipewatch.export.history import HistoryEntry


def _entry(pipeline: str, errors: int, total: int = 100) -> dict:
    return {
        "timestamp": "2024-01-01T00:00:00",
        "total_events": total,
        "total_errors": errors,
        "errors_by_pipeline": {pipeline: errors},
    }


def _write_history(path: Path, entries: list[dict]) -> None:
    with open(path, "w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


def _build_args(history: str, **kwargs) -> argparse.Namespace:
    defaults = {"window": 10, "min_slope": 0.02, "min_periods": 3}
    defaults.update(kwargs)
    return argparse.Namespace(history=history, **defaults)


def test_add_backpressure_subparser_registers():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    add_backpressure_subparser(subs)
    args = parser.parse_args(["backpressure"])
    assert hasattr(args, "func")
    assert args.func is run_backpressure_cmd


def test_run_backpressure_cmd_no_history(tmp_path, capsys):
    args = _build_args(str(tmp_path / "missing.jsonl"))
    run_backpressure_cmd(args)
    out = capsys.readouterr().out
    assert "Backpressure" in out
    assert "No data" in out


def test_run_backpressure_cmd_no_backpressure(tmp_path, capsys):
    hist = tmp_path / "history.jsonl"
    _write_history(hist, [_entry("stable", 10) for _ in range(5)])
    args = _build_args(str(hist))
    run_backpressure_cmd(args)
    out = capsys.readouterr().out
    assert "stable" in out
    assert "BACKPRESSURE" not in out


def test_run_backpressure_cmd_detects_rising(tmp_path, capsys):
    hist = tmp_path / "history.jsonl"
    _write_history(hist, [
        _entry("rising", errors=5),
        _entry("rising", errors=15),
        _entry("rising", errors=25),
        _entry("rising", errors=35),
        _entry("rising", errors=45),
    ])
    args = _build_args(str(hist), min_slope=0.02)
    run_backpressure_cmd(args)
    out = capsys.readouterr().out
    assert "rising" in out
    assert "BACKPRESSURE" in out


def test_run_backpressure_cmd_respects_window(tmp_path, capsys):
    hist = tmp_path / "history.jsonl"
    # First two entries have a spike; window=3 should ignore them
    _write_history(hist, [
        _entry("pipe", errors=90),
        _entry("pipe", errors=80),
        _entry("pipe", errors=10),
        _entry("pipe", errors=10),
        _entry("pipe", errors=10),
    ])
    args = _build_args(str(hist), window=3)
    run_backpressure_cmd(args)
    out = capsys.readouterr().out
    assert "BACKPRESSURE" not in out
