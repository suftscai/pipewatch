"""Tests for the cascade CLI subcommand."""
import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.cli.cascade_cmd import add_cascade_subparser, run_cascade_cmd
from pipewatch.export.history import HistoryEntry


def _entry(pipeline: str, offset_minutes: float, errors: int = 3) -> dict:
    ts = (datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=offset_minutes)).isoformat()
    return {
        "timestamp": ts,
        "pipeline": pipeline,
        "total_events": 10,
        "errors": errors,
        "warnings": 0,
        "failure_rate": errors / 10,
    }


def _write_history(path: Path, entries: list) -> None:
    with open(path, "w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")


def _build_args(history_file: str, window: int = 5, min_pipelines: int = 2, recent: int = 50) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    add_cascade_subparser(sub)
    return parser.parse_args(
        [
            "cascade",
            history_file,
            "--window", str(window),
            "--min-pipelines", str(min_pipelines),
            "--recent", str(recent),
        ]
    )


def test_add_cascade_subparser_registers():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    add_cascade_subparser(sub)
    args = parser.parse_args(["cascade", "some_file.jsonl"])
    assert hasattr(args, "func")
    assert args.func is run_cascade_cmd


def test_run_cascade_cmd_no_cascades(tmp_path, capsys):
    f = tmp_path / "history.jsonl"
    _write_history(f, [_entry("pipe-a", 0), _entry("pipe-b", 20)])
    args = _build_args(str(f))
    run_cascade_cmd(args)
    out = capsys.readouterr().out
    assert "No cascade" in out


def test_run_cascade_cmd_detects_cascade(tmp_path, capsys):
    f = tmp_path / "history.jsonl"
    _write_history(f, [_entry("pipe-a", 0), _entry("pipe-b", 1), _entry("pipe-c", 2)])
    args = _build_args(str(f), window=5, min_pipelines=2)
    run_cascade_cmd(args)
    out = capsys.readouterr().out
    assert "pipe-a" in out or "pipe-b" in out
    assert "errors=" in out
