"""Tests for pipewatch.cli.heartbeat_cmd."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.cli.heartbeat_cmd import add_heartbeat_subparser, run_heartbeat_cmd


def _entry(pipeline: str, minutes_ago: float) -> dict:
    ts = (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()
    return {
        "timestamp": ts,
        "total_events": 10,
        "error_count": 1,
        "warning_count": 0,
        "failure_rate": 0.1,
        "top_failing": [pipeline],
    }


def _write_history(path: Path, entries: list) -> None:
    with open(path, "w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")


def _build_args(history_path: str, interval: float = 300.0, window: int = 50):
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    add_heartbeat_subparser(sub)
    return parser.parse_args(["heartbeat", "--history", history_path,
                               "--interval", str(interval),
                               "--window", str(window)])


def test_add_heartbeat_subparser_registers():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    add_heartbeat_subparser(sub)
    args = parser.parse_args(["heartbeat"])
    assert args.cmd == "heartbeat"


def test_run_heartbeat_cmd_no_history(tmp_path, capsys):
    history = tmp_path / "hist.jsonl"
    history.write_text("")
    args = _build_args(str(history))
    run_heartbeat_cmd(args)
    out = capsys.readouterr().out
    assert "Heartbeat" in out


def test_run_heartbeat_cmd_recent_pipeline_ok(tmp_path, capsys):
    history = tmp_path / "hist.jsonl"
    _write_history(history, [_entry("pipe-ok", minutes_ago=1)])
    args = _build_args(str(history), interval=300.0)
    run_heartbeat_cmd(args)
    out = capsys.readouterr().out
    assert "pipe-ok" in out
    assert "FLATLINE" not in out


def test_run_heartbeat_cmd_flatline_reported(tmp_path, capsys):
    history = tmp_path / "hist.jsonl"
    _write_history(history, [_entry("pipe-dead", minutes_ago=30)])
    args = _build_args(str(history), interval=300.0)
    run_heartbeat_cmd(args)
    out = capsys.readouterr().out
    assert "pipe-dead" in out
    assert "FLATLINE" in out
    assert "flatlined" in out
