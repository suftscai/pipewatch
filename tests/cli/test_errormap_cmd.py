"""Tests for pipewatch.cli.errormap_cmd."""
from __future__ import annotations
import argparse
import json
import os
import pytest
from pipewatch.cli.errormap_cmd import add_errormap_subparser, run_errormap_cmd
from pipewatch.export.history import HistoryEntry


def _write_history(path: str, entries: list) -> None:
    with open(path, "w") as f:
        json.dump([e.__dict__ for e in entries], f)


def _entry(msgs: dict) -> HistoryEntry:
    return HistoryEntry(
        timestamp="2024-01-01T00:00:00",
        total_events=5,
        error_count=2,
        warning_count=0,
        pipeline_errors={"pipe_a": 2},
        error_messages=msgs,
    )


def _build_args(history: str, window: int = 50, top: int = 10) -> argparse.Namespace:
    return argparse.Namespace(history=history, window=window, top=top)


def test_add_errormap_subparser_registers():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    add_errormap_subparser(subs)
    args = parser.parse_args(["errormap"])
    assert hasattr(args, "history")


def test_run_errormap_cmd_prints_output(tmp_path, capsys):
    p = tmp_path / "hist.json"
    _write_history(str(p), [_entry({"pipe_a": ["timeout"]})])
    run_errormap_cmd(_build_args(str(p)))
    out = capsys.readouterr().out
    assert "pipe_a" in out or "timeout" in out or "no error" in out.lower()


def test_run_errormap_cmd_empty_history(tmp_path, capsys):
    p = tmp_path / "empty.json"
    _write_history(str(p), [])
    run_errormap_cmd(_build_args(str(p)))
    out = capsys.readouterr().out
    assert "no error" in out.lower()
