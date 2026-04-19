"""Tests for burst CLI subcommand."""
import json
import argparse
import pytest
from pathlib import Path
from pipewatch.cli.burst_cmd import add_burst_subparser, run_burst_cmd
from pipewatch.export.history import HistoryEntry


def _entry(ts, pipeline, errors, total):
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        total_errors=errors,
        failure_rate=errors / total if total else 0.0,
        top_failing=[],
        per_pipeline={pipeline: {"errors": errors, "total": total}},
    )


def _write_history(path: Path, entries):
    import dataclasses
    path.write_text(
        json.dumps([dataclasses.asdict(e) for e in entries])
    )


def _build_args(history_file, window=24, min_rate=0.5, min_errors=3):
    return argparse.Namespace(
        history_file=str(history_file),
        window=window,
        min_rate=min_rate,
        min_errors=min_errors,
    )


def test_add_burst_subparser_registers():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    add_burst_subparser(sub)
    args = parser.parse_args(["burst", "history.json"])
    assert args.history_file == "history.json"
    assert args.window == 24


def test_run_burst_cmd_no_bursts(tmp_path, capsys):
    f = tmp_path / "h.json"
    _write_history(f, [_entry("2024-01-01T10", "pipe", 1, 100)])
    run_burst_cmd(_build_args(f))
    out = capsys.readouterr().out
    assert "No error bursts" in out


def test_run_burst_cmd_detects_burst(tmp_path, capsys):
    f = tmp_path / "h.json"
    _write_history(f, [_entry("2024-01-01T10", "bad_pipe", 9, 10)])
    run_burst_cmd(_build_args(f, min_rate=0.5, min_errors=3))
    out = capsys.readouterr().out
    assert "bad_pipe" in out


def test_run_burst_cmd_empty_history(tmp_path, capsys):
    f = tmp_path / "h.json"
    f.write_text("[]")
    run_burst_cmd(_build_args(f))
    out = capsys.readouterr().out
    assert "No error bursts" in out
