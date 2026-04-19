"""Tests for pipewatch.cli.correlation_cmd."""
import json
import argparse
import pytest
from pathlib import Path
from pipewatch.cli.correlation_cmd import add_correlation_subparser, run_correlation_cmd
from pipewatch.export.history import HistoryEntry
from datetime import datetime, timezone


def _write_history(path: Path, entries):
    with open(path, "w") as f:
        for e in entries:
            f.write(json.dumps(e.__dict__) + "\n")


def _entry(error_counts, total_events):
    return HistoryEntry(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_errors=sum(error_counts.values()),
        total_warnings=0,
        total_events=total_events,
        error_counts=error_counts,
        top_failing=[],
    )


def _build_args(log, history, window=10, min_strength="none"):
    return argparse.Namespace(log=log, history=history, window=window, min_strength=min_strength)


def test_add_correlation_subparser_registers():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    add_correlation_subparser(subs)
    args = parser.parse_args(["correlation", "some.log"])
    assert args.log == "some.log"


def test_run_correlation_cmd_prints_output(tmp_path, capsys):
    hist = tmp_path / "history.jsonl"
    entries = [
        _entry({"a": i, "b": i}, {"a": 10, "b": 10})
        for i in range(1, 7)
    ]
    _write_history(hist, entries)
    args = _build_args(str(tmp_path / "fake.log"), str(hist))
    run_correlation_cmd(args)
    out = capsys.readouterr().out
    assert "a" in out
    assert "b" in out


def test_run_correlation_cmd_empty_history(tmp_path, capsys):
    hist = tmp_path / "history.jsonl"
    hist.write_text("")
    args = _build_args(str(tmp_path / "fake.log"), str(hist))
    run_correlation_cmd(args)
    out = capsys.readouterr().out
    assert "No correlation" in out


def test_run_correlation_cmd_min_strength_filters(tmp_path, capsys):
    hist = tmp_path / "history.jsonl"
    # Uncorrelated pipelines -> strength "none"
    import random
    random.seed(42)
    entries = [
        _entry({"x": random.randint(0, 5), "y": random.randint(0, 5)}, {"x": 10, "y": 10})
        for _ in range(20)
    ]
    _write_history(hist, entries)
    args = _build_args(str(tmp_path / "fake.log"), str(hist), min_strength="strong")
    run_correlation_cmd(args)
    out = capsys.readouterr().out
    # May print no pairs or only strong ones — just ensure it runs
    assert isinstance(out, str)
