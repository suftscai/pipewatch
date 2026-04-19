"""Tests for pipewatch.cli.sla_cmd."""
import argparse
import json
from pathlib import Path
from pipewatch.cli.sla_cmd import add_sla_subparser, run_sla_cmd
from pipewatch.export.history import HistoryEntry


def _write_history(path: Path, entries):
    with open(path, "w") as f:
        for e in entries:
            f.write(json.dumps({
                "timestamp": e.timestamp,
                "total_events": e.total_events,
                "error_counts": e.error_counts,
                "warning_counts": e.warning_counts,
                "top_failing": e.top_failing,
            }) + "\n")


def _entry(error_counts, total=100):
    return HistoryEntry(
        timestamp="2024-01-01T00:00:00",
        total_events=total,
        error_counts=error_counts,
        warning_counts={},
        top_failing=[],
    )


def _build_args(history_path, threshold=0.05, window=20):
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    add_sla_subparser(sub)
    return parser.parse_args(["sla", "--history", str(history_path),
                              "--threshold", str(threshold),
                              "--window", str(window)])


def test_add_sla_subparser_registers():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    add_sla_subparser(sub)
    args = parser.parse_args(["sla"])
    assert hasattr(args, "func")


def test_run_sla_cmd_all_compliant(tmp_path, capsys):
    hist = tmp_path / "h.jsonl"
    _write_history(hist, [_entry({"pipe_a": 1})])
    args = _build_args(hist, threshold=0.05)
    run_sla_cmd(args)
    out = capsys.readouterr().out
    assert "All pipelines within SLA" in out


def test_run_sla_cmd_violation(tmp_path, capsys):
    hist = tmp_path / "h.jsonl"
    _write_history(hist, [_entry({"bad": 20})])
    args = _build_args(hist, threshold=0.05)
    run_sla_cmd(args)
    out = capsys.readouterr().out
    assert "VIOLATION" in out
    assert "1 pipeline(s) violating SLA" in out


def test_run_sla_cmd_empty_history(tmp_path, capsys):
    hist = tmp_path / "empty.jsonl"
    hist.write_text("")
    args = _build_args(hist)
    run_sla_cmd(args)
    out = capsys.readouterr().out
    assert "no data" in out
