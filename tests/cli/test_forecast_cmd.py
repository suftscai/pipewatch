"""Tests for pipewatch.cli.forecast_cmd."""
import argparse
import json
from pathlib import Path
import pytest
from pipewatch.cli.forecast_cmd import add_forecast_subparser, run_forecast_cmd
from pipewatch.export.history import HistoryEntry


def _write_history(path: Path, entries):
    with open(path, "w") as f:
        for e in entries:
            f.write(json.dumps(e.__dict__) + "\n")


def _entry(pipeline, failures, total, ts="2024-01-01T00:00:00"):
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        error_count=failures,
        warning_count=0,
        failure_rate=failures / total if total else 0.0,
        top_failing=[pipeline],
        failures_by_pipeline={pipeline: failures},
    )


def _build_args(history_path, pipeline="etl", steps=3, window=10):
    ns = argparse.Namespace(
        pipeline=pipeline,
        history=str(history_path),
        steps=steps,
        window=window,
    )
    return ns


def test_add_forecast_subparser_registers():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    add_forecast_subparser(subs)
    args = parser.parse_args(["forecast", "etl"])
    assert args.pipeline == "etl"


def test_run_forecast_cmd_prints_output(tmp_path, capsys):
    hist_file = tmp_path / "history.jsonl"
    entries = [_entry("etl", i + 1, 100, f"2024-01-{i+1:02d}T00:00:00") for i in range(5)]
    _write_history(hist_file, entries)
    args = _build_args(hist_file, pipeline="etl", steps=2)
    run_forecast_cmd(args)
    out = capsys.readouterr().out
    assert "etl" in out
    assert "period" in out


def test_run_forecast_cmd_empty_history(tmp_path, capsys):
    hist_file = tmp_path / "empty.jsonl"
    hist_file.write_text("")
    args = _build_args(hist_file, pipeline="missing", steps=1)
    run_forecast_cmd(args)
    out = capsys.readouterr().out
    assert "missing" in out
