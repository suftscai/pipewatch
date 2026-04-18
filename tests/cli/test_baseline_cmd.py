"""Tests for pipewatch.cli.baseline_cmd."""
import json
import argparse
import pytest

from pipewatch.cli.baseline_cmd import add_baseline_subparser, run_baseline_cmd
from pipewatch.export.history import HistoryEntry, append_entry
from pipewatch.analysis.aggregator import PipelineSummary


def _write_history(path, entries):
    for e in entries:
        import dataclasses, json as _json
        with open(path, "a") as f:
            f.write(_json.dumps(dataclasses.asdict(e)) + "\n")


def _summary(top_failing=None):
    return PipelineSummary(
        total_events=100,
        total_errors=10,
        total_warnings=5,
        top_failing=top_failing or [("pipe_a", 10)],
    )


def _build_args(**kwargs):
    ns = argparse.Namespace(**kwargs)
    return ns


def test_add_baseline_subparser_registers():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    add_baseline_subparser(sub)
    args = parser.parse_args(["baseline", "save", "history.jsonl"])
    assert args.baseline_cmd == "save"


def test_run_baseline_save(tmp_path):
    hist = tmp_path / "history.jsonl"
    append_entry(str(hist), _summary(), [])
    out = str(tmp_path / "baseline.json")
    args = _build_args(baseline_cmd="save", history=str(hist), output=out)
    rc = run_baseline_cmd(args)
    assert rc == 0
    import json, os
    assert os.path.exists(out)


def test_run_baseline_compare(tmp_path):
    hist = tmp_path / "history.jsonl"
    append_entry(str(hist), _summary(top_failing=[("pipe_a", 10)]), [])
    baseline = tmp_path / "baseline.json"
    baseline.write_text(json.dumps({"pipe_a": 0.05}))
    args = _build_args(
        baseline_cmd="compare",
        history=str(hist),
        baseline=str(baseline),
        threshold=0.05,
    )
    rc = run_baseline_cmd(args)
    assert rc == 0


def test_run_baseline_compare_empty_history(tmp_path, capsys):
    hist = tmp_path / "empty.jsonl"
    hist.write_text("")
    baseline = tmp_path / "baseline.json"
    baseline.write_text("{}")
    args = _build_args(
        baseline_cmd="compare",
        history=str(hist),
        baseline=str(baseline),
        threshold=0.05,
    )
    rc = run_baseline_cmd(args)
    assert rc == 1
