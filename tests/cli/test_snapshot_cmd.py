import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import argparse

import pytest

from pipewatch.cli.snapshot_cmd import add_snapshot_subparser, run_snapshot_cmd
from pipewatch.analysis.aggregator import PipelineSummary
from pipewatch.export.snapshot import Snapshot


def _summary():
    return PipelineSummary(total_events=5, error_count=1, warning_count=0, error_counts={"p": 1})


def _build_args(**kwargs):
    defaults = dict(logfile="fake.log", save=None, diff=None, label="", tail=200)
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _write_snapshot(path, error_counts):
    """Write a minimal snapshot JSON file to *path* with the given error_counts."""
    data = {
        "timestamp": 1.0,
        "summary": {"error_counts": error_counts},
        "alerts": [],
        "label": "",
    }
    Path(path).write_text(json.dumps(data))


def test_add_snapshot_subparser_registers():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    add_snapshot_subparser(subs)
    args = parser.parse_args(["snapshot", "mylog.log"])
    assert args.logfile == "mylog.log"


def test_run_snapshot_cmd_prints_summary(capsys):
    with patch("pipewatch.cli.snapshot_cmd.run_once", return_value=(_summary(), [])):
        run_snapshot_cmd(_build_args())
    out = capsys.readouterr().out
    assert "{" in out


def test_run_snapshot_cmd_saves_file(tmp_path):
    out_file = str(tmp_path / "snap.json")
    with patch("pipewatch.cli.snapshot_cmd.run_once", return_value=(_summary(), [])):
        run_snapshot_cmd(_build_args(save=out_file))
    assert Path(out_file).exists()


def test_run_snapshot_cmd_diff_missing(tmp_path, capsys):
    with patch("pipewatch.cli.snapshot_cmd.run_once", return_value=(_summary(), [])):
        run_snapshot_cmd(_build_args(diff=str(tmp_path / "missing.json")))
    out = capsys.readouterr().out
    assert "No snapshot found" in out


def test_run_snapshot_cmd_diff_existing(tmp_path, capsys):
    p = tmp_path / "old.json"
    _write_snapshot(p, {"p": 0})
    with patch("pipewatch.cli.snapshot_cmd.run_once", return_value=(_summary(), [])):
        run_snapshot_cmd(_build_args(diff=str(p)))
    out = capsys.readouterr().out
    assert "diff" in out.lower() or "change" in out.lower() or "p" in out
