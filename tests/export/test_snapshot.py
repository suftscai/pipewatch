import json
import time
from pathlib import Path

import pytest

from pipewatch.analysis.aggregator import PipelineSummary
from pipewatch.analysis.alert import Alert
from pipewatch.export.snapshot import (
    capture, save_snapshot, load_snapshot, diff_snapshots, format_snapshot_diff, Snapshot
)


def _summary(errors=2, warnings=1, total=10):
    return PipelineSummary(
        total_events=total,
        error_count=errors,
        warning_count=warnings,
        error_counts={"pipe_a": errors},
    )


def _alerts():
    return [Alert(pipeline="pipe_a", message="High failure rate")]


def test_capture_returns_snapshot():
    snap = capture(_summary(), _alerts(), label="test")
    assert isinstance(snap, Snapshot)
    assert snap.label == "test"
    assert snap.timestamp <= time.time()
    assert len(snap.alerts) == 1
    assert snap.alerts[0]["pipeline"] == "pipe_a"


def test_capture_no_alerts():
    snap = capture(_summary(), [])
    assert snap.alerts == []


def test_save_and_load_snapshot(tmp_path):
    snap = capture(_summary(), _alerts(), label="v1")
    p = tmp_path / "snap.json"
    save_snapshot(snap, p)
    loaded = load_snapshot(p)
    assert loaded is not None
    assert loaded.label == "v1"
    assert loaded.summary == snap.summary


def test_load_snapshot_missing_returns_none(tmp_path):
    result = load_snapshot(tmp_path / "nope.json")
    assert result is None


def test_diff_snapshots_detects_changes():
    old = Snapshot(timestamp=1.0, summary={"error_counts": {"pipe_a": 2}}, alerts=[])
    new = Snapshot(timestamp=2.0, summary={"error_counts": {"pipe_a": 5, "pipe_b": 3}}, alerts=[])
    diff = diff_snapshots(old, new)
    assert diff["pipe_a"] == 3
    assert diff["pipe_b"] == 3


def test_diff_snapshots_no_changes():
    snap = Snapshot(timestamp=1.0, summary={"error_counts": {"pipe_a": 2}}, alerts=[])
    assert diff_snapshots(snap, snap) == {}


def test_format_snapshot_diff_empty():
    assert "No changes" in format_snapshot_diff({})


def test_format_snapshot_diff_shows_delta():
    out = format_snapshot_diff({"pipe_a": 3, "pipe_b": -1})
    assert "+3" in out
    assert "-1" in out
