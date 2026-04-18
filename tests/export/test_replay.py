"""Tests for pipewatch.export.replay."""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone

import pytest

from pipewatch.export.replay import compute_replay, format_replay, ReplayFrame


def _entry(ts: str, errors: int, warnings: int) -> dict:
    return {
        "timestamp": ts,
        "total_events": errors + warnings + 10,
        "total_errors": errors,
        "total_warnings": warnings,
        "failure_rate": round(errors / max(errors + warnings + 10, 1), 4),
        "top_failing": [],
    }


def _write_history(path: str, entries: list) -> None:
    with open(path, "w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")


def test_compute_replay_empty(tmp_path):
    p = str(tmp_path / "h.jsonl")
    report = compute_replay(p, window=5)
    assert report.frames == []
    assert report.window == 5


def test_compute_replay_single_entry(tmp_path):
    p = str(tmp_path / "h.jsonl")
    _write_history(p, [_entry("2024-01-01T00:00:00", 3, 1)])
    report = compute_replay(p, window=10)
    assert len(report.frames) == 1
    frame = report.frames[0]
    assert frame.delta_errors == 3
    assert frame.delta_warnings == 1


def test_compute_replay_deltas(tmp_path):
    p = str(tmp_path / "h.jsonl")
    _write_history(p, [
        _entry("2024-01-01T00:00:00", 2, 1),
        _entry("2024-01-02T00:00:00", 5, 3),
        _entry("2024-01-03T00:00:00", 4, 3),
    ])
    report = compute_replay(p, window=10)
    assert report.frames[1].delta_errors == 3
    assert report.frames[2].delta_errors == -1


def test_compute_replay_respects_window(tmp_path):
    p = str(tmp_path / "h.jsonl")
    entries = [_entry(f"2024-01-{i+1:02d}T00:00:00", i, 0) for i in range(8)]
    _write_history(p, entries)
    report = compute_replay(p, window=4)
    assert len(report.frames) == 4
    assert report.frames[0].entry.total_errors == 4


def test_format_replay_empty(tmp_path):
    p = str(tmp_path / "h.jsonl")
    report = compute_replay(p)
    out = format_replay(report)
    assert "No history" in out


def test_format_replay_contains_timestamps(tmp_path):
    p = str(tmp_path / "h.jsonl")
    _write_history(p, [_entry("2024-06-15T12:00:00", 2, 0)])
    report = compute_replay(p)
    out = format_replay(report)
    assert "2024-06-15" in out
    assert "errors=2" in out
