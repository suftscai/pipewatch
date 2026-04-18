"""Tests for pipewatch.export.baseline."""
import json
import os
import pytest

from pipewatch.export.baseline import (
    save_baseline,
    load_baseline,
    compare_to_baseline,
    format_baseline,
    BaselineReport,
)
from pipewatch.export.history import HistoryEntry


def _entry(top_failing=None, total_events=100, failure_rate=0.1):
    return HistoryEntry(
        timestamp="2024-01-01T00:00:00",
        total_events=total_events,
        total_errors=int(total_events * failure_rate),
        total_warnings=0,
        failure_rate=failure_rate,
        top_failing=top_failing or [("pipe_a", 10)],
    )


def test_load_baseline_missing_returns_empty(tmp_path):
    result = load_baseline(str(tmp_path / "nope.json"))
    assert result == {}


def test_save_and_load_baseline(tmp_path):
    path = str(tmp_path / "baseline.json")
    entries = [_entry(top_failing=[("pipe_a", 10)], total_events=100)]
    save_baseline(entries, path)
    data = load_baseline(path)
    assert "pipe_a" in data
    assert isinstance(data["pipe_a"], float)


def test_save_baseline_empty_entries(tmp_path):
    path = str(tmp_path / "baseline.json")
    save_baseline([], path)
    data = load_baseline(path)
    assert data == {}


def test_compare_no_regression():
    baseline = {"pipe_a": 0.10}
    current = {"pipe_a": 0.12}
    reports = compare_to_baseline(baseline, current, threshold=0.05)
    assert len(reports) == 1
    assert not reports[0].regressed


def test_compare_regression_flagged():
    baseline = {"pipe_a": 0.05}
    current = {"pipe_a": 0.20}
    reports = compare_to_baseline(baseline, current, threshold=0.05)
    assert reports[0].regressed
    assert pytest.approx(reports[0].delta, abs=1e-6) == 0.15


def test_compare_new_pipeline_no_baseline():
    baseline = {}
    current = {"pipe_new": 0.30}
    reports = compare_to_baseline(baseline, current, threshold=0.05)
    assert reports[0].baseline_rate == 0.0
    assert reports[0].regressed


def test_format_baseline_empty():
    out = format_baseline([])
    assert "no pipelines" in out


def test_format_baseline_shows_regression():
    r = BaselineReport(pipeline="p", baseline_rate=0.05, current_rate=0.20, delta=0.15, regressed=True)
    out = format_baseline([r])
    assert "REGRESSED" in out
    assert "p" in out


def test_format_baseline_no_regression_label():
    r = BaselineReport(pipeline="p", baseline_rate=0.10, current_rate=0.11, delta=0.01, regressed=False)
    out = format_baseline([r])
    assert "REGRESSED" not in out
