"""Tests for pipewatch.export.drift."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.export.drift import (
    DriftReport,
    compute_drift,
    format_drift,
    has_drift,
)
from pipewatch.export.history import HistoryEntry


def _entry(
    pipeline: str,
    total: int,
    errors: int,
    ts: str = "2024-01-01T00:00:00+00:00",
) -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        total_errors=errors,
        failure_rate=errors / total if total else 0.0,
        top_failing=[],
        pipeline_totals={pipeline: total},
        pipeline_errors={pipeline: errors},
    )


def test_compute_drift_empty():
    report = compute_drift([])
    assert report.results == []
    assert not has_drift(report)


def test_compute_drift_no_drift_stable():
    history = [_entry("etl", 100, 5)] * 10
    report = compute_drift(history, baseline_window=7, recent_window=3, threshold=0.10)
    assert len(report.results) == 1
    result = report.results[0]
    assert result.pipeline == "etl"
    assert abs(result.delta) < 0.01
    assert not result.is_drifting
    assert not has_drift(report)


def test_compute_drift_detects_spike():
    baseline = [_entry("etl", 100, 5)] * 7   # 5% error rate
    recent = [_entry("etl", 100, 30)] * 3    # 30% error rate
    report = compute_drift(baseline + recent, baseline_window=7, recent_window=3, threshold=0.10)
    assert len(report.results) == 1
    r = report.results[0]
    assert r.is_drifting
    assert r.delta == pytest.approx(0.25, abs=0.01)
    assert has_drift(report)


def test_compute_drift_respects_window():
    # Only the last 3 entries should be "recent"; older spike should not matter
    old_spike = [_entry("pipe", 100, 90)] * 5
    recent = [_entry("pipe", 100, 5)] * 3
    report = compute_drift(old_spike + recent, baseline_window=5, recent_window=3, threshold=0.10)
    r = report.results[0]
    # recent is healthy; delta should be negative
    assert r.delta < 0
    assert not r.is_drifting


def test_compute_drift_multiple_pipelines():
    history = [
        HistoryEntry(
            timestamp="2024-01-01T00:00:00+00:00",
            total_events=200,
            total_errors=20,
            failure_rate=0.1,
            top_failing=[],
            pipeline_totals={"a": 100, "b": 100},
            pipeline_errors={"a": 2, "b": 18},
        )
    ] * 10
    report = compute_drift(history, baseline_window=7, recent_window=3, threshold=0.10)
    pipelines = {r.pipeline for r in report.results}
    assert {"a", "b"} == pipelines


def test_format_drift_shows_header():
    report = compute_drift([])
    text = format_drift(report)
    assert "Drift Report" in text


def test_format_drift_shows_drifting_flag():
    baseline = [_entry("etl", 100, 5)] * 7
    recent = [_entry("etl", 100, 50)] * 3
    report = compute_drift(baseline + recent, threshold=0.10)
    text = format_drift(report)
    assert "[DRIFTING]" in text
    assert "etl" in text
