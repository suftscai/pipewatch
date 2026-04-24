"""Tests for pipewatch.export.throttle."""
from __future__ import annotations

import pytest

from pipewatch.export.history import HistoryEntry
from pipewatch.export.throttle import (
    ThrottleReport,
    ThrottleResult,
    compute_throttle,
    format_throttle,
)


def _entry(events_by_pipeline: dict) -> HistoryEntry:
    total = sum(events_by_pipeline.values())
    errors = {p: 0 for p in events_by_pipeline}
    return HistoryEntry(
        timestamp="2024-01-01T00:00:00",
        total_events=total,
        total_errors=0,
        failure_rate=0.0,
        events_by_pipeline=events_by_pipeline,
        errors_by_pipeline=errors,
    )


def test_compute_throttle_empty():
    report = compute_throttle([])
    assert isinstance(report, ThrottleReport)
    assert report.results == []
    assert not report.has_throttled()


def test_compute_throttle_below_threshold():
    entry = _entry({"pipe_a": 100})
    report = compute_throttle([entry], threshold=500.0)
    assert len(report.results) == 1
    result = report.results[0]
    assert result.pipeline == "pipe_a"
    assert result.events_per_hour == 100.0
    assert not result.throttled
    assert not report.has_throttled()


def test_compute_throttle_above_threshold():
    entry = _entry({"pipe_a": 600})
    report = compute_throttle([entry], threshold=500.0)
    assert report.results[0].throttled
    assert report.has_throttled()
    assert report.throttled()[0].pipeline == "pipe_a"


def test_compute_throttle_averages_across_entries():
    entries = [
        _entry({"pipe_a": 200}),
        _entry({"pipe_a": 400}),
    ]
    report = compute_throttle(entries, threshold=500.0)
    assert report.results[0].events_per_hour == 300.0
    assert not report.results[0].throttled


def test_compute_throttle_respects_window():
    # Only the last 2 entries should be considered.
    entries = [
        _entry({"pipe_a": 1000}),  # outside window
        _entry({"pipe_a": 100}),
        _entry({"pipe_a": 100}),
    ]
    report = compute_throttle(entries, threshold=500.0, window=2)
    assert report.results[0].events_per_hour == 100.0
    assert not report.results[0].throttled


def test_compute_throttle_multiple_pipelines():
    entry = _entry({"fast": 800, "slow": 50})
    report = compute_throttle([entry], threshold=500.0)
    by_name = {r.pipeline: r for r in report.results}
    assert by_name["fast"].throttled
    assert not by_name["slow"].throttled


def test_format_throttle_empty():
    report = ThrottleReport()
    text = format_throttle(report)
    assert "No data" in text


def test_format_throttle_shows_status():
    results = [
        ThrottleResult(pipeline="pipe_a", events_per_hour=600.0, threshold=500.0, throttled=True),
        ThrottleResult(pipeline="pipe_b", events_per_hour=200.0, threshold=500.0, throttled=False),
    ]
    report = ThrottleReport(results=results)
    text = format_throttle(report)
    assert "THROTTLED" in text
    assert "ok" in text
    assert "pipe_a" in text
    assert "pipe_b" in text
