"""Tests for pipewatch.export.backpressure."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.export.backpressure import (
    BackpressureReport,
    compute_backpressure,
    format_backpressure,
)
from pipewatch.export.history import HistoryEntry


def _entry(
    pipeline: str,
    errors: int,
    total: int = 100,
    ts: str = "2024-01-01T00:00:00",
) -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        total_errors=errors,
        errors_by_pipeline={pipeline: errors},
    )


def test_compute_backpressure_empty():
    report = compute_backpressure([])
    assert isinstance(report, BackpressureReport)
    assert report.results == []
    assert not report.has_backpressure()


def test_compute_backpressure_no_growth():
    # Stable pipeline — slope should be ~0, not flagged
    history = [_entry("pipe-a", 10) for _ in range(5)]
    report = compute_backpressure(history, min_slope=0.02)
    assert len(report.results) == 1
    assert not report.results[0].is_backpressured
    assert report.results[0].slope == pytest.approx(0.0)


def test_compute_backpressure_detects_rising_rate():
    # Failure rate rises by 0.05 each step: 0.05, 0.10, 0.15, 0.20, 0.25
    history = [
        _entry("pipe-b", errors=5),
        _entry("pipe-b", errors=10),
        _entry("pipe-b", errors=15),
        _entry("pipe-b", errors=20),
        _entry("pipe-b", errors=25),
    ]
    report = compute_backpressure(history, min_slope=0.02)
    assert report.has_backpressure()
    result = report.backpressured()[0]
    assert result.pipeline == "pipe-b"
    assert result.slope == pytest.approx(0.05)


def test_compute_backpressure_below_slope_threshold_not_flagged():
    # Slope of 0.01 < default min_slope 0.02
    history = [
        _entry("pipe-c", errors=1),
        _entry("pipe-c", errors=2),
        _entry("pipe-c", errors=3),
        _entry("pipe-c", errors=4),
    ]
    report = compute_backpressure(history, min_slope=0.02)
    assert not report.has_backpressure()


def test_compute_backpressure_respects_window():
    # Only last 3 entries are within window; earlier spike is ignored
    history = [
        _entry("pipe-d", errors=90),
        _entry("pipe-d", errors=80),
        _entry("pipe-d", errors=10),
        _entry("pipe-d", errors=10),
        _entry("pipe-d", errors=10),
    ]
    report = compute_backpressure(history, window=3, min_slope=0.02)
    assert not report.has_backpressure()
    assert len(report.results[0].rates) == 3


def test_compute_backpressure_below_min_periods_excluded():
    history = [
        _entry("pipe-e", errors=10),
        _entry("pipe-e", errors=20),
    ]
    # Requires min_periods=3 but only 2 available
    report = compute_backpressure(history, min_periods=3)
    assert report.results == []


def test_format_backpressure_empty():
    report = BackpressureReport(results=[])
    output = format_backpressure(report)
    assert "No data" in output


def test_format_backpressure_shows_flag():
    history = [
        _entry("pipe-f", errors=5),
        _entry("pipe-f", errors=15),
        _entry("pipe-f", errors=25),
        _entry("pipe-f", errors=35),
    ]
    report = compute_backpressure(history, min_slope=0.02)
    output = format_backpressure(report)
    assert "pipe-f" in output
    assert "BACKPRESSURE" in output
