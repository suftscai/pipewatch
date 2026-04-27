"""Tests for pipewatch.export.echorate."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.export.echorate import (
    EchoReport,
    compute_echorate,
    format_echorate,
)
from pipewatch.export.history import HistoryEntry


def _entry(total: int, errors: int, ts: str = "2024-01-01T00:00:00") -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        total_errors=errors,
        total_warnings=0,
        failure_rate=errors / total if total else 0.0,
        top_failing=[],
    )


def test_compute_echorate_empty():
    report = compute_echorate([])
    assert isinstance(report, EchoReport)
    assert not report.has_echoes()
    assert report.results == []


def test_compute_echorate_too_few_periods():
    history = [_entry(100, 10), _entry(100, 10)]
    report = compute_echorate(history, min_periods=3)
    assert not report.has_echoes()


def test_compute_echorate_no_echo_varying_rates():
    history = [
        _entry(100, 5),
        _entry(100, 50),
        _entry(100, 10),
        _entry(100, 80),
    ]
    report = compute_echorate(history, min_periods=3, variance_threshold=0.0001)
    assert not report.has_echoes()


def test_compute_echorate_detects_constant_rate():
    # All entries have exactly 10% error rate — variance should be 0
    history = [_entry(100, 10) for _ in range(5)]
    report = compute_echorate(history, min_periods=3, variance_threshold=0.0001)
    assert report.has_echoes()
    flagged = report.echoing()
    assert len(flagged) == 1
    assert flagged[0].variance == 0.0


def test_compute_echorate_zero_error_rate_not_flagged():
    # Constant 0% failure — healthy pipeline, should NOT be flagged
    history = [_entry(100, 0) for _ in range(5)]
    report = compute_echorate(history, min_periods=3, variance_threshold=0.0001)
    assert not report.has_echoes()


def test_compute_echorate_respects_window():
    # First 3 entries vary wildly; last 4 are constant — only last 4 used
    history = [
        _entry(100, 1),
        _entry(100, 90),
        _entry(100, 50),
        _entry(100, 20),
        _entry(100, 20),
        _entry(100, 20),
        _entry(100, 20),
    ]
    report = compute_echorate(history, window=4, min_periods=3, variance_threshold=0.0001)
    assert report.has_echoes()


def test_format_echorate_no_data():
    report = EchoReport(results=[])
    output = format_echorate(report)
    assert "No data" in output


def test_format_echorate_shows_status():
    history = [_entry(100, 10) for _ in range(5)]
    report = compute_echorate(history, min_periods=3)
    output = format_echorate(report)
    assert "ECHO" in output
    assert "variance" in output
