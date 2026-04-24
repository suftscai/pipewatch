"""Tests for pipewatch.export.recovery."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.export.history import HistoryEntry
from pipewatch.export.recovery import (
    RecoveryReport,
    compute_recovery,
    format_recovery,
)


def _entry(
    pipeline: str,
    errors: int,
    total: int = 10,
    ts: str = "2024-01-01T00:00:00+00:00",
) -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        errors=errors,
        warnings=0,
        failure_rate=errors / total if total else 0.0,
        top_failing=[pipeline] if errors > 0 else [],
    )


def test_compute_recovery_empty():
    report = compute_recovery([])
    assert isinstance(report, RecoveryReport)
    assert report.results == []


def test_compute_recovery_no_failures():
    entries = [_entry("pipe_a", errors=0) for _ in range(5)]
    report = compute_recovery(entries)
    # pipe_a never appears in top_failing, so no groups
    assert report.results == []


def test_compute_recovery_all_failures_no_recovery():
    entries = [_entry("pipe_a", errors=3) for _ in range(5)]
    report = compute_recovery(entries)
    result = report.by_pipeline("pipe_a")
    assert result is not None
    assert result.recoveries == 0
    assert result.recovery_rate == 0.0


def test_compute_recovery_single_recovery():
    # failure, failure, success, failure
    entries = [
        _entry("pipe_a", errors=2),
        _entry("pipe_a", errors=2),
        HistoryEntry(
            timestamp="2024-01-01T01:00:00+00:00",
            total_events=10,
            errors=0,
            warnings=0,
            failure_rate=0.0,
            top_failing=[],
        ),
        _entry("pipe_a", errors=1),
    ]
    report = compute_recovery(entries)
    result = report.by_pipeline("pipe_a")
    assert result is not None
    assert result.recoveries == 1
    assert result.recovery_rate > 0.0


def test_compute_recovery_respects_window():
    # 20 entries outside window, 5 inside with failures
    old = [_entry("pipe_b", errors=5) for _ in range(20)]
    recent = [_entry("pipe_b", errors=5) for _ in range(5)]
    report = compute_recovery(old + recent, window=5)
    result = report.by_pipeline("pipe_b")
    assert result is not None
    assert result.total_failures == 5


def test_format_recovery_empty():
    report = RecoveryReport(results=[])
    output = format_recovery(report)
    assert "No data" in output


def test_format_recovery_shows_pipeline():
    entries = [
        _entry("etl_main", errors=3),
        HistoryEntry(
            timestamp="2024-01-01T01:00:00+00:00",
            total_events=10,
            errors=0,
            warnings=0,
            failure_rate=0.0,
            top_failing=[],
        ),
    ]
    report = compute_recovery(entries)
    output = format_recovery(report)
    assert "etl_main" in output
    assert "%" in output
