"""Tests for pipewatch.export.stall."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

import pytest

from pipewatch.export.history import HistoryEntry
from pipewatch.export.stall import (
    StallReport,
    StallResult,
    compute_stall,
    format_stall,
)


def _entry(
    error_counts: Dict[str, int],
    warning_counts: Dict[str, int] | None = None,
    ts: str = "2024-01-01T00:00:00+00:00",
) -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=sum(error_counts.values()),
        error_counts=error_counts,
        warning_counts=warning_counts or {},
        failure_rate=0.0,
    )


def test_compute_stall_empty():
    report = compute_stall([])
    assert isinstance(report, StallReport)
    assert report.results == []
    assert not report.has_stalls


def test_compute_stall_no_stall_stable():
    history = [
        _entry({"pipe_a": 10}),
        _entry({"pipe_a": 10}),
        _entry({"pipe_a": 10}),
        _entry({"pipe_a": 9}),
        _entry({"pipe_a": 10}),
    ]
    report = compute_stall(history, recent_slices=2, drop_threshold=60.0)
    assert not report.has_stalls
    assert len(report.results) == 1
    assert report.results[0].drop_pct < 60.0


def test_compute_stall_detects_stall():
    # historical avg ~10, recent avg ~1 => ~90% drop
    history = [
        _entry({"pipe_b": 10}),
        _entry({"pipe_b": 10}),
        _entry({"pipe_b": 10}),
        _entry({"pipe_b": 10}),
        _entry({"pipe_b": 1}),
        _entry({"pipe_b": 0}),
    ]
    report = compute_stall(history, recent_slices=2, drop_threshold=60.0, min_avg=2.0)
    assert report.has_stalls
    stalled = [r for r in report.results if r.stalled]
    assert len(stalled) == 1
    assert stalled[0].pipeline == "pipe_b"
    assert stalled[0].drop_pct >= 60.0


def test_compute_stall_below_min_avg_excluded():
    # Only 1 event historically — below min_avg=2.0, should not appear
    history = [
        _entry({"rare_pipe": 1}),
        _entry({"rare_pipe": 1}),
        _entry({"rare_pipe": 0}),
    ]
    report = compute_stall(history, min_avg=2.0)
    assert report.results == []


def test_compute_stall_respects_window():
    # Build 30 entries; first 20 have high counts, last 10 low — window=10 sees only low
    history = [_entry({"p": 20})] * 20 + [_entry({"p": 1})] * 10
    report_full = compute_stall(history, window=30, recent_slices=3, drop_threshold=60.0)
    report_small = compute_stall(history, window=10, recent_slices=3, drop_threshold=60.0)
    # With full window the stall should be detected
    assert report_full.has_stalls
    # With window=10 all entries are low, hist_avg ≈ recent_avg → no stall
    assert not report_small.has_stalls


def test_format_stall_empty():
    report = StallReport(results=[])
    out = format_stall(report)
    assert "Stall Detection" in out
    assert "No data" in out


def test_format_stall_shows_entries():
    report = StallReport(
        results=[
            StallResult(
                pipeline="alpha",
                historical_avg=10.0,
                recent_count=1,
                drop_pct=90.0,
                stalled=True,
            )
        ]
    )
    out = format_stall(report)
    assert "alpha" in out
    assert "STALLED" in out
    assert "90.0%" in out
