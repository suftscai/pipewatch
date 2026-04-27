"""Tests for pipewatch.export.spillover."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.export.history import HistoryEntry
from pipewatch.export.spillover import (
    compute_spillover,
    format_spillover,
)


def _entry(
    pipeline: str,
    errors: int,
    total: int = 100,
    ts: str = "2024-01-01T00:00:00+00:00",
) -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        error_count=errors,
        warning_count=0,
        failure_rate=errors / total if total else 0.0,
        top_failing={pipeline: errors} if errors else {},
    )


def test_compute_spillover_empty():
    report = compute_spillover([])
    assert report.results == []
    assert not report.has_spillover()


def test_compute_spillover_below_threshold():
    entries = [_entry("pipe_a", 10) for _ in range(3)]
    report = compute_spillover(entries, threshold=50)
    assert len(report.results) == 1
    assert not report.results[0].exceeded
    assert not report.has_spillover()


def test_compute_spillover_above_threshold():
    entries = [_entry("pipe_a", 20) for _ in range(4)]
    report = compute_spillover(entries, threshold=50)
    assert len(report.results) == 1
    result = report.results[0]
    assert result.pipeline == "pipe_a"
    assert result.total_errors == 80
    assert result.exceeded
    assert report.has_spillover()


def test_compute_spillover_respects_window():
    # 10 entries with 20 errors each = 200 total; window=3 => 60 > 50
    entries = [_entry("pipe_b", 20) for _ in range(10)]
    report = compute_spillover(entries, threshold=50, window=3)
    assert report.results[0].total_errors == 60
    assert report.results[0].exceeded


def test_compute_spillover_min_periods_excludes_sparse():
    # Only one period has data for pipe_c
    entries = [_entry("pipe_c", 100)] + [_entry("pipe_d", 5) for _ in range(3)]
    report = compute_spillover(entries, threshold=50, min_periods=2)
    pipelines = {r.pipeline for r in report.results}
    assert "pipe_c" not in pipelines  # only 1 period
    assert "pipe_d" in pipelines


def test_compute_spillover_multiple_pipelines_sorted():
    entries_a = [_entry("alpha", 30) for _ in range(3)]
    entries_b = [_entry("beta", 5) for _ in range(3)]
    merged = []
    for a, b in zip(entries_a, entries_b):
        combined = HistoryEntry(
            timestamp=a.timestamp,
            total_events=a.total_events + b.total_events,
            error_count=a.error_count + b.error_count,
            warning_count=0,
            failure_rate=0.0,
            top_failing={"alpha": 30, "beta": 5},
        )
        merged.append(combined)
    report = compute_spillover(merged, threshold=50)
    assert report.results[0].pipeline == "alpha"
    assert report.results[1].pipeline == "beta"


def test_format_spillover_empty():
    from pipewatch.export.spillover import SpilloverReport
    report = SpilloverReport(results=[], threshold=50, window=5)
    out = format_spillover(report)
    assert "No data" in out


def test_format_spillover_shows_status():
    entries = [_entry("pipe_x", 30) for _ in range(3)]
    report = compute_spillover(entries, threshold=50)
    out = format_spillover(report)
    assert "pipe_x" in out
    assert "SPILLED" in out or "ok" in out
