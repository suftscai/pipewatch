"""Tests for pipewatch.export.jitter."""
import pytest
from pipewatch.export.history import HistoryEntry
from pipewatch.export.jitter import (
    JitterReport,
    compute_jitter,
    format_jitter,
    has_jitter,
    jittering,
)


def _entry(pipeline: str, total: int, errors: int, ts: str = "2024-01-01T00:00:00") -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        totals={pipeline: total},
        errors={pipeline: errors},
        warnings={},
    )


def test_compute_jitter_empty():
    report = compute_jitter([])
    assert report.results == []
    assert not has_jitter(report)


def test_compute_jitter_too_few_periods_excluded():
    entries = [
        _entry("pipe-a", 100, 10, "2024-01-01T00:00:00"),
        _entry("pipe-a", 100, 90, "2024-01-01T01:00:00"),
    ]
    report = compute_jitter(entries, min_periods=3)
    assert report.results == []


def test_compute_jitter_stable_pipeline_not_flagged():
    entries = [
        _entry("pipe-a", 100, 10, f"2024-01-01T0{i}:00:00")
        for i in range(5)
    ]
    report = compute_jitter(entries, cv_threshold=0.5, min_periods=3)
    assert len(report.results) == 1
    result = report.results[0]
    assert result.pipeline == "pipe-a"
    assert not result.jittery


def test_compute_jitter_detects_high_variance():
    # alternating 0% and 100% failure rate => very high CV
    entries = [
        _entry("pipe-b", 100, 0,   "2024-01-01T00:00:00"),
        _entry("pipe-b", 100, 100, "2024-01-01T01:00:00"),
        _entry("pipe-b", 100, 0,   "2024-01-01T02:00:00"),
        _entry("pipe-b", 100, 100, "2024-01-01T03:00:00"),
        _entry("pipe-b", 100, 0,   "2024-01-01T04:00:00"),
    ]
    report = compute_jitter(entries, cv_threshold=0.5, min_periods=3)
    assert len(report.results) == 1
    result = report.results[0]
    assert result.jittery
    assert result.cv > 0.5


def test_compute_jitter_respects_window():
    # first 5 entries: stable; last 5 entries: volatile
    stable = [_entry("pipe-c", 100, 10, f"2024-01-01T0{i}:00:00") for i in range(5)]
    volatile = [
        _entry("pipe-c", 100, 0 if i % 2 == 0 else 100, f"2024-01-01T1{i}:00:00")
        for i in range(5)
    ]
    report = compute_jitter(stable + volatile, window=5, cv_threshold=0.5, min_periods=3)
    assert report.results[0].jittery


def test_jittering_filters_correctly():
    entries = [
        _entry("pipe-a", 100, 10, f"2024-01-01T0{i}:00:00") for i in range(5)
    ] + [
        _entry("pipe-b", 100, 0 if i % 2 == 0 else 100, f"2024-01-01T0{i}:00:00")
        for i in range(5)
    ]
    report = compute_jitter(entries, cv_threshold=0.5, min_periods=3)
    flagged = jittering(report)
    assert all(r.jittery for r in flagged)
    assert any(r.pipeline == "pipe-b" for r in flagged)


def test_format_jitter_empty():
    report = JitterReport(results=[], window=10, cv_threshold=0.5, min_periods=3)
    out = format_jitter(report)
    assert "No data" in out


def test_format_jitter_shows_pipeline():
    entries = [
        _entry("pipe-x", 100, 0 if i % 2 == 0 else 100, f"2024-01-01T0{i}:00:00")
        for i in range(5)
    ]
    report = compute_jitter(entries, cv_threshold=0.5, min_periods=3)
    out = format_jitter(report)
    assert "pipe-x" in out
    assert "JITTER" in out
