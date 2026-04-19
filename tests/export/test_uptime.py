"""Tests for pipewatch.export.uptime."""
import pytest
from pipewatch.export.uptime import compute_uptime, format_uptime, UptimeReport
from pipewatch.export.history import HistoryEntry
from datetime import datetime, timezone


def _entry(pipeline: str, total: int, errors: int, ts: str = "2024-01-01T00:00:00") -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        error_count=errors,
        warning_count=0,
        failure_rate=errors / total if total else 0.0,
        top_failing=[pipeline] if errors else [],
        error_counts={pipeline: errors} if errors else {},
        total_events_by_pipeline={pipeline: total},
    )


def test_compute_uptime_empty():
    report = compute_uptime([])
    assert isinstance(report, UptimeReport)
    assert report.results == []


def test_compute_uptime_perfect_uptime():
    entry = _entry("etl_a", total=100, errors=0)
    report = compute_uptime([entry])
    assert len(report.results) == 1
    r = report.results[0]
    assert r.pipeline == "etl_a"
    assert r.uptime_pct == 100.0
    assert r.error_events == 0


def test_compute_uptime_partial_errors():
    entry = _entry("etl_b", total=200, errors=50)
    report = compute_uptime([entry])
    r = report.results[0]
    assert r.uptime_pct == 75.0
    assert r.error_events == 50
    assert r.total_events == 200


def test_compute_uptime_respects_window():
    entries = [_entry("pipe", total=10, errors=10)] * 5 + [_entry("pipe", total=100, errors=0)] * 5
    report = compute_uptime(entries, window=5)
    # Only last 5 entries (all zero errors)
    r = report.results[0]
    assert r.uptime_pct == 100.0


def test_compute_uptime_multiple_pipelines():
    e1 = _entry("alpha", total=100, errors=10)
    e2 = _entry("beta", total=100, errors=0)
    # Merge manually: two entries for two different pipelines
    from pipewatch.export.history import HistoryEntry
    combined = HistoryEntry(
        timestamp="2024-01-01T00:00:00",
        total_events=200,
        error_count=10,
        warning_count=0,
        failure_rate=0.05,
        top_failing=["alpha"],
        error_counts={"alpha": 10, "beta": 0},
        total_events_by_pipeline={"alpha": 100, "beta": 100},
    )
    report = compute_uptime([combined])
    names = [r.pipeline for r in report.results]
    assert "alpha" in names
    assert "beta" in names


def test_format_uptime_empty():
    report = compute_uptime([])
    out = format_uptime(report)
    assert "No uptime data" in out


def test_format_uptime_contains_pipeline_name():
    entry = _entry("my_pipeline", total=50, errors=5)
    report = compute_uptime([entry])
    out = format_uptime(report)
    assert "my_pipeline" in out
    assert "%" in out
