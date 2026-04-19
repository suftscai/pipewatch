import pytest
from pipewatch.export.burndown import compute_burndown, format_burndown, BurndownReport
from pipewatch.export.history import HistoryEntry


def _entry(ts: str, failures: dict) -> HistoryEntry:
    total = sum(failures.values())
    return HistoryEntry(
        timestamp=ts,
        total_events=100,
        total_errors=total,
        failure_rate=total / 100,
        failures_by_pipeline=failures,
    )


def test_compute_burndown_empty():
    report = compute_burndown([])
    assert isinstance(report, BurndownReport)
    assert report.points == []
    assert report.total_opened == 0
    assert report.total_resolved == 0


def test_compute_burndown_single_entry():
    entries = [_entry("2024-01-01T00:00:00", {"pipe_a": 5})]
    report = compute_burndown(entries)
    assert len(report.points) == 1
    assert report.points[0].opened == 5
    assert report.points[0].resolved == 0
    assert report.total_opened == 5


def test_compute_burndown_resolved():
    entries = [
        _entry("2024-01-01T00:00:00", {"pipe_a": 10}),
        _entry("2024-01-01T01:00:00", {"pipe_a": 4}),
    ]
    report = compute_burndown(entries)
    pts = {p.timestamp: p for p in report.points if p.pipeline == "pipe_a"}
    assert pts["2024-01-01T00:00:00"].opened == 10
    assert pts["2024-01-01T01:00:00"].resolved == 6
    assert report.total_resolved == 6


def test_compute_burndown_respects_window():
    entries = [_entry(f"2024-01-01T{i:02d}:00:00", {"p": i}) for i in range(10)]
    report = compute_burndown(entries, window=3)
    timestamps = {pt.timestamp for pt in report.points}
    assert "2024-01-01T00:00:00" not in timestamps


def test_format_burndown_no_data():
    report = BurndownReport(points=[], total_opened=0, total_resolved=0)
    out = format_burndown(report)
    assert "no data" in out


def test_format_burndown_contains_headers():
    entries = [_entry("2024-01-01T00:00:00", {"pipe_a": 3})]
    report = compute_burndown(entries)
    out = format_burndown(report)
    assert "Burndown" in out
    assert "pipe_a" in out
    assert "Opened" in out
    assert "Resolved" in out
