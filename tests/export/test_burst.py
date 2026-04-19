"""Tests for burst detection."""
import pytest
from pipewatch.export.burst import compute_burst, format_burst, BurstReport
from pipewatch.export.history import HistoryEntry


def _entry(ts: str, pipeline: str, errors: int, total: int) -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        total_errors=errors,
        failure_rate=errors / total if total else 0.0,
        top_failing=[],
        per_pipeline={pipeline: {"errors": errors, "total": total}},
    )


def test_compute_burst_empty():
    report = compute_burst([])
    assert not report.has_bursts()


def test_compute_burst_no_spike():
    entries = [_entry("2024-01-01T10", "pipe_a", 1, 100)]
    report = compute_burst(entries, min_rate=0.5, min_errors=3)
    assert not report.has_bursts()


def test_compute_burst_detects_spike():
    entries = [_entry("2024-01-01T10", "pipe_a", 8, 10)]
    report = compute_burst(entries, min_rate=0.5, min_errors=3)
    assert report.has_bursts()
    assert report.bursts[0].pipeline == "pipe_a"
    assert report.bursts[0].error_count == 8


def test_compute_burst_accumulates_same_hour():
    entries = [
        _entry("2024-01-01T10", "pipe_a", 3, 5),
        _entry("2024-01-01T10", "pipe_a", 3, 5),
    ]
    report = compute_burst(entries, min_rate=0.5, min_errors=3)
    assert report.has_bursts()
    assert report.bursts[0].error_count == 6
    assert report.bursts[0].total_events == 10


def test_compute_burst_respects_window():
    entries = [
        _entry("2024-01-01T08", "pipe_a", 9, 10),
        _entry("2024-01-01T09", "pipe_b", 1, 100),
    ]
    report = compute_burst(entries, window=1, min_rate=0.5, min_errors=3)
    pipelines = {b.pipeline for b in report.bursts}
    assert "pipe_a" not in pipelines


def test_format_burst_no_bursts():
    report = BurstReport()
    assert "No error bursts" in format_burst(report)


def test_format_burst_with_bursts():
    entries = [_entry("2024-01-01T14", "etl_load", 7, 10)]
    report = compute_burst(entries, min_rate=0.5, min_errors=3)
    text = format_burst(report)
    assert "etl_load" in text
    assert "70.0%" in text
