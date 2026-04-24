"""Tests for pipewatch.export.latency."""
import time
from typing import List

import pytest

from pipewatch.export.history import HistoryEntry
from pipewatch.export.latency import (
    LatencyReport,
    compute_latency,
    format_latency,
)


def _entry(pipeline: str, ts: float, errors: int = 1, total: int = 10) -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        error_count=errors,
        warning_count=0,
        failure_rate=errors / total,
        top_failing=[pipeline],
    )


def test_compute_latency_empty():
    report = compute_latency([])
    assert isinstance(report, LatencyReport)
    assert report.results == []


def test_compute_latency_single_entry_no_gaps():
    entries = [_entry("pipe_a", ts=1000.0)]
    report = compute_latency(entries)
    # Only one timestamp → no gaps → no results
    assert report.results == []


def test_compute_latency_two_entries_gap():
    entries = [
        _entry("pipe_a", ts=1000.0),
        _entry("pipe_a", ts=1060.0),
    ]
    report = compute_latency(entries)
    assert len(report.results) == 1
    r = report.results[0]
    assert r.pipeline == "pipe_a"
    assert r.avg_gap_seconds == pytest.approx(60.0)
    assert r.min_gap_seconds == pytest.approx(60.0)
    assert r.max_gap_seconds == pytest.approx(60.0)
    assert r.sample_count == 1


def test_compute_latency_multiple_gaps():
    entries = [
        _entry("pipe_b", ts=0.0),
        _entry("pipe_b", ts=10.0),
        _entry("pipe_b", ts=40.0),
    ]
    report = compute_latency(entries)
    assert len(report.results) == 1
    r = report.results[0]
    assert r.min_gap_seconds == pytest.approx(10.0)
    assert r.max_gap_seconds == pytest.approx(30.0)
    assert r.avg_gap_seconds == pytest.approx(20.0)
    assert r.sample_count == 2


def test_compute_latency_respects_window():
    entries = [
        _entry("pipe_c", ts=float(i * 5))
        for i in range(20)
    ]
    report_full = compute_latency(entries, window=20)
    report_small = compute_latency(entries, window=3)
    # Smaller window → fewer samples
    assert report_small.results[0].sample_count < report_full.results[0].sample_count


def test_compute_latency_sorted_by_avg_gap():
    entries = [
        _entry("fast", ts=0.0),
        _entry("fast", ts=5.0),
        _entry("slow", ts=0.0),
        _entry("slow", ts=100.0),
    ]
    report = compute_latency(entries)
    assert report.results[0].pipeline == "fast"
    assert report.results[1].pipeline == "slow"


def test_format_latency_empty():
    report = LatencyReport(results=[])
    output = format_latency(report)
    assert "No data" in output


def test_format_latency_shows_pipeline_name():
    from pipewatch.export.latency import LatencyResult
    report = LatencyReport(
        results=[
            LatencyResult(
                pipeline="my_pipeline",
                avg_gap_seconds=30.5,
                min_gap_seconds=10.0,
                max_gap_seconds=60.0,
                sample_count=5,
            )
        ]
    )
    output = format_latency(report)
    assert "my_pipeline" in output
    assert "30.50" in output
