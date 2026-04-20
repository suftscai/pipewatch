"""Tests for pipewatch.export.bottleneck."""
from __future__ import annotations

from datetime import datetime, timezone

from pipewatch.export.bottleneck import (
    BottleneckReport,
    compute_bottleneck,
    format_bottleneck,
)
from pipewatch.export.history import HistoryEntry


def _entry(
    error_counts: dict,
    event_counts: dict | None = None,
    ts: str = "2024-01-01T00:00:00+00:00",
) -> HistoryEntry:
    if event_counts is None:
        event_counts = {k: max(v, 1) for k, v in error_counts.items()}
    return HistoryEntry(
        timestamp=ts,
        total_events=sum(event_counts.values()),
        total_errors=sum(error_counts.values()),
        failure_rate=0.0,
        top_failing=list(error_counts.keys()),
        error_counts=error_counts,
        event_counts=event_counts,
    )


def test_compute_bottleneck_empty():
    report = compute_bottleneck([])
    assert isinstance(report, BottleneckReport)
    assert report.results == []
    assert not report.has_bottlenecks()


def test_compute_bottleneck_single_occurrence_not_flagged():
    entries = [_entry({"pipe_a": 5}, {"pipe_a": 10})]
    report = compute_bottleneck(entries, min_occurrences=2)
    assert not report.has_bottlenecks()


def test_compute_bottleneck_detects_repeated_failures():
    entries = [
        _entry({"pipe_a": 5}, {"pipe_a": 10}),
        _entry({"pipe_a": 4}, {"pipe_a": 10}),
        _entry({"pipe_a": 6}, {"pipe_a": 10}),
    ]
    report = compute_bottleneck(entries, min_occurrences=2, min_failure_rate=0.1)
    assert report.has_bottlenecks()
    assert report.results[0].pipeline == "pipe_a"
    assert report.results[0].total_errors == 15
    assert report.results[0].occurrences == 3


def test_compute_bottleneck_below_rate_threshold_excluded():
    entries = [
        _entry({"pipe_b": 1}, {"pipe_b": 100}),
        _entry({"pipe_b": 1}, {"pipe_b": 100}),
    ]
    report = compute_bottleneck(entries, min_occurrences=2, min_failure_rate=0.5)
    assert not report.has_bottlenecks()


def test_compute_bottleneck_respects_window():
    entries = [_entry({"pipe_c": 5}, {"pipe_c": 10}) for _ in range(10)]
    report = compute_bottleneck(entries, window=3, min_occurrences=2)
    assert report.window == 3
    assert report.results[0].occurrences == 3


def test_compute_bottleneck_orders_by_failure_rate():
    entries = [
        _entry({"low": 1, "high": 9}, {"low": 10, "high": 10}),
        _entry({"low": 1, "high": 9}, {"low": 10, "high": 10}),
    ]
    report = compute_bottleneck(entries, min_occurrences=2, min_failure_rate=0.0)
    pipelines = [r.pipeline for r in report.results]
    assert pipelines[0] == "high"
    assert pipelines[1] == "low"


def test_format_bottleneck_no_bottlenecks():
    report = BottleneckReport(results=[], window=10)
    output = format_bottleneck(report)
    assert "No bottlenecks" in output


def test_format_bottleneck_shows_pipeline():
    entries = [
        _entry({"etl_main": 8}, {"etl_main": 10}),
        _entry({"etl_main": 7}, {"etl_main": 10}),
    ]
    report = compute_bottleneck(entries, min_occurrences=2, min_failure_rate=0.1)
    output = format_bottleneck(report)
    assert "etl_main" in output
    assert "%" in output
