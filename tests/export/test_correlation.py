"""Tests for pipewatch.export.correlation."""
import pytest
from pipewatch.export.correlation import (
    compute_correlation, format_correlation, CorrelationReport
)
from pipewatch.export.history import HistoryEntry
from datetime import datetime, timezone


def _entry(error_counts: dict, total_events: dict) -> HistoryEntry:
    return HistoryEntry(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_errors=sum(error_counts.values()),
        total_warnings=0,
        total_events=total_events,
        error_counts=error_counts,
        top_failing=[],
    )


def test_compute_correlation_empty():
    report = compute_correlation([])
    assert report.pairs == []


def test_compute_correlation_single_pipeline():
    entries = [_entry({"pipe_a": 1}, {"pipe_a": 10}) for _ in range(5)]
    report = compute_correlation(entries)
    assert report.pairs == []  # need at least 2 pipelines


def test_compute_correlation_two_pipelines_positive():
    # Both pipelines spike together -> positive correlation
    entries = [
        _entry({"a": i, "b": i}, {"a": 10, "b": 10})
        for i in range(1, 8)
    ]
    report = compute_correlation(entries)
    assert len(report.pairs) == 1
    pair = report.pairs[0]
    assert pair.pipeline_a == "a"
    assert pair.pipeline_b == "b"
    assert pair.coefficient > 0.9
    assert pair.strength == "strong"


def test_compute_correlation_negative():
    # a goes up as b goes down
    entries = [
        _entry({"a": i, "b": 6 - i}, {"a": 10, "b": 10})
        for i in range(1, 7)
    ]
    report = compute_correlation(entries)
    assert report.pairs[0].coefficient < -0.9


def test_compute_correlation_respects_window():
    # Only last 3 entries used
    entries = [_entry({"a": 5, "b": 1}, {"a": 10, "b": 10})] * 10 + \
              [_entry({"a": i, "b": i}, {"a": 10, "b": 10}) for i in range(1, 4)]
    report = compute_correlation(entries, window=3)
    assert len(report.pairs) == 1


def test_format_correlation_empty():
    report = CorrelationReport(pairs=[])
    out = format_correlation(report)
    assert "No correlation" in out


def test_format_correlation_shows_pairs():
    entries = [
        _entry({"alpha": i, "beta": i}, {"alpha": 10, "beta": 10})
        for i in range(1, 6)
    ]
    report = compute_correlation(entries)
    out = format_correlation(report)
    assert "alpha" in out
    assert "beta" in out
    assert "strong" in out
