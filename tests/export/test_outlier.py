"""Tests for pipewatch.export.outlier."""
from __future__ import annotations

from datetime import datetime, timezone

from pipewatch.export.history import HistoryEntry
from pipewatch.export.outlier import compute_outliers, format_outliers


def _entry(
    pipeline_totals: dict,
    pipeline_errors: dict,
    ts: str = "2024-01-01T00:00:00+00:00",
) -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=sum(pipeline_totals.values()),
        total_errors=sum(pipeline_errors.values()),
        failure_rate=0.0,
        pipeline_totals=pipeline_totals,
        pipeline_errors=pipeline_errors,
    )


def test_compute_outliers_empty():
    report = compute_outliers([])
    assert not report.has_outliers()
    assert report.outliers == []


def test_compute_outliers_no_spike():
    entries = [
        _entry({"A": 100, "B": 100}, {"A": 5, "B": 5})
        for _ in range(5)
    ]
    report = compute_outliers(entries)
    assert not report.has_outliers()


def test_compute_outliers_detects_spike():
    entries = [
        _entry({"A": 100, "B": 100, "C": 100}, {"A": 5, "B": 5, "C": 5})
        for _ in range(4)
    ]
    # Last entry: C has a huge spike
    entries.append(
        _entry({"A": 100, "B": 100, "C": 100}, {"A": 5, "B": 5, "C": 90})
    )
    report = compute_outliers(entries, threshold=2.0)
    names = [r.pipeline for r in report.outliers]
    assert "C" in names


def test_compute_outliers_respects_window():
    # Old entries have C spiking, but within window it's normal
    old = [_entry({"A": 100, "C": 100}, {"A": 5, "C": 90}) for _ in range(10)]
    recent = [_entry({"A": 100, "C": 100}, {"A": 5, "C": 5}) for _ in range(5)]
    report = compute_outliers(old + recent, window=5, threshold=2.0)
    assert not report.has_outliers()


def test_format_outliers_no_outliers():
    entries = [_entry({"A": 100}, {"A": 1})]
    report = compute_outliers(entries)
    text = format_outliers(report)
    assert "No outliers" in text


def test_format_outliers_shows_pipeline():
    entries = [
        _entry({"A": 100, "B": 100, "C": 100}, {"A": 5, "B": 5, "C": 5})
        for _ in range(4)
    ]
    entries.append(
        _entry({"A": 100, "B": 100, "C": 100}, {"A": 5, "B": 5, "C": 90})
    )
    report = compute_outliers(entries, threshold=2.0)
    text = format_outliers(report)
    assert "C" in text
    assert "%" in text
