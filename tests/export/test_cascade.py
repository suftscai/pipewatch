"""Tests for cascade failure detection."""
from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.export.cascade import compute_cascade, format_cascade, CascadeReport
from pipewatch.export.history import HistoryEntry


def _entry(pipeline: str, offset_minutes: float, errors: int = 3, total: int = 10) -> HistoryEntry:
    ts = (datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=offset_minutes)).isoformat()
    return HistoryEntry(
        timestamp=ts,
        pipeline=pipeline,
        total_events=total,
        errors=errors,
        warnings=0,
        failure_rate=errors / total if total else 0.0,
    )


def test_compute_cascade_empty():
    report = compute_cascade([])
    assert isinstance(report, CascadeReport)
    assert not report.has_cascades()


def test_no_cascade_when_single_pipeline():
    entries = [_entry("pipe-a", 0), _entry("pipe-a", 2)]
    report = compute_cascade(entries, window_minutes=5, min_pipelines=2)
    assert not report.has_cascades()


def test_cascade_detected_multiple_pipelines():
    entries = [
        _entry("pipe-a", 0),
        _entry("pipe-b", 1),
        _entry("pipe-c", 2),
    ]
    report = compute_cascade(entries, window_minutes=5, min_pipelines=2)
    assert report.has_cascades()
    assert len(report.windows) == 1
    assert set(report.windows[0].pipelines) == {"pipe-a", "pipe-b", "pipe-c"}


def test_no_cascade_when_spread_apart():
    entries = [
        _entry("pipe-a", 0),
        _entry("pipe-b", 10),
    ]
    report = compute_cascade(entries, window_minutes=5, min_pipelines=2)
    assert not report.has_cascades()


def test_cascade_total_errors_summed():
    entries = [
        _entry("pipe-a", 0, errors=4),
        _entry("pipe-b", 1, errors=6),
    ]
    report = compute_cascade(entries, window_minutes=5, min_pipelines=2)
    assert report.windows[0].total_errors == 10


def test_format_cascade_no_cascades():
    report = CascadeReport(windows=[])
    out = format_cascade(report)
    assert "No cascade" in out


def test_format_cascade_shows_pipelines():
    entries = [
        _entry("pipe-a", 0),
        _entry("pipe-b", 2),
    ]
    report = compute_cascade(entries, window_minutes=5, min_pipelines=2)
    out = format_cascade(report)
    assert "pipe-a" in out
    assert "pipe-b" in out
    assert "errors=" in out
