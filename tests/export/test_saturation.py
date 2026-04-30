"""Tests for pipewatch.export.saturation."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.export.history import HistoryEntry
from pipewatch.export.saturation import (
    SaturationReport,
    compute_saturation,
    format_saturation,
    has_saturation,
    saturated,
)


def _entry(
    error_count: int = 0,
    total_events: int = 100,
    top_failing: List[str] | None = None,
) -> HistoryEntry:
    return HistoryEntry(
        timestamp=datetime.now(tz=timezone.utc).isoformat(),
        total_events=total_events,
        error_count=error_count,
        warning_count=0,
        failure_rate=error_count / total_events if total_events else 0.0,
        top_failing=top_failing or [],
    )


def test_compute_saturation_empty():
    report = compute_saturation([])
    assert report.results == []
    assert not has_saturation(report)


def test_compute_saturation_no_saturation():
    entries = [_entry(error_count=5, top_failing=["pipe_a"]) for _ in range(5)]
    report = compute_saturation(entries, threshold=100, saturation_rate=0.8)
    assert not has_saturation(report)
    flagged = saturated(report.results)
    assert flagged == []


def test_compute_saturation_detects_saturated_pipeline():
    # avg errors per entry for pipe_a = 90 (single pipeline so full share)
    entries = [_entry(error_count=90, top_failing=["pipe_a"]) for _ in range(6)]
    report = compute_saturation(entries, threshold=100, saturation_rate=0.8)
    assert has_saturation(report)
    flagged = saturated(report.results)
    assert len(flagged) == 1
    assert flagged[0].pipeline == "pipe_a"
    assert flagged[0].saturated is True


def test_compute_saturation_respects_window():
    # first 5 entries: saturated; last 5 entries: healthy
    old = [_entry(error_count=90, top_failing=["pipe_b"]) for _ in range(5)]
    new = [_entry(error_count=5, top_failing=["pipe_b"]) for _ in range(5)]
    report = compute_saturation(old + new, window=5, threshold=100, saturation_rate=0.8)
    # only the last 5 (healthy) entries should be considered
    assert not has_saturation(report)


def test_compute_saturation_multiple_pipelines_sorted_by_avg():
    entries = [
        _entry(error_count=80, top_failing=["pipe_high", "pipe_low"]),
        _entry(error_count=80, top_failing=["pipe_high", "pipe_low"]),
        _entry(error_count=80, top_failing=["pipe_high", "pipe_low"]),
    ]
    report = compute_saturation(entries, threshold=100, saturation_rate=0.3)
    assert len(report.results) == 2
    # sorted descending by avg_errors
    assert report.results[0].avg_errors >= report.results[1].avg_errors


def test_format_saturation_empty():
    report = SaturationReport(results=[])
    output = format_saturation(report)
    assert "No data" in output


def test_format_saturation_shows_saturated_flag():
    entries = [_entry(error_count=90, top_failing=["pipe_a"]) for _ in range(4)]
    report = compute_saturation(entries, threshold=100, saturation_rate=0.8)
    output = format_saturation(report)
    assert "SATURATED" in output
    assert "pipe_a" in output


def test_format_saturation_no_flag_when_healthy():
    entries = [_entry(error_count=10, top_failing=["pipe_ok"]) for _ in range(4)]
    report = compute_saturation(entries, threshold=100, saturation_rate=0.8)
    output = format_saturation(report)
    assert "SATURATED" not in output
