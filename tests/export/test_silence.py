"""Tests for pipewatch.export.silence."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Dict

import pytest

from pipewatch.export.history import HistoryEntry
from pipewatch.export.silence import (
    SilenceReport,
    compute_silence,
    format_silence,
)


def _entry(
    pipeline: str,
    errors: int = 0,
    total: int = 10,
    minutes_ago: float = 0,
    top_failing: Dict[str, int] | None = None,
) -> HistoryEntry:
    ts = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        error_count=errors,
        warning_count=0,
        failure_rate=errors / total if total else 0.0,
        top_failing=top_failing if top_failing is not None else ({pipeline: errors} if errors else {}),
    )


def test_compute_silence_empty():
    report = compute_silence([])
    assert isinstance(report, SilenceReport)
    assert report.results == []
    assert not report.has_silent


def test_no_silence_when_recent():
    entries = [_entry("pipe_a", errors=2, minutes_ago=10)]
    report = compute_silence(entries, window=24, threshold_hours=6.0)
    results = {r.pipeline: r for r in report.results}
    assert "pipe_a" in results
    assert not results["pipe_a"].flagged


def test_silence_detected_when_old():
    entries = [_entry("pipe_b", errors=3, minutes_ago=60 * 8)]  # 8 hours ago
    report = compute_silence(entries, window=24, threshold_hours=6.0)
    results = {r.pipeline: r for r in report.results}
    assert "pipe_b" in results
    assert results["pipe_b"].flagged
    assert results["pipe_b"].hours_silent >= 6.0


def test_silence_threshold_respected():
    entries = [_entry("pipe_c", errors=1, minutes_ago=60 * 3)]  # 3 hours ago
    report = compute_silence(entries, window=24, threshold_hours=6.0)
    results = {r.pipeline: r for r in report.results}
    assert not results["pipe_c"].flagged


def test_sorted_by_silence_descending():
    entries = [
        _entry("pipe_x", errors=1, minutes_ago=60 * 2),
        _entry("pipe_y", errors=1, minutes_ago=60 * 10),
    ]
    report = compute_silence(entries, window=24, threshold_hours=6.0)
    assert report.results[0].pipeline == "pipe_y"
    assert report.results[1].pipeline == "pipe_x"


def test_has_silent_property():
    entries = [_entry("pipe_z", errors=5, minutes_ago=60 * 9)]
    report = compute_silence(entries, window=24, threshold_hours=6.0)
    assert report.has_silent


def test_format_silence_empty():
    report = compute_silence([])
    text = format_silence(report)
    assert "No pipeline data" in text


def test_format_silence_shows_pipeline_name():
    entries = [_entry("critical_pipe", errors=2, minutes_ago=60 * 7)]
    report = compute_silence(entries, window=24, threshold_hours=6.0)
    text = format_silence(report)
    assert "critical_pipe" in text
    assert "SILENT" in text
