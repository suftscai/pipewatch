"""Tests for pipewatch.export.watchdog."""
from datetime import datetime, timedelta
from typing import List

import pytest

from pipewatch.export.history import HistoryEntry
from pipewatch.export.watchdog import detect_silent, format_watchdog


def _entry(pipeline: str, minutes_ago: float, now: datetime) -> HistoryEntry:
    ts = now - timedelta(minutes=minutes_ago)
    return HistoryEntry(
        timestamp=ts,
        total_events=100,
        error_count=5,
        warning_count=3,
        failure_rate=0.05,
        top_failing=[pipeline],
    )


NOW = datetime(2024, 6, 1, 12, 0, 0)


def test_no_silent_when_all_recent():
    entries = [_entry("pipe_a", 2, NOW), _entry("pipe_b", 1, NOW)]
    report = detect_silent(entries, threshold_seconds=300, now=NOW)
    assert not report.has_silent
    assert report.silent == []


def test_silent_pipeline_detected():
    entries = [_entry("pipe_old", 10, NOW), _entry("pipe_new", 1, NOW)]
    report = detect_silent(entries, threshold_seconds=300, now=NOW)
    assert report.has_silent
    assert len(report.silent) == 1
    assert report.silent[0].pipeline == "pipe_old"
    assert report.silent[0].silent_for_seconds == pytest.approx(600.0, abs=1)


def test_empty_history_no_silent():
    report = detect_silent([], threshold_seconds=300, now=NOW)
    assert not report.has_silent


def test_sorted_by_silence_descending():
    entries = [
        _entry("pipe_a", 20, NOW),
        _entry("pipe_b", 30, NOW),
        _entry("pipe_c", 10, NOW),
    ]
    report = detect_silent(entries, threshold_seconds=60, now=NOW)
    assert [s.pipeline for s in report.silent] == ["pipe_b", "pipe_a", "pipe_c"]


def test_format_watchdog_no_silent():
    entries = [_entry("pipe_a", 1, NOW)]
    report = detect_silent(entries, threshold_seconds=300, now=NOW)
    out = format_watchdog(report)
    assert "All pipelines active" in out


def test_format_watchdog_with_silent():
    entries = [_entry("pipe_old", 10, NOW)]
    report = detect_silent(entries, threshold_seconds=300, now=NOW)
    out = format_watchdog(report)
    assert "pipe_old" in out
    assert "10.0 min ago" in out
