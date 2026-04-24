"""Tests for pipewatch.export.deadletter."""
from __future__ import annotations

from pipewatch.export.deadletter import (
    compute_deadletter,
    format_deadletter,
    DeadLetterReport,
)
from pipewatch.export.history import HistoryEntry


def _entry(
    pipeline: str,
    failure_rate: float,
    ts: str = "2024-01-01T00:00:00",
    total: int = 10,
    errors: int = 0,
) -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        total_errors=errors,
        total_warnings=0,
        failure_rate=failure_rate,
        top_failing=[pipeline] if failure_rate > 0 else [],
    )


def test_compute_deadletter_empty():
    report = compute_deadletter([])
    assert isinstance(report, DeadLetterReport)
    assert report.entries == []
    assert not report.has_dead_letters()


def test_no_dead_letter_when_healthy():
    history = [_entry("pipe_a", 0.0, ts=f"2024-01-01T0{i}:00:00") for i in range(5)]
    report = compute_deadletter(history, min_consecutive=3)
    assert not report.has_dead_letters()


def test_dead_letter_detected_on_streak():
    history = [
        _entry("pipe_x", 0.5, ts=f"2024-01-01T0{i}:00:00") for i in range(4)
    ]
    report = compute_deadletter(history, min_consecutive=3)
    assert report.has_dead_letters()
    flagged = report.flagged()
    assert len(flagged) == 1
    assert flagged[0].pipeline == "pipe_x"
    assert flagged[0].consecutive_failures == 4


def test_streak_resets_on_recovery():
    history = [
        _entry("pipe_y", 0.8, ts="2024-01-01T00:00:00"),
        _entry("pipe_y", 0.8, ts="2024-01-01T01:00:00"),
        _entry("pipe_y", 0.0, ts="2024-01-01T02:00:00"),
        _entry("pipe_y", 0.8, ts="2024-01-01T03:00:00"),
        _entry("pipe_y", 0.8, ts="2024-01-01T04:00:00"),
    ]
    report = compute_deadletter(history, min_consecutive=3)
    assert not report.has_dead_letters()


def test_window_limits_history():
    history = [
        _entry("pipe_z", 0.9, ts=f"2024-01-01T{i:02d}:00:00") for i in range(10)
    ]
    # window=3 should only see last 3 entries — streak of 3 == min_consecutive=3
    report = compute_deadletter(history, min_consecutive=3, window=3)
    assert report.has_dead_letters()


def test_format_deadletter_empty():
    report = DeadLetterReport(entries=[])
    output = format_deadletter(report)
    assert "Dead-Letter" in output
    assert "No data" in output


def test_format_deadletter_shows_flagged():
    history = [
        _entry("alpha", 1.0, ts=f"2024-01-01T0{i}:00:00") for i in range(5)
    ]
    report = compute_deadletter(history, min_consecutive=3)
    output = format_deadletter(report)
    assert "alpha" in output
    assert "DEAD" in output
    assert "streak=5" in output
