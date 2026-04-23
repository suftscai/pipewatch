"""Tests for pipewatch.export.recurrence."""
from datetime import datetime, timezone
from pipewatch.export.history import HistoryEntry
from pipewatch.export.recurrence import (
    compute_recurrence,
    format_recurrence,
    RecurrenceReport,
)


def _entry(pipeline: str, failures: int, total: int = 10) -> HistoryEntry:
    return HistoryEntry(
        timestamp=datetime.now(tz=timezone.utc).isoformat(),
        total_events=total,
        error_count=failures,
        warning_count=0,
        failure_rate=failures / total if total else 0.0,
        top_failing={pipeline: failures} if failures > 0 else {},
    )


def test_compute_recurrence_empty():
    report = compute_recurrence([])
    assert isinstance(report, RecurrenceReport)
    assert report.results == []


def test_compute_recurrence_single_entry_no_failures():
    entries = [_entry("pipe_a", failures=0)]
    report = compute_recurrence(entries)
    # pipeline never appears in top_failing, so no results
    assert report.results == []


def test_compute_recurrence_single_pipeline_all_failures():
    entries = [_entry("pipe_a", failures=3) for _ in range(5)]
    report = compute_recurrence(entries)
    assert len(report.results) == 1
    r = report.results[0]
    assert r.pipeline == "pipe_a"
    assert r.failure_entries == 5
    assert r.total_entries == 5
    assert r.recurrence_rate == 1.0
    assert r.streak == 5


def test_compute_recurrence_streak_resets():
    entries = [
        _entry("pipe_b", failures=2),
        _entry("pipe_b", failures=2),
        _entry("pipe_b", failures=0),
        _entry("pipe_b", failures=2),
    ]
    report = compute_recurrence(entries)
    assert len(report.results) == 1
    r = report.results[0]
    assert r.streak == 2   # longest streak before reset
    assert r.failure_entries == 3
    assert r.recurrence_rate == 0.75


def test_compute_recurrence_respects_window():
    # 15 old clean entries + 5 recent failures
    entries = [_entry("pipe_c", failures=0)] * 15 + [_entry("pipe_c", failures=1)] * 5
    report = compute_recurrence(entries, window=5)
    assert len(report.results) == 1
    r = report.results[0]
    assert r.total_entries == 5
    assert r.failure_entries == 5


def test_flagged_filters_correctly():
    entries = [_entry("pipe_d", failures=1) for _ in range(4)]
    report = compute_recurrence(entries)
    flagged = report.flagged(min_rate=0.5, min_streak=3)
    assert len(flagged) == 1
    assert flagged[0].pipeline == "pipe_d"

    not_flagged = report.flagged(min_rate=0.5, min_streak=10)
    assert not_flagged == []


def test_format_recurrence_no_flagged():
    report = RecurrenceReport(results=[])
    output = format_recurrence(report)
    assert "No recurring" in output


def test_format_recurrence_shows_pipeline():
    entries = [_entry("pipe_e", failures=2) for _ in range(6)]
    report = compute_recurrence(entries)
    output = format_recurrence(report, min_rate=0.5, min_streak=2)
    assert "pipe_e" in output
    assert "100.0%" in output
