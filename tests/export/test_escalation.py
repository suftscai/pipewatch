"""Tests for pipewatch.export.escalation."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.export.escalation import (
    compute_escalation,
    escalating,
    format_escalation,
    has_escalations,
)
from pipewatch.export.history import HistoryEntry


def _entry(
    pipeline: str,
    total: int,
    errors: int,
    ts: str = "2024-01-01T00:00:00+00:00",
) -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        total_errors=errors,
        failure_rate=errors / total if total else 0.0,
        top_failing=[pipeline],
    )


def test_compute_escalation_empty():
    report = compute_escalation([])
    assert report.results == []
    assert not has_escalations(report)


def test_compute_escalation_below_threshold_not_flagged():
    entries = [_entry("pipe_a", 100, 10)] * 5  # 10 % < 50 %
    report = compute_escalation(entries, rate_threshold=0.5, min_periods=3)
    assert not has_escalations(report)
    assert all(not r.escalated for r in report.results)


def test_compute_escalation_detects_streak():
    # 4 consecutive entries above 50 %
    entries = [_entry("pipe_b", 100, 60)] * 4
    report = compute_escalation(entries, rate_threshold=0.5, min_periods=3)
    assert has_escalations(report)
    flagged = escalating(report)
    assert len(flagged) == 1
    assert flagged[0].pipeline == "pipe_b"
    assert flagged[0].consecutive_periods == 4


def test_compute_escalation_streak_breaks_on_recovery():
    # Two bad entries, one good, two bad — streak is only 2 at the end
    entries = [
        _entry("pipe_c", 100, 80),
        _entry("pipe_c", 100, 80),
        _entry("pipe_c", 100, 5),   # recovery
        _entry("pipe_c", 100, 80),
        _entry("pipe_c", 100, 80),
    ]
    report = compute_escalation(entries, rate_threshold=0.5, min_periods=3)
    assert not has_escalations(report)
    result = report.results[0]
    assert result.consecutive_periods == 2


def test_compute_escalation_respects_window():
    # 10 old bad entries + 2 recent good entries; window=5 → only 2 bad in window
    old = [_entry("pipe_d", 100, 90)] * 10
    recent = [_entry("pipe_d", 100, 5)] * 2
    entries = old + recent
    report = compute_escalation(entries, window=5, rate_threshold=0.5, min_periods=3)
    assert not has_escalations(report)


def test_compute_escalation_latest_rate_correct():
    entries = [_entry("pipe_e", 200, 100)] * 3  # exactly 50 %
    report = compute_escalation(entries, rate_threshold=0.5, min_periods=3)
    r = report.results[0]
    assert abs(r.latest_rate - 0.5) < 1e-9


def test_format_escalation_no_data():
    from pipewatch.export.escalation import EscalationReport
    output = format_escalation(EscalationReport())
    assert "No data" in output


def test_format_escalation_shows_escalated_flag():
    entries = [_entry("pipe_f", 100, 70)] * 4
    report = compute_escalation(entries, rate_threshold=0.5, min_periods=3)
    output = format_escalation(report)
    assert "ESCALATED" in output
    assert "pipe_f" in output
