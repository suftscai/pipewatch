"""Tests for pipewatch.export.churn."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

import pytest

from pipewatch.export.churn import (
    ChurnReport,
    ChurnResult,
    compute_churn,
    format_churn,
)
from pipewatch.export.history import HistoryEntry


def _entry(
    ts: str,
    top_failing: Dict[str, int],
    total: int = 10,
    errors: int = 0,
) -> HistoryEntry:
    return HistoryEntry(
        timestamp=datetime.fromisoformat(ts).replace(tzinfo=timezone.utc),
        total_events=total,
        total_errors=errors,
        top_failing=top_failing,
    )


def test_compute_churn_empty():
    report = compute_churn([])
    assert isinstance(report, ChurnReport)
    assert report.results == []
    assert not report.has_churn


def test_compute_churn_single_entry_no_churn():
    history = [_entry("2024-01-01T00:00:00", {"pipe_a": 2})]
    report = compute_churn(history)
    # Only one entry per pipeline → skipped (need >= 2)
    assert report.results == []


def test_compute_churn_stable_pipeline_not_flagged():
    # Pipeline fails every entry → no transitions
    history = [
        _entry(f"2024-01-01T{h:02d}:00:00", {"pipe_a": 3}, total=10, errors=3)
        for h in range(6)
    ]
    report = compute_churn(history, min_transitions=3, min_churn_rate=0.4)
    assert not report.has_churn
    if report.results:
        r = report.results[0]
        assert not r.is_churning


def test_compute_churn_detects_alternating_pipeline():
    # Alternates: fail, ok, fail, ok, fail, ok → 5 transitions
    history = []
    for i in range(6):
        if i % 2 == 0:
            history.append(_entry(f"2024-01-01T{i:02d}:00:00", {"pipe_b": 4}, total=10))
        else:
            history.append(_entry(f"2024-01-01T{i:02d}:00:00", {}, total=10))
    report = compute_churn(history, min_transitions=3, min_churn_rate=0.4)
    assert report.has_churn
    churning = report.churning
    assert any(r.pipeline == "pipe_b" for r in churning)


def test_compute_churn_respects_window():
    # 10 entries, window=4 → only last 4 considered
    history = [
        _entry(f"2024-01-01T{i:02d}:00:00", {"pipe_c": 2}, total=10)
        for i in range(10)
    ]
    report_full = compute_churn(history, window=10, min_transitions=3)
    report_small = compute_churn(history, window=2, min_transitions=3)
    # With window=2 there are fewer entries so fewer possible transitions
    if report_small.results:
        assert report_small.results[0].total_entries <= 2


def test_churn_rate_is_transitions_over_entries():
    history = [
        _entry("2024-01-01T00:00:00", {"pipe_d": 5}, total=10),
        _entry("2024-01-01T01:00:00", {}, total=10),
        _entry("2024-01-01T02:00:00", {"pipe_d": 5}, total=10),
        _entry("2024-01-01T03:00:00", {}, total=10),
    ]
    report = compute_churn(history, min_transitions=1, min_churn_rate=0.0)
    assert report.results
    r = next(x for x in report.results if x.pipeline == "pipe_d")
    assert r.transitions == 3
    assert abs(r.churn_rate - 3 / 4) < 1e-6


def test_format_churn_empty_shows_no_data():
    report = ChurnReport(results=[])
    output = format_churn(report)
    assert "No data" in output


def test_format_churn_shows_pipeline_name():
    report = ChurnReport(results=[
        ChurnResult(pipeline="pipe_x", transitions=4, total_entries=8,
                    churn_rate=0.5, is_churning=True)
    ])
    output = format_churn(report)
    assert "pipe_x" in output
    assert "CHURNING" in output
