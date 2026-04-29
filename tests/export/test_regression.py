"""Tests for pipewatch.export.regression."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.export.history import HistoryEntry
from pipewatch.export.regression import (
    RegressionReport,
    RegressionResult,
    compute_regression,
    format_regression,
)


def _entry(
    pipeline: str,
    errors: int,
    total: int,
    ts: str = "2024-01-01T00:00:00+00:00",
) -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        total_failures=errors,
        failure_rate=errors / total if total else 0.0,
        top_failing=[],
        failure_counts={pipeline: errors},
        event_counts={pipeline: total},
    )


def test_compute_regression_empty():
    report = compute_regression([])
    assert not report.has_regressions()
    assert report.results == []


def test_compute_regression_no_regression_stable():
    history = [_entry("pipe_a", 1, 10)] * 15
    report = compute_regression(history, baseline_window=10, current_window=5, threshold=0.10)
    assert not report.has_regressions()
    result = report.results[0]
    assert result.pipeline == "pipe_a"
    assert abs(result.delta) < 0.001


def test_compute_regression_detects_spike():
    baseline = [_entry("pipe_b", 1, 100)] * 10  # 1 %
    current = [_entry("pipe_b", 30, 100)] * 5   # 30 %
    history = baseline + current
    report = compute_regression(history, baseline_window=10, current_window=5, threshold=0.10)
    assert report.has_regressions()
    r = report.regressions()[0]
    assert r.pipeline == "pipe_b"
    assert r.delta > 0.10


def test_compute_regression_below_threshold_not_flagged():
    baseline = [_entry("pipe_c", 5, 100)] * 10  # 5 %
    current = [_entry("pipe_c", 10, 100)] * 5   # 10 % — delta = 5 % < threshold 10 %
    history = baseline + current
    report = compute_regression(history, baseline_window=10, current_window=5, threshold=0.10)
    assert not report.has_regressions()


def test_compute_regression_multiple_pipelines():
    baseline_a = [_entry("a", 1, 100)] * 10
    current_a = [_entry("a", 50, 100)] * 5   # regressed
    baseline_b = [_entry("b", 5, 100)] * 10
    current_b = [_entry("b", 6, 100)] * 5   # stable

    def _merge(ea, eb):
        return HistoryEntry(
            timestamp=ea.timestamp,
            total_events=ea.total_events + eb.total_events,
            total_failures=ea.total_failures + eb.total_failures,
            failure_rate=0.0,
            top_failing=[],
            failure_counts={**ea.failure_counts, **eb.failure_counts},
            event_counts={**ea.event_counts, **eb.event_counts},
        )

    history = [_merge(a, b) for a, b in zip(baseline_a + current_a, baseline_b + current_b)]
    report = compute_regression(history, baseline_window=10, current_window=5, threshold=0.10)
    names = {r.pipeline for r in report.regressions()}
    assert "a" in names
    assert "b" not in names


def test_format_regression_empty():
    report = RegressionReport(results=[])
    out = format_regression(report)
    assert "No data" in out


def test_format_regression_shows_pipeline():
    r = RegressionResult(
        pipeline="pipe_x",
        baseline_rate=0.05,
        current_rate=0.30,
        delta=0.25,
        regressed=True,
    )
    report = RegressionReport(results=[r])
    out = format_regression(report)
    assert "pipe_x" in out
    assert "REGRESSED" in out
    assert "5.0%" in out
    assert "30.0%" in out
