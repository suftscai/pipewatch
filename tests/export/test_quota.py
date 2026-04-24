"""Tests for pipewatch.export.quota."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

import pytest

from pipewatch.export.history import HistoryEntry
from pipewatch.export.quota import (
    QuotaReport,
    QuotaResult,
    compute_quota,
    format_quota,
)


def _entry(error_counts: Dict[str, int], ts: str = "2024-01-01T00:00:00+00:00") -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=sum(error_counts.values()) + 10,
        total_errors=sum(error_counts.values()),
        error_counts=error_counts,
        top_failing=list(error_counts.keys())[:3],
    )


def test_compute_quota_empty():
    report = compute_quota([])
    assert report.results == []
    assert report.compliant is True


def test_compute_quota_compliant():
    history = [_entry({"etl_a": 10, "etl_b": 5})]
    report = compute_quota(history, quota=50)
    assert report.compliant is True
    for r in report.results:
        assert r.exceeded is False
        assert r.overage == 0


def test_compute_quota_violation():
    history = [_entry({"etl_a": 120})]
    report = compute_quota(history, quota=100)
    assert not report.compliant
    assert len(report.violations) == 1
    v = report.violations[0]
    assert v.pipeline == "etl_a"
    assert v.total_errors == 120
    assert v.overage == 20
    assert v.exceeded is True


def test_compute_quota_accumulates_across_entries():
    history = [
        _entry({"etl_a": 40}),
        _entry({"etl_a": 40}),
        _entry({"etl_a": 40}),
    ]
    report = compute_quota(history, quota=100)
    assert not report.compliant
    r = report.results[0]
    assert r.total_errors == 120
    assert r.overage == 20


def test_compute_quota_respects_window():
    history = [
        _entry({"etl_a": 200}),  # outside window
        _entry({"etl_a": 10}),
        _entry({"etl_a": 10}),
    ]
    report = compute_quota(history, quota=100, window=2)
    r = next(r for r in report.results if r.pipeline == "etl_a")
    assert r.total_errors == 20
    assert r.exceeded is False


def test_format_quota_no_data():
    report = QuotaReport()
    out = format_quota(report)
    assert "No data" in out


def test_format_quota_shows_violations():
    report = QuotaReport(
        results=[
            QuotaResult(pipeline="etl_a", total_errors=150, quota=100, exceeded=True, overage=50),
            QuotaResult(pipeline="etl_b", total_errors=30, quota=100, exceeded=False, overage=0),
        ]
    )
    out = format_quota(report)
    assert "VIOLATIONS DETECTED" in out
    assert "etl_a" in out
    assert "!!" in out
    assert "etl_b" in out
