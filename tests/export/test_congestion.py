"""Tests for pipewatch.export.congestion."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.export.congestion import (
    compute_congestion,
    format_congestion,
    CongestionReport,
)
from pipewatch.export.history import HistoryEntry


def _entry(pipeline: str, total: int, errors: int = 0) -> HistoryEntry:
    return HistoryEntry(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_events=total,
        total_errors=errors,
        failure_rate=errors / total if total else 0.0,
        top_failing=[],
        per_pipeline={pipeline: {"total": total, "errors": errors}},
    )


def test_compute_congestion_empty():
    report = compute_congestion([])
    assert isinstance(report, CongestionReport)
    assert report.results == []
    assert not report.has_congestion()


def test_compute_congestion_below_threshold():
    history = [_entry("pipe_a", 10) for _ in range(5)]
    report = compute_congestion(history, threshold=100.0)
    assert len(report.results) == 1
    assert not report.results[0].congested
    assert not report.has_congestion()


def test_compute_congestion_above_threshold():
    history = [_entry("pipe_b", 200) for _ in range(5)]
    report = compute_congestion(history, threshold=100.0)
    assert len(report.results) == 1
    assert report.results[0].congested
    assert report.has_congestion()
    assert report.results[0].pipeline == "pipe_b"


def test_compute_congestion_respects_window():
    # Only the last 3 entries should be considered (window=3)
    old = [_entry("pipe_c", 500) for _ in range(10)]
    recent = [_entry("pipe_c", 10) for _ in range(3)]
    history = old + recent
    report = compute_congestion(history, window=3, threshold=100.0, min_entries=3)
    assert not report.has_congestion()
    assert report.results[0].avg_events_per_entry == pytest.approx(10.0)


def test_compute_congestion_min_entries_excludes_sparse():
    history = [_entry("pipe_d", 999)]
    report = compute_congestion(history, threshold=100.0, min_entries=3)
    # Only 1 entry; below min_entries=3, so excluded
    assert report.results == []


def test_compute_congestion_peak_is_max():
    volumes = [50, 80, 300, 90, 70]
    history = [_entry("pipe_e", v) for v in volumes]
    report = compute_congestion(history, threshold=10.0, min_entries=1)
    result = report.results[0]
    assert result.peak_events == 300


def test_format_congestion_empty():
    report = CongestionReport(results=[])
    out = format_congestion(report)
    assert "No data" in out


def test_format_congestion_shows_congested_flag():
    history = [_entry("pipe_f", 200) for _ in range(4)]
    report = compute_congestion(history, threshold=100.0, min_entries=3)
    out = format_congestion(report)
    assert "[CONGESTED]" in out
    assert "pipe_f" in out


def test_format_congestion_sorted_by_avg_desc():
    h_low = [_entry("low", 10) for _ in range(4)]
    h_high = [_entry("high", 500) for _ in range(4)]
    report = compute_congestion(h_low + h_high, threshold=1.0, min_entries=3)
    out = format_congestion(report)
    assert out.index("high") < out.index("low")
