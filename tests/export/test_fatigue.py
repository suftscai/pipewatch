"""Tests for pipewatch.export.fatigue."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

import pytest

from pipewatch.export.fatigue import (
    FatigueReport,
    compute_fatigue,
    format_fatigue,
)
from pipewatch.export.history import HistoryEntry


def _entry(
    ts: str = "2024-01-01T00:00:00+00:00",
    per_pipeline: Dict | None = None,
) -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=0,
        total_errors=0,
        failure_rate=0.0,
        per_pipeline=per_pipeline or {},
    )


def test_compute_fatigue_empty():
    report = compute_fatigue([])
    assert isinstance(report, FatigueReport)
    assert report.results == []
    assert report.fatiguing() == []


def test_compute_fatigue_below_min_events_excluded():
    entry = _entry(per_pipeline={"pipe_a": {"total": 3, "errors": 0, "warnings": 2}})
    report = compute_fatigue([entry], min_events=5)
    assert report.results == []


def test_compute_fatigue_noisy_pipeline_flagged():
    entry = _entry(
        per_pipeline={"noisy_pipe": {"total": 20, "errors": 2, "warnings": 14}}
    )
    report = compute_fatigue([entry], noise_threshold=0.4, min_events=5)
    assert len(report.results) == 1
    result = report.results[0]
    assert result.pipeline == "noisy_pipe"
    assert result.is_fatiguing is True
    assert result.warning_count == 14
    assert result.error_count == 2


def test_compute_fatigue_healthy_pipeline_not_flagged():
    entry = _entry(
        per_pipeline={"stable_pipe": {"total": 20, "errors": 1, "warnings": 1}}
    )
    report = compute_fatigue([entry], noise_threshold=0.4, min_events=5)
    assert len(report.results) == 1
    assert report.results[0].is_fatiguing is False


def test_compute_fatigue_respects_window():
    entries = [
        _entry(per_pipeline={"p": {"total": 10, "errors": 0, "warnings": 8}}),
        _entry(per_pipeline={"p": {"total": 10, "errors": 0, "warnings": 8}}),
        _entry(per_pipeline={"p": {"total": 10, "errors": 0, "warnings": 8}}),
    ]
    report_full = compute_fatigue(entries, window=3, min_events=5)
    report_limited = compute_fatigue(entries, window=1, min_events=5)
    assert report_full.results[0].total_events == 30
    assert report_limited.results[0].total_events == 10


def test_compute_fatigue_sorted_by_noise_score_descending():
    entry = _entry(
        per_pipeline={
            "low_noise": {"total": 10, "errors": 1, "warnings": 1},
            "high_noise": {"total": 10, "errors": 1, "warnings": 7},
        }
    )
    report = compute_fatigue([entry], min_events=5)
    assert report.results[0].pipeline == "high_noise"
    assert report.results[1].pipeline == "low_noise"


def test_format_fatigue_no_data():
    report = FatigueReport(results=[])
    output = format_fatigue(report)
    assert "No data" in output


def test_format_fatigue_shows_noisy_label():
    entry = _entry(
        per_pipeline={"chatty": {"total": 20, "errors": 2, "warnings": 14}}
    )
    report = compute_fatigue([entry], min_events=5)
    output = format_fatigue(report)
    assert "chatty" in output
    assert "[NOISY]" in output
