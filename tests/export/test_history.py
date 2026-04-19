"""Tests for pipewatch.export.history."""
from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

from pipewatch.analysis.aggregator import PipelineSummary
from pipewatch.export.history import (
    append_entry,
    load_history,
    recent_failure_trend,
)


def _summary(total=10, errors=2, warnings=1, counts=None) -> PipelineSummary:
    return PipelineSummary(
        total=total,
        errors=errors,
        warnings=warnings,
        stage_counts=Counter(counts or {"stage_a": 2, "stage_b": 1}),
    )


def test_append_creates_file(tmp_path):
    p = tmp_path / "history.jsonl"
    append_entry(p, _summary())
    assert p.exists()


def test_append_entry_fields(tmp_path):
    p = tmp_path / "history.jsonl"
    entry = append_entry(p, _summary(total=20, errors=4, warnings=2))
    assert entry.total == 20
    assert entry.errors == 4
    assert entry.warnings == 2
    assert 0.0 <= entry.failure_rate <= 1.0
    assert isinstance(entry.top_failing, list)
    assert isinstance(entry.timestamp, str)


def test_load_history_empty(tmp_path):
    p = tmp_path / "missing.jsonl"
    assert load_history(p) == []


def test_load_history_roundtrip(tmp_path):
    p = tmp_path / "history.jsonl"
    append_entry(p, _summary(total=10, errors=2, warnings=0))
    append_entry(p, _summary(total=15, errors=5, warnings=1))
    entries = load_history(p)
    assert len(entries) == 2
    assert entries[0].total == 10
    assert entries[1].total == 15


def test_recent_failure_trend_returns_rates(tmp_path):
    p = tmp_path / "history.jsonl"
    for errors in [1, 2, 3]:
        append_entry(p, _summary(total=10, errors=errors))
    trend = recent_failure_trend(p, n=3)
    assert len(trend) == 3
    assert all(isinstance(r, float) for r in trend)
    assert trend == sorted(trend)  # rates should increase with more errors


def test_recent_failure_trend_limited(tmp_path):
    p = tmp_path / "history.jsonl"
    for _ in range(10):
        append_entry(p, _summary())
    trend = recent_failure_trend(p, n=4)
    assert len(trend) == 4


def test_recent_failure_trend_empty(tmp_path):
    """recent_failure_trend on a missing file should return an empty list."""
    p = tmp_path / "missing.jsonl"
    trend = recent_failure_trend(p, n=5)
    assert trend == []


def test_recent_failure_trend_fewer_than_n(tmp_path):
    """When fewer entries exist than n, all available rates are returned."""
    p = tmp_path / "history.jsonl"
    append_entry(p, _summary(total=10, errors=1))
    append_entry(p, _summary(total=10, errors=2))
    trend = recent_failure_trend(p, n=10)
    assert len(trend) == 2
