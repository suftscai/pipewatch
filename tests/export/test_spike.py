"""Tests for pipewatch.export.spike."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

import pytest

from pipewatch.export.history import HistoryEntry
from pipewatch.export.spike import (
    SpikeReport,
    compute_spike,
    format_spike,
    has_spikes,
    spikes,
)


def _entry(pipeline_counts: Dict[str, Dict[str, int]], ts: str = "2024-01-01T00:00:00") -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=sum(sum(v.values()) for v in pipeline_counts.values()),
        total_errors=sum(v.get("errors", 0) for v in pipeline_counts.values()),
        pipeline_counts=pipeline_counts,
    )


def test_compute_spike_empty():
    report = compute_spike([])
    assert isinstance(report, SpikeReport)
    assert report.results == []
    assert not has_spikes(report)


def test_compute_spike_single_entry_no_spike():
    entries = [_entry({"pipe_a": {"errors": 5}})]
    report = compute_spike(entries)
    # Only one entry per pipeline — needs at least 2 to evaluate
    assert report.results == []


def test_compute_spike_stable_no_spike():
    entries = [
        _entry({"pipe_a": {"errors": 4}}),
        _entry({"pipe_a": {"errors": 5}}),
        _entry({"pipe_a": {"errors": 4}}),
        _entry({"pipe_a": {"errors": 5}}),
    ]
    report = compute_spike(entries, multiplier=2.5)
    assert len(report.results) == 1
    result = report.results[0]
    assert not result.is_spike
    assert result.pipeline == "pipe_a"


def test_compute_spike_detects_spike():
    entries = [
        _entry({"pipe_a": {"errors": 2}}),
        _entry({"pipe_a": {"errors": 2}}),
        _entry({"pipe_a": {"errors": 2}}),
        _entry({"pipe_a": {"errors": 20}}),  # spike
    ]
    report = compute_spike(entries, multiplier=2.5, min_mean=1.0)
    assert len(report.results) == 1
    result = report.results[0]
    assert result.is_spike
    assert result.latest_errors == 20
    assert result.ratio >= 2.5


def test_compute_spike_below_min_mean_excluded():
    entries = [
        _entry({"pipe_a": {"errors": 0}}),
        _entry({"pipe_a": {"errors": 0}}),
        _entry({"pipe_a": {"errors": 10}}),
    ]
    report = compute_spike(entries, multiplier=2.5, min_mean=1.0)
    result = report.results[0]
    assert not result.is_spike  # mean < min_mean, excluded from spike


def test_compute_spike_respects_window():
    entries = [
        _entry({"pipe_a": {"errors": 100}}),  # outside window
        _entry({"pipe_a": {"errors": 2}}),
        _entry({"pipe_a": {"errors": 2}}),
        _entry({"pipe_a": {"errors": 2}}),
    ]
    report = compute_spike(entries, window=3, multiplier=2.5)
    result = report.results[0]
    # The 100-error entry is outside the window=3, so no spike
    assert not result.is_spike


def test_has_spikes_and_spikes_filter():
    entries = [
        _entry({"pipe_a": {"errors": 2}, "pipe_b": {"errors": 2}}),
        _entry({"pipe_a": {"errors": 2}, "pipe_b": {"errors": 2}}),
        _entry({"pipe_a": {"errors": 2}, "pipe_b": {"errors": 2}}),
        _entry({"pipe_a": {"errors": 20}, "pipe_b": {"errors": 2}}),
    ]
    report = compute_spike(entries, multiplier=2.5)
    assert has_spikes(report)
    flagged = spikes(report)
    assert len(flagged) == 1
    assert flagged[0].pipeline == "pipe_a"


def test_format_spike_empty():
    report = SpikeReport()
    output = format_spike(report)
    assert "No data" in output


def test_format_spike_shows_entries():
    entries = [
        _entry({"pipe_a": {"errors": 2}}),
        _entry({"pipe_a": {"errors": 2}}),
        _entry({"pipe_a": {"errors": 20}}),
    ]
    report = compute_spike(entries, multiplier=2.5)
    output = format_spike(report)
    assert "pipe_a" in output
    assert "SPIKE" in output
