"""Tests for pipewatch.export.mttr."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List
from pipewatch.export.mttr import compute_mttr, format_mttr
from pipewatch.export.history import HistoryEntry


def _entry(pipeline: str, failure_rate: float) -> HistoryEntry:
    return HistoryEntry(
        timestamp="2024-01-01T00:00:00",
        total_events=100,
        error_count=int(failure_rate * 100),
        warning_count=0,
        failure_rate=failure_rate,
        top_failing=[pipeline],
    )


def test_compute_mttr_empty():
    report = compute_mttr([])
    assert report.entries == []
    assert report.window == 24


def test_compute_mttr_no_incidents():
    history = [_entry("etl_a", 0.1) for _ in range(5)]
    report = compute_mttr(history)
    assert len(report.entries) == 1
    assert report.entries[0].incident_count == 0
    assert report.entries[0].mean_recovery_minutes == 0.0


def test_compute_mttr_single_incident_recovered():
    history = [
        _entry("etl_b", 0.1),
        _entry("etl_b", 0.8),  # spike
        _entry("etl_b", 0.9),
        _entry("etl_b", 0.05),  # recovery
        _entry("etl_b", 0.05),
    ]
    report = compute_mttr(history)
    e = report.entries[0]
    assert e.pipeline == "etl_b"
    assert e.incident_count == 1
    assert e.mean_recovery_minutes == 120.0  # 2 steps * 60 min


def test_compute_mttr_open_incident():
    history = [
        _entry("etl_c", 0.05),
        _entry("etl_c", 0.7),  # spike, never recovers
        _entry("etl_c", 0.8),
    ]
    report = compute_mttr(history)
    e = report.entries[0]
    assert e.incident_count == 1
    assert e.mean_recovery_minutes == 0.0  # no completed recovery


def test_compute_mttr_respects_window():
    history = [_entry("etl_d", 0.9) for _ in range(30)]
    report = compute_mttr(history, window=5)
    assert report.window == 5


def test_format_mttr_empty():
    report = compute_mttr([])
    assert "No MTTR" in format_mttr(report)


def test_format_mttr_contains_pipeline():
    history = [_entry("etl_e", 0.1) for _ in range(3)]
    report = compute_mttr(history)
    out = format_mttr(report)
    assert "etl_e" in out
    assert "incident" in out
