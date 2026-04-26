"""Tests for pipewatch.export.heartbeat."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.export.heartbeat import (
    HeartbeatResult,
    HeartbeatReport,
    compute_heartbeat,
    format_heartbeat,
)
from pipewatch.export.history import HistoryEntry


def _entry(pipeline: str, minutes_ago: float, errors: int = 0, total: int = 10) -> HistoryEntry:
    ts = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        error_count=errors,
        warning_count=0,
        failure_rate=errors / total if total else 0.0,
        top_failing=[pipeline],
    )


def test_compute_heartbeat_empty():
    report = compute_heartbeat([])
    assert report.results == []
    assert not report.has_flatlines()


def test_compute_heartbeat_recent_pipeline_ok():
    now = datetime.now(timezone.utc)
    entries = [_entry("pipe-a", minutes_ago=1)]
    report = compute_heartbeat(entries, expected_interval_s=300.0, now=now)
    assert len(report.results) == 1
    r = report.results[0]
    assert r.pipeline == "pipe-a"
    assert r.missed_beats == 0
    assert not r.is_flatline


def test_compute_heartbeat_flatline_detected():
    now = datetime.now(timezone.utc)
    entries = [_entry("pipe-b", minutes_ago=20)]
    report = compute_heartbeat(entries, expected_interval_s=300.0, now=now)
    r = report.results[0]
    # 20 min = 1200s, interval=300s => missed=4
    assert r.missed_beats >= 2
    assert r.is_flatline
    assert report.has_flatlines()


def test_compute_heartbeat_multiple_pipelines_sorted():
    now = datetime.now(timezone.utc)
    entries = [
        _entry("pipe-fast", minutes_ago=1),
        _entry("pipe-slow", minutes_ago=30),
    ]
    report = compute_heartbeat(entries, expected_interval_s=300.0, now=now)
    assert report.results[0].pipeline == "pipe-slow"
    assert report.results[1].pipeline == "pipe-fast"


def test_compute_heartbeat_respects_window():
    now = datetime.now(timezone.utc)
    old_entries = [_entry("pipe-old", minutes_ago=60)] * 5
    new_entry = _entry("pipe-new", minutes_ago=1)
    entries = old_entries + [new_entry]
    report = compute_heartbeat(entries, expected_interval_s=300.0, window=1, now=now)
    pipelines = {r.pipeline for r in report.results}
    assert "pipe-new" in pipelines


def test_flatlines_filters_correctly():
    now = datetime.now(timezone.utc)
    entries = [
        _entry("pipe-ok", minutes_ago=1),
        _entry("pipe-dead", minutes_ago=30),
    ]
    report = compute_heartbeat(entries, expected_interval_s=300.0, now=now)
    flatlines = report.flatlines()
    assert len(flatlines) == 1
    assert flatlines[0].pipeline == "pipe-dead"


def test_format_heartbeat_empty():
    report = HeartbeatReport()
    out = format_heartbeat(report)
    assert "No pipeline data" in out


def test_format_heartbeat_shows_pipelines():
    now = datetime.now(timezone.utc)
    entries = [_entry("pipe-x", minutes_ago=20)]
    report = compute_heartbeat(entries, expected_interval_s=300.0, now=now)
    out = format_heartbeat(report)
    assert "pipe-x" in out
    assert "FLATLINE" in out
