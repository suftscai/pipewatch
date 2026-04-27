"""Tests for pipewatch.export.flap."""
from __future__ import annotations

from datetime import datetime, timezone

from pipewatch.export.flap import (
    FlapReport,
    compute_flap,
    format_flap,
)
from pipewatch.export.history import HistoryEntry


def _entry(per_pipeline: dict) -> HistoryEntry:
    return HistoryEntry(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_events=sum(sum(v.values()) for v in per_pipeline.values()),
        total_errors=sum(v.get("errors", 0) for v in per_pipeline.values()),
        per_pipeline=per_pipeline,
    )


# ── helpers ──────────────────────────────────────────────────────────────────

def _ok(pipeline: str) -> dict:
    return {pipeline: {"events": 10, "errors": 0}}


def _fail(pipeline: str) -> dict:
    return {pipeline: {"events": 10, "errors": 5}}


# ── tests ─────────────────────────────────────────────────────────────────────

def test_compute_flap_empty():
    report = compute_flap([])
    assert isinstance(report, FlapReport)
    assert report.results == []
    assert not report.has_flaps()


def test_compute_flap_single_entry_no_flap():
    entries = [_entry(_ok("pipe_a"))]
    report = compute_flap(entries)
    # Only one period — not enough to compute transitions
    assert report.results == []


def test_compute_flap_stable_pipeline_not_flagged():
    # Always failing — no transitions
    entries = [_entry(_fail("pipe_a")) for _ in range(10)]
    report = compute_flap(entries, min_transitions=3, min_flap_rate=0.4)
    assert not report.has_flaps()
    result = report.results[0]
    assert result.transitions == 0
    assert not result.flagged


def test_compute_flap_detects_alternating_pipeline():
    # Alternates ok/fail every period → maximum transitions
    entries = []
    for i in range(10):
        if i % 2 == 0:
            entries.append(_entry(_ok("pipe_a")))
        else:
            entries.append(_entry(_fail("pipe_a")))

    report = compute_flap(entries, min_transitions=3, min_flap_rate=0.4)
    assert report.has_flaps()
    result = report.results[0]
    assert result.pipeline == "pipe_a"
    assert result.transitions == 9  # 10 periods → 9 changes
    assert result.flagged


def test_compute_flap_respects_window():
    # 20 stable entries then 10 alternating — window=10 should only see flapping
    stable = [_entry(_ok("pipe_a")) for _ in range(20)]
    flapping = []
    for i in range(10):
        flapping.append(_entry(_ok("pipe_a") if i % 2 == 0 else _fail("pipe_a")))

    report = compute_flap(stable + flapping, window=10, min_transitions=3, min_flap_rate=0.4)
    assert report.has_flaps()


def test_compute_flap_below_min_transitions_not_flagged():
    # Only 2 transitions — below min_transitions=3
    entries = [
        _entry(_ok("pipe_a")),
        _entry(_fail("pipe_a")),
        _entry(_ok("pipe_a")),
        _entry(_ok("pipe_a")),
        _entry(_ok("pipe_a")),
        _entry(_ok("pipe_a")),
        _entry(_ok("pipe_a")),
        _entry(_ok("pipe_a")),
        _entry(_ok("pipe_a")),
        _entry(_ok("pipe_a")),
    ]
    report = compute_flap(entries, min_transitions=3, min_flap_rate=0.4)
    assert not report.has_flaps()


def test_format_flap_empty():
    report = FlapReport(results=[])
    text = format_flap(report)
    assert "No data" in text


def test_format_flap_shows_flapping_label():
    entries = []
    for i in range(10):
        entries.append(_entry(_ok("pipe_a") if i % 2 == 0 else _fail("pipe_a")))
    report = compute_flap(entries, min_transitions=3, min_flap_rate=0.4)
    text = format_flap(report)
    assert "FLAPPING" in text
    assert "pipe_a" in text
