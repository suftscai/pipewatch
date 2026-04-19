"""Tests for pipewatch.export.errormap."""
from __future__ import annotations
from pipewatch.export.history import HistoryEntry
from pipewatch.export.errormap import compute_errormap, format_errormap, ErrorMapReport


def _entry(pipeline_messages: dict, pipeline_errors: dict = None) -> HistoryEntry:
    total = sum(sum(len(msgs) for msgs in pipeline_messages.values()), 0) if pipeline_messages else 0
    errors = sum(pipeline_errors.values()) if pipeline_errors else 0
    return HistoryEntry(
        timestamp="2024-01-01T00:00:00",
        total_events=max(total, errors, 1),
        error_count=errors,
        warning_count=0,
        pipeline_errors=pipeline_errors or {},
        error_messages=pipeline_messages,
    )


def test_compute_errormap_empty():
    report = compute_errormap([])
    assert isinstance(report, ErrorMapReport)
    assert report.entries == []


def test_compute_errormap_single_message():
    e = _entry({"pipe_a": ["timeout error"]}, {"pipe_a": 1})
    report = compute_errormap([e])
    assert len(report.entries) == 1
    assert report.entries[0].pipeline == "pipe_a"
    assert report.entries[0].message == "timeout error"
    assert report.entries[0].count == 1


def test_compute_errormap_accumulates_same_message():
    e1 = _entry({"pipe_a": ["conn refused"]}, {"pipe_a": 1})
    e2 = _entry({"pipe_a": ["conn refused"]}, {"pipe_a": 1})
    report = compute_errormap([e1, e2])
    assert report.entries[0].count == 2


def test_compute_errormap_orders_by_count():
    e = _entry({"pipe_a": ["err1", "err1", "err2"]}, {"pipe_a": 3})
    report = compute_errormap([e])
    assert report.entries[0].message == "err1"
    assert report.entries[0].count == 2


def test_compute_errormap_respects_top_n():
    messages = {"pipe_a": [f"err{i}" for i in range(20)]}
    e = _entry(messages, {"pipe_a": 20})
    report = compute_errormap([e], top_n=5)
    assert len(report.entries) <= 5


def test_compute_errormap_respects_window():
    entries = [_entry({"pipe_a": ["old_err"]}) for _ in range(10)]
    new_entry = _entry({"pipe_a": ["new_err", "new_err", "new_err"]})
    all_entries = entries + [new_entry]
    report = compute_errormap(all_entries, window=1)
    assert report.entries[0].message == "new_err"


def test_format_errormap_empty():
    report = ErrorMapReport(entries=[])
    out = format_errormap(report)
    assert "no error" in out.lower()


def test_format_errormap_contains_pipeline_and_message():
    e = _entry({"pipe_x": ["disk full"]}, {"pipe_x": 1})
    report = compute_errormap([e])
    out = format_errormap(report)
    assert "pipe_x" in out
    assert "disk full" in out
