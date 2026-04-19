"""Tests for pipewatch.export.sla."""
import pytest
from pipewatch.export.history import HistoryEntry
from pipewatch.export.sla import compute_sla, format_sla, SLAReport


def _entry(error_counts, total_events=100, ts="2024-01-01T00:00:00"):
    return HistoryEntry(
        timestamp=ts,
        total_events=total_events,
        error_counts=error_counts,
        warning_counts={},
        top_failing=[],
    )


def test_compute_sla_empty():
    report = compute_sla([])
    assert report.results == []
    assert report.all_compliant


def test_compute_sla_compliant():
    entries = [_entry({"etl_a": 2}, total_events=100)]
    report = compute_sla(entries, threshold=0.05)
    assert len(report.results) == 1
    r = report.results[0]
    assert r.pipeline == "etl_a"
    assert r.compliant is True
    assert pytest.approx(r.failure_rate, abs=1e-6) == 0.02


def test_compute_sla_violation():
    entries = [_entry({"etl_b": 10}, total_events=100)]
    report = compute_sla(entries, threshold=0.05)
    r = report.results[0]
    assert r.compliant is False
    assert report.violations == [r]


def test_compute_sla_multiple_entries_accumulate():
    entries = [
        _entry({"pipe": 3}, total_events=100),
        _entry({"pipe": 4}, total_events=100),
    ]
    report = compute_sla(entries, threshold=0.1)
    r = report.results[0]
    assert r.total_events == 200
    assert r.error_events == 7


def test_compute_sla_respects_window():
    entries = [_entry({"p": 50}, total_events=100)] * 10
    report = compute_sla(entries, threshold=0.05, window=2)
    r = report.results[0]
    assert r.total_events == 200


def test_format_sla_no_data():
    report = SLAReport(results=[], threshold=0.05)
    assert "no data" in format_sla(report)


def test_format_sla_shows_violation():
    entries = [_entry({"bad_pipe": 20}, total_events=100)]
    report = compute_sla(entries, threshold=0.05)
    text = format_sla(report)
    assert "VIOLATION" in text
    assert "bad_pipe" in text


def test_format_sla_shows_ok():
    entries = [_entry({"good_pipe": 1}, total_events=100)]
    report = compute_sla(entries, threshold=0.05)
    text = format_sla(report)
    assert "OK" in text
