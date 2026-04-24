"""Tests for pipewatch.export.fingerprint."""
import pytest
from pipewatch.export.fingerprint import (
    _fingerprint,
    compute_fingerprint,
    format_fingerprint,
    FingerprintReport,
)
from pipewatch.export.history import HistoryEntry


def _entry(pipeline: str, errors: list[str], total: int = 10, failed: int = 3) -> HistoryEntry:
    return HistoryEntry(
        timestamp="2024-01-01T00:00:00",
        pipeline=pipeline,
        total_events=total,
        error_count=failed,
        warning_count=0,
        top_errors=errors,
    )


# --- _fingerprint unit tests ---

def test_fingerprint_normalises_numbers():
    msg = "Row 123456 failed validation"
    fp = _fingerprint(msg)
    assert "<NUM>" in fp
    assert "123456" not in fp


def test_fingerprint_normalises_timestamps():
    msg = "Error at 2024-06-15T12:34:56Z in pipeline"
    fp = _fingerprint(msg)
    assert "<TIMESTAMP>" in fp
    assert "2024-06-15" not in fp


def test_fingerprint_normalises_uuid():
    msg = "Job 550e8400-e29b-41d4-a716-446655440000 failed"
    fp = _fingerprint(msg)
    assert "<UUID>" in fp


def test_fingerprint_same_template_produces_same_fp():
    msg1 = "Connection refused to 192.168.1.10"
    msg2 = "Connection refused to 10.0.0.1"
    assert _fingerprint(msg1) == _fingerprint(msg2)


# --- compute_fingerprint tests ---

def test_compute_fingerprint_empty():
    report = compute_fingerprint([])
    assert isinstance(report, FingerprintReport)
    assert report.total_groups == 0
    assert report.entries == []


def test_compute_fingerprint_groups_same_pattern():
    entries = [
        _entry("etl_a", ["Row 100001 failed validation"]),
        _entry("etl_b", ["Row 999999 failed validation"]),
    ]
    report = compute_fingerprint(entries)
    assert report.total_groups == 1
    assert report.entries[0].count == 2
    assert "etl_a" in report.entries[0].pipelines
    assert "etl_b" in report.entries[0].pipelines


def test_compute_fingerprint_distinct_patterns():
    entries = [
        _entry("etl_a", ["Disk full on node 192.168.1.1"]),
        _entry("etl_b", ["Timeout waiting for lock"]),
    ]
    report = compute_fingerprint(entries)
    assert report.total_groups == 2


def test_compute_fingerprint_respects_window():
    entries = [_entry("pipe", ["Error code 12345"]) for _ in range(10)]
    entries += [_entry("pipe", ["Totally different error"]) for _ in range(5)]
    report = compute_fingerprint(entries, window=5)
    # Only the last 5 entries (all "Totally different error") should be included
    assert report.total_groups == 1
    assert "different" in report.entries[0].fingerprint


def test_compute_fingerprint_max_examples_respected():
    entries = [
        _entry("pipe", [f"Row {100000 + i} failed"]) for i in range(10)
    ]
    report = compute_fingerprint(entries, max_examples=2)
    assert len(report.entries[0].examples) <= 2


# --- format_fingerprint tests ---

def test_format_fingerprint_empty():
    report = FingerprintReport(entries=[])
    output = format_fingerprint(report)
    assert "No error patterns" in output


def test_format_fingerprint_shows_count_and_pipeline():
    entries = [
        _entry("etl_a", ["Row 100001 failed validation"]),
        _entry("etl_a", ["Row 200002 failed validation"]),
    ]
    report = compute_fingerprint(entries)
    output = format_fingerprint(report)
    assert "count=2" in output
    assert "etl_a" in output
    assert "pattern" in output
