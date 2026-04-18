"""Tests for pipewatch.export.digest."""
import pytest
from pipewatch.export.digest import compute_digest, format_digest, DigestReport


def _entry(total=100, errors=10, warnings=5, top_failing=None):
    """Build a minimal HistoryEntry-like object via a simple namespace."""
    from types import SimpleNamespace
    return SimpleNamespace(
        total_events=total,
        total_errors=errors,
        total_warnings=warnings,
        top_failing=top_failing or [],
    )


def test_compute_digest_empty():
    report = compute_digest([], period_label="week")
    assert report.total_events == 0
    assert report.total_errors == 0
    assert report.overall_failure_rate == 0.0
    assert report.most_failing_pipeline is None
    assert report.entry_count == 0
    assert report.period_label == "week"


def test_compute_digest_single_entry():
    entry = _entry(total=200, errors=20, warnings=10, top_failing=[("etl_a", 20)])
    report = compute_digest([entry])
    assert report.total_events == 200
    assert report.total_errors == 20
    assert report.total_warnings == 10
    assert report.overall_failure_rate == pytest.approx(0.1)
    assert report.most_failing_pipeline == "etl_a"
    assert report.entry_count == 1


def test_compute_digest_multiple_entries():
    entries = [
        _entry(total=100, errors=10, top_failing=[("etl_a", 10)]),
        _entry(total=100, errors=30, top_failing=[("etl_b", 30)]),
    ]
    report = compute_digest(entries)
    assert report.total_events == 200
    assert report.total_errors == 40
    assert report.overall_failure_rate == pytest.approx(0.2)
    assert report.most_failing_pipeline == "etl_b"
    assert report.entry_count == 2


def test_compute_digest_zero_total_events():
    entry = _entry(total=0, errors=0)
    report = compute_digest([entry])
    assert report.overall_failure_rate == 0.0


def test_format_digest_contains_key_fields():
    report = DigestReport(
        period_label="daily",
        total_events=500,
        total_errors=50,
        total_warnings=25,
        overall_failure_rate=0.1,
        most_failing_pipeline="pipe_x",
        entry_count=12,
    )
    text = format_digest(report)
    assert "daily" in text
    assert "500" in text
    assert "50" in text
    assert "10.0%" in text
    assert "pipe_x" in text
    assert "12" in text


def test_format_digest_no_failing_pipeline():
    report = DigestReport(
        period_label="test",
        total_events=0,
        total_errors=0,
        total_warnings=0,
        overall_failure_rate=0.0,
        most_failing_pipeline=None,
        entry_count=0,
    )
    text = format_digest(report)
    assert "n/a" in text
