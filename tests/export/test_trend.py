"""Tests for pipewatch.export.trend."""
import pytest
from pipewatch.export.history import HistoryEntry
from pipewatch.export.trend import TrendReport, compute_trend, format_trend


def _entry(errors: int, warnings: int = 0, total: int = 100) -> HistoryEntry:
    return HistoryEntry(
        timestamp="2024-01-01T00:00:00",
        total_events=total,
        error_count=errors,
        warning_count=warnings,
        top_failing=[],
    )


def test_compute_trend_empty():
    report = compute_trend([])
    assert report.total_runs == 0
    assert report.avg_failure_rate == 0.0
    assert report.is_degrading is False


def test_compute_trend_basic():
    entries = [_entry(10), _entry(20), _entry(30)]
    report = compute_trend(entries)
    assert report.total_runs == 3
    assert report.avg_failure_rate == pytest.approx(0.2, rel=1e-3)
    assert report.max_failure_rate == pytest.approx(0.3, rel=1e-3)
    assert report.total_errors == 60


def test_compute_trend_respects_window():
    entries = [_entry(50)] * 5 + [_entry(10)] * 3
    report = compute_trend(entries, window=3)
    assert report.total_runs == 3
    assert report.avg_failure_rate == pytest.approx(0.1, rel=1e-3)


def test_compute_trend_zero_total_events():
    entries = [_entry(0, total=0), _entry(0, total=0)]
    report = compute_trend(entries)
    assert report.avg_failure_rate == 0.0
    assert report.max_failure_rate == 0.0


def test_is_degrading_true():
    # First half: low errors, second half: high errors
    entries = [_entry(2), _entry(3), _entry(40), _entry(50)]
    report = compute_trend(entries)
    assert report.is_degrading is True


def test_is_degrading_false_when_stable():
    entries = [_entry(10), _entry(10), _entry(10), _entry(10)]
    report = compute_trend(entries)
    assert report.is_degrading is False


def test_format_trend_contains_key_info():
    report = TrendReport(
        window=5,
        total_runs=5,
        avg_failure_rate=0.15,
        max_failure_rate=0.30,
        total_errors=75,
        total_warnings=10,
        is_degrading=True,
    )
    output = format_trend(report)
    assert "15.0%" in output
    assert "30.0%" in output
    assert "75" in output
    assert "YES" in output


def test_format_trend_not_degrading():
    report = TrendReport(
        window=5, total_runs=3,
        avg_failure_rate=0.05, max_failure_rate=0.08,
        total_errors=15, total_warnings=2,
        is_degrading=False,
    )
    output = format_trend(report)
    assert "no" in output
