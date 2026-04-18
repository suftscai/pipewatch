"""Tests for pipewatch.export.comparison."""
import pytest
from pipewatch.export.history import HistoryEntry
from pipewatch.export.comparison import (
    compare_entries,
    format_comparison,
    ComparisonReport,
)


def _entry(total: int, errors: int, top_failing: dict) -> HistoryEntry:
    return HistoryEntry(
        timestamp="2024-01-01T00:00:00",
        total_events=total,
        error_count=errors,
        warning_count=0,
        failure_rate=errors / total if total else 0.0,
        top_failing=top_failing,
    )


def test_no_changes_when_identical():
    e = _entry(100, 10, {"etl_a": 10})
    report = compare_entries(e, e)
    assert len(report.comparisons) == 1
    assert report.comparisons[0].delta == pytest.approx(0.0)
    assert not report.comparisons[0].regression
    assert report.new_pipelines == []
    assert report.dropped_pipelines == []


def test_regression_flagged():
    prev = _entry(100, 5, {"etl_a": 5})
    curr = _entry(100, 20, {"etl_a": 20})
    report = compare_entries(prev, curr, regression_threshold=0.05)
    assert report.comparisons[0].regression is True
    assert report.comparisons[0].delta == pytest.approx(0.15)


def test_improvement_not_flagged():
    prev = _entry(100, 20, {"etl_a": 20})
    curr = _entry(100, 5, {"etl_a": 5})
    report = compare_entries(prev, curr)
    assert not report.comparisons[0].regression
    assert report.comparisons[0].delta < 0


def test_new_pipeline_detected():
    prev = _entry(100, 5, {"etl_a": 5})
    curr = _entry(100, 10, {"etl_a": 5, "etl_b": 5})
    report = compare_entries(prev, curr)
    assert "etl_b" in report.new_pipelines


def test_dropped_pipeline_detected():
    prev = _entry(100, 10, {"etl_a": 5, "etl_b": 5})
    curr = _entry(100, 5, {"etl_a": 5})
    report = compare_entries(prev, curr)
    assert "etl_b" in report.dropped_pipelines


def test_format_comparison_regression_label():
    prev = _entry(100, 5, {"etl_a": 5})
    curr = _entry(100, 20, {"etl_a": 20})
    report = compare_entries(prev, curr)
    text = format_comparison(report)
    assert "REGRESSION" in text
    assert "etl_a" in text


def test_format_comparison_no_changes():
    report = ComparisonReport(comparisons=[], new_pipelines=[], dropped_pipelines=[])
    text = format_comparison(report)
    assert "No changes" in text


def test_zero_total_events_no_crash():
    prev = _entry(0, 0, {})
    curr = _entry(0, 0, {})
    report = compare_entries(prev, curr)
    assert report.comparisons == []
