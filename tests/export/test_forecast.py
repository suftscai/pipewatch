"""Tests for pipewatch.export.forecast."""
import pytest
from pipewatch.export.forecast import compute_forecast, format_forecast
from pipewatch.export.history import HistoryEntry


def _entry(pipeline: str, failures: int, total: int, ts: str = "2024-01-01T00:00:00") -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        error_count=failures,
        warning_count=0,
        failure_rate=failures / total if total else 0.0,
        top_failing=[pipeline],
        failures_by_pipeline={pipeline: failures},
    )


def test_compute_forecast_empty():
    report = compute_forecast([], "etl", steps=2)
    assert report.pipeline == "etl"
    assert report.history_points == 0
    assert report.slope == 0.0
    assert len(report.forecast) == 2
    for fp in report.forecast:
        assert fp.predicted_failure_rate == 0.0


def test_compute_forecast_flat_trend():
    history = [_entry("etl", 5, 100, f"2024-01-0{i+1}T00:00:00") for i in range(5)]
    report = compute_forecast(history, "etl", steps=3)
    assert report.history_points == 5
    assert abs(report.slope) < 1e-6
    for fp in report.forecast:
        assert abs(fp.predicted_failure_rate - 0.05) < 0.01


def test_compute_forecast_rising_trend():
    history = [
        _entry("etl", failures, 100, f"2024-01-{i+1:02d}T00:00:00")
        for i, failures in enumerate([1, 2, 3, 4, 5])
    ]
    report = compute_forecast(history, "etl", steps=2)
    assert report.slope > 0
    assert report.forecast[1].predicted_failure_rate > report.forecast[0].predicted_failure_rate


def test_compute_forecast_respects_window():
    history = [_entry("etl", 50, 100, f"2024-01-{i+1:02d}T00:00:00") for i in range(20)]
    report = compute_forecast(history, "etl", steps=1, window=5)
    assert report.history_points == 5


def test_compute_forecast_clamps_to_zero_one():
    history = [_entry("etl", 100, 100, f"2024-01-{i+1:02d}T00:00:00") for i in range(5)]
    report = compute_forecast(history, "etl", steps=3)
    for fp in report.forecast:
        assert 0.0 <= fp.predicted_failure_rate <= 1.0


def test_format_forecast_contains_pipeline_name():
    history = [_entry("myflow", 10, 100)]
    report = compute_forecast(history, "myflow", steps=2)
    output = format_forecast(report)
    assert "myflow" in output
    assert "+1 period" in output
    assert "+2 period" in output
