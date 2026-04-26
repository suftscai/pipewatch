"""Tests for pipewatch.export.pressure."""
import pytest
from pipewatch.export.pressure import (
    compute_pressure,
    format_pressure,
    PressureReport,
)


def _entry(per_pipeline: dict):
    from pipewatch.export.history import HistoryEntry
    return HistoryEntry(
        timestamp="2024-01-01T00:00:00",
        total_events=sum(v.get("total", 0) for v in per_pipeline.values()),
        total_errors=sum(v.get("errors", 0) for v in per_pipeline.values()),
        per_pipeline=per_pipeline,
    )


def test_compute_pressure_empty():
    report = compute_pressure([])
    assert isinstance(report, PressureReport)
    assert report.results == []
    assert not report.has_pressure()


def test_compute_pressure_below_min_periods_excluded():
    entries = [
        _entry({"pipe_a": {"total": 10, "errors": 9}}),
        _entry({"pipe_a": {"total": 10, "errors": 9}}),
    ]
    report = compute_pressure(entries, min_periods=3)
    assert report.results == []


def test_compute_pressure_healthy_pipeline_not_flagged():
    entries = [
        _entry({"pipe_a": {"total": 100, "errors": 1}})
        for _ in range(5)
    ]
    report = compute_pressure(entries, rate_threshold=0.3, pressure_ratio=0.5)
    assert len(report.results) == 1
    assert not report.results[0].under_pressure
    assert not report.has_pressure()


def test_compute_pressure_detects_sustained_failures():
    # 4 out of 5 periods above 30% threshold => ratio 0.8 >= 0.5
    entries = [
        _entry({"pipe_b": {"total": 10, "errors": 4}}),  # 40%
        _entry({"pipe_b": {"total": 10, "errors": 5}}),  # 50%
        _entry({"pipe_b": {"total": 10, "errors": 4}}),  # 40%
        _entry({"pipe_b": {"total": 10, "errors": 4}}),  # 40%
        _entry({"pipe_b": {"total": 10, "errors": 1}}),  # 10%
    ]
    report = compute_pressure(entries, rate_threshold=0.3, pressure_ratio=0.5)
    assert len(report.results) == 1
    result = report.results[0]
    assert result.pipeline == "pipe_b"
    assert result.under_pressure
    assert result.periods_above_threshold == 4
    assert result.total_periods == 5
    assert report.has_pressure()
    assert report.pressured() == [result]


def test_compute_pressure_respects_window():
    old = [_entry({"pipe_c": {"total": 10, "errors": 9}}) for _ in range(10)]
    recent = [_entry({"pipe_c": {"total": 10, "errors": 0}}) for _ in range(5)]
    report = compute_pressure(old + recent, window=5, rate_threshold=0.3, pressure_ratio=0.5)
    assert len(report.results) == 1
    assert not report.results[0].under_pressure


def test_compute_pressure_peak_and_avg_correct():
    entries = [
        _entry({"pipe_d": {"total": 10, "errors": 2}}),  # 20%
        _entry({"pipe_d": {"total": 10, "errors": 8}}),  # 80%
        _entry({"pipe_d": {"total": 10, "errors": 4}}),  # 40%
    ]
    report = compute_pressure(entries, min_periods=3)
    r = report.results[0]
    assert abs(r.avg_failure_rate - 0.4667) < 0.001
    assert r.peak_failure_rate == pytest.approx(0.8, abs=1e-4)


def test_format_pressure_empty():
    report = PressureReport()
    out = format_pressure(report)
    assert "No data" in out


def test_format_pressure_shows_pipelines():
    entries = [_entry({"pipe_e": {"total": 10, "errors": 4}}) for _ in range(4)]
    report = compute_pressure(entries, rate_threshold=0.3, pressure_ratio=0.5)
    out = format_pressure(report)
    assert "pipe_e" in out
    assert "[PRESSURE]" in out
