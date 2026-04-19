"""Tests for pipewatch.export.velocity."""
import pytest
from pipewatch.export.velocity import compute_velocity, format_velocity, VelocityReport


def _entry(timestamp: str, per_pipeline: dict):
    """Minimal HistoryEntry-like object."""
    from types import SimpleNamespace
    return SimpleNamespace(timestamp=timestamp, per_pipeline=per_pipeline)


def test_compute_velocity_empty():
    report = compute_velocity([])
    assert isinstance(report, VelocityReport)
    assert report.points == []


def test_compute_velocity_single_entry():
    entry = _entry("2024-06-01T14:05:00", {"etl_main": {"total": 10, "errors": 2}})
    report = compute_velocity([entry])
    assert len(report.points) == 1
    pt = report.points[0]
    assert pt.pipeline == "etl_main"
    assert pt.hour == "2024-06-01T14"
    assert pt.total_events == 10
    assert pt.error_events == 2


def test_compute_velocity_accumulates_same_hour():
    entries = [
        _entry("2024-06-01T14:00:00", {"pipe_a": {"total": 5, "errors": 1}}),
        _entry("2024-06-01T14:30:00", {"pipe_a": {"total": 3, "errors": 0}}),
    ]
    report = compute_velocity(entries)
    assert len(report.points) == 1
    pt = report.points[0]
    assert pt.total_events == 8
    assert pt.error_events == 1


def test_compute_velocity_multiple_pipelines():
    entries = [
        _entry("2024-06-01T10:00:00", {
            "alpha": {"total": 4, "errors": 1},
            "beta":  {"total": 6, "errors": 3},
        }),
    ]
    report = compute_velocity(entries)
    pipelines = report.pipelines
    assert "alpha" in pipelines
    assert "beta" in pipelines


def test_compute_velocity_respects_window():
    entries = [
        _entry(f"2024-06-01T{h:02d}:00:00", {"p": {"total": 1, "errors": 0}})
        for h in range(10)
    ]
    report = compute_velocity(entries, window=3)
    # Only last 3 entries should be considered
    assert len(report.points) == 3


def test_error_rate_calculation():
    entry = _entry("2024-06-01T09:00:00", {"pipe": {"total": 20, "errors": 5}})
    report = compute_velocity([entry])
    assert abs(report.points[0].error_rate - 0.25) < 1e-9


def test_format_velocity_empty():
    report = compute_velocity([])
    output = format_velocity(report)
    assert "No velocity data" in output


def test_format_velocity_contains_pipeline_name():
    entry = _entry("2024-06-01T08:00:00", {"my_pipeline": {"total": 7, "errors": 2}})
    report = compute_velocity([entry])
    output = format_velocity(report)
    assert "my_pipeline" in output
    assert "2024-06-01T08" in output
