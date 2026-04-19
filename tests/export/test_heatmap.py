"""Tests for pipewatch.export.heatmap."""
from datetime import datetime, timezone

from pipewatch.export.history import HistoryEntry
from pipewatch.export.heatmap import compute_heatmap, format_heatmap


def _entry(hour: int, failures: dict) -> HistoryEntry:
    ts = datetime(2024, 1, 1, hour, 0, 0, tzinfo=timezone.utc)
    total = sum(failures.values())
    return HistoryEntry(
        timestamp=ts,
        total_events=max(total * 2, 1),
        total_errors=total,
        total_warnings=0,
        failure_rate=total / max(total * 2, 1),
        failures_by_pipeline=failures,
    )


def test_compute_heatmap_empty():
    report = compute_heatmap([])
    assert report.data == {}


def test_compute_heatmap_single_entry():
    entry = _entry(9, {"etl_main": 3})
    report = compute_heatmap([entry])
    assert "etl_main" in report.data
    assert report.data["etl_main"][9] == 3
    assert report.data["etl_main"][10] == 0


def test_compute_heatmap_accumulates_same_hour():
    entries = [
        _entry(14, {"pipe_a": 2}),
        _entry(14, {"pipe_a": 5}),
    ]
    report = compute_heatmap(entries)
    assert report.data["pipe_a"][14] == 7


def test_compute_heatmap_multiple_pipelines():
    entries = [
        _entry(8, {"pipe_a": 1, "pipe_b": 4}),
        _entry(20, {"pipe_a": 2}),
    ]
    report = compute_heatmap(entries)
    assert report.data["pipe_a"][8] == 1
    assert report.data["pipe_b"][8] == 4
    assert report.data["pipe_a"][20] == 2
    assert report.data["pipe_b"][20] == 0


def test_format_heatmap_empty():
    from pipewatch.export.heatmap import HeatmapReport
    output = format_heatmap(HeatmapReport())
    assert "No heatmap data" in output


def test_format_heatmap_contains_pipeline_name():
    entry = _entry(3, {"my_pipeline": 6})
    report = compute_heatmap([entry])
    output = format_heatmap(report)
    assert "my_pipeline" in output
    assert "!" in output  # value >= 5 shows !


def test_format_heatmap_low_value_shows_number():
    entry = _entry(5, {"pipe_x": 2})
    report = compute_heatmap([entry])
    output = format_heatmap(report)
    assert "2" in output
