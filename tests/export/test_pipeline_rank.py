import pytest
from pipewatch.export.history import HistoryEntry
from pipewatch.export.pipeline_rank import compute_rank, format_rank


def _entry(error_counts: dict, event_counts: dict) -> HistoryEntry:
    return HistoryEntry(
        timestamp="2024-01-01T00:00:00",
        error_counts=error_counts,
        event_counts=event_counts,
        warning_counts={},
        top_failing=[],
    )


def test_compute_rank_empty():
    report = compute_rank([])
    assert report.rankings == []


def test_compute_rank_single_pipeline():
    entry = _entry({"etl_a": 3}, {"etl_a": 10})
    report = compute_rank([entry])
    assert len(report.rankings) == 1
    r = report.rankings[0]
    assert r.pipeline == "etl_a"
    assert r.rank == 1
    assert abs(r.failure_rate - 0.3) < 1e-6


def test_compute_rank_orders_by_failure_rate():
    entry = _entry(
        {"bad_pipe": 9, "ok_pipe": 1},
        {"bad_pipe": 10, "ok_pipe": 10},
    )
    report = compute_rank([entry])
    assert report.rankings[0].pipeline == "bad_pipe"
    assert report.rankings[1].pipeline == "ok_pipe"


def test_compute_rank_respects_window():
    entries = [
        _entry({"pipe": 10}, {"pipe": 10}),
        _entry({"pipe": 0}, {"pipe": 10}),
    ]
    report = compute_rank(entries, window=1)
    # only last entry: 0 errors out of 10
    assert report.rankings[0].failure_rate == 0.0


def test_compute_rank_zero_total_events():
    entry = _entry({"ghost": 0}, {})
    report = compute_rank([entry])
    assert report.rankings[0].failure_rate == 0.0


def test_format_rank_empty():
    report = compute_rank([])
    out = format_rank(report)
    assert "No pipeline data" in out


def test_format_rank_contains_pipeline_name():
    entry = _entry({"my_pipeline": 2}, {"my_pipeline": 8})
    report = compute_rank([entry])
    out = format_rank(report)
    assert "my_pipeline" in out
    assert "25.0%" in out
