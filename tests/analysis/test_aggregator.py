"""Tests for pipewatch.analysis.aggregator."""
import pytest
from datetime import datetime

from pipewatch.ingestion.log_parser import PipelineEvent
from pipewatch.analysis.aggregator import aggregate, PipelineSummary


def _make_event(level: str, pipeline: str = "etl_main") -> PipelineEvent:
    return PipelineEvent(
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        level=level,
        pipeline=pipeline,
        message="test message",
    )


def test_aggregate_empty():
    summary = aggregate([])
    assert summary.total == 0
    assert summary.failures == 0
    assert summary.failure_rate == 0.0


def test_aggregate_counts_errors():
    events = [_make_event("ERROR"), _make_event("INFO"), _make_event("ERROR")]
    summary = aggregate(events)
    assert summary.total == 3
    assert summary.failures == 2
    assert summary.warnings == 0


def test_aggregate_counts_warnings():
    events = [_make_event("WARNING"), _make_event("INFO")]
    summary = aggregate(events)
    assert summary.warnings == 1
    assert summary.failures == 0


def test_failure_rate():
    events = [_make_event("ERROR")] * 3 + [_make_event("INFO")] * 7
    summary = aggregate(events)
    assert abs(summary.failure_rate - 0.3) < 1e-9


def test_by_pipeline_tracking():
    events = [
        _make_event("ERROR", "pipe_a"),
        _make_event("ERROR", "pipe_b"),
        _make_event("ERROR", "pipe_a"),
        _make_event("INFO", "pipe_a"),
    ]
    summary = aggregate(events)
    assert summary.by_pipeline["pipe_a"]["failures"] == 2
    assert summary.by_pipeline["pipe_b"]["failures"] == 1


def test_top_failing():
    events = (
        [_make_event("ERROR", "pipe_a")] * 3
        + [_make_event("ERROR", "pipe_b")] * 5
        + [_make_event("ERROR", "pipe_c")] * 1
    )
    summary = aggregate(events)
    top = summary.top_failing(2)
    assert top[0] == ("pipe_b", 5)
    assert top[1] == ("pipe_a", 3)
