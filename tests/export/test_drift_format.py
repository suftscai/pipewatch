"""Focused formatting tests for pipewatch.export.drift.format_drift."""
from __future__ import annotations

from pipewatch.export.drift import DriftReport, DriftResult, format_drift


def _result(pipeline: str, b: float, r: float, threshold: float = 0.10) -> DriftResult:
    delta = r - b
    return DriftResult(
        pipeline=pipeline,
        baseline_rate=b,
        recent_rate=r,
        delta=delta,
        is_drifting=delta >= threshold,
    )


def test_format_drift_empty_shows_no_data():
    report = DriftReport(results=[])
    text = format_drift(report)
    assert "No pipeline data" in text


def test_format_drift_shows_baseline_and_recent():
    report = DriftReport(results=[_result("pipe_a", 0.05, 0.20)])
    text = format_drift(report)
    assert "baseline=5.0%" in text
    assert "recent=20.0%" in text


def test_format_drift_shows_delta():
    report = DriftReport(results=[_result("pipe_a", 0.10, 0.35)])
    text = format_drift(report)
    assert "delta=+25.0%" in text


def test_format_drift_drifting_flag_present():
    report = DriftReport(results=[_result("critical", 0.02, 0.25)])
    text = format_drift(report)
    assert "[DRIFTING]" in text


def test_format_drift_healthy_no_flag():
    report = DriftReport(results=[_result("healthy", 0.10, 0.12)])
    text = format_drift(report)
    assert "[DRIFTING]" not in text


def test_format_drift_shows_all_pipelines():
    report = DriftReport(
        results=[
            _result("alpha", 0.05, 0.30),
            _result("beta", 0.10, 0.11),
        ]
    )
    text = format_drift(report)
    assert "alpha" in text
    assert "beta" in text
