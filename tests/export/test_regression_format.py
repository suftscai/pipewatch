"""Tests focused on format_regression output formatting."""
from __future__ import annotations

from pipewatch.export.regression import RegressionReport, RegressionResult, format_regression


def _result(pipeline: str, base: float, curr: float, regressed: bool) -> RegressionResult:
    return RegressionResult(
        pipeline=pipeline,
        baseline_rate=base,
        current_rate=curr,
        delta=curr - base,
        regressed=regressed,
    )


def test_format_regression_header_present():
    report = RegressionReport(results=[_result("p", 0.1, 0.2, False)])
    out = format_regression(report)
    assert "Regression Report" in out


def test_format_regression_shows_baseline_and_current():
    report = RegressionReport(results=[_result("alpha", 0.05, 0.25, True)])
    out = format_regression(report)
    assert "5.0%" in out
    assert "25.0%" in out


def test_format_regression_shows_positive_delta():
    report = RegressionReport(results=[_result("beta", 0.10, 0.35, True)])
    out = format_regression(report)
    assert "+25.0%" in out


def test_format_regression_no_flag_when_not_regressed():
    report = RegressionReport(results=[_result("gamma", 0.10, 0.12, False)])
    out = format_regression(report)
    assert "REGRESSED" not in out


def test_format_regression_flag_when_regressed():
    report = RegressionReport(results=[_result("delta", 0.02, 0.50, True)])
    out = format_regression(report)
    assert "[REGRESSED]" in out
    assert "delta" in out


def test_format_regression_multiple_entries_all_shown():
    report = RegressionReport(
        results=[
            _result("pipe1", 0.05, 0.06, False),
            _result("pipe2", 0.05, 0.50, True),
        ]
    )
    out = format_regression(report)
    assert "pipe1" in out
    assert "pipe2" in out
