"""Focused formatting tests for pipewatch.export.quota.format_quota."""
from __future__ import annotations

from pipewatch.export.quota import QuotaReport, QuotaResult, format_quota


def _result(pipeline: str, errors: int, quota: int) -> QuotaResult:
    exceeded = errors > quota
    return QuotaResult(
        pipeline=pipeline,
        total_errors=errors,
        quota=quota,
        exceeded=exceeded,
        overage=max(0, errors - quota),
    )


def test_format_quota_header_present():
    report = QuotaReport(results=[_result("p", 10, 100)])
    out = format_quota(report)
    assert "Error Quota Report" in out


def test_format_quota_ok_status_when_compliant():
    report = QuotaReport(results=[_result("p", 10, 100)])
    out = format_quota(report)
    assert "OK" in out
    assert "VIOLATIONS" not in out


def test_format_quota_violation_status_when_exceeded():
    report = QuotaReport(results=[_result("p", 150, 100)])
    out = format_quota(report)
    assert "VIOLATIONS DETECTED" in out


def test_format_quota_shows_overage_value():
    report = QuotaReport(results=[_result("pipe_x", 175, 100)])
    out = format_quota(report)
    assert "75" in out  # overage


def test_format_quota_compliant_no_exclamation():
    report = QuotaReport(results=[_result("pipe_ok", 50, 100)])
    out = format_quota(report)
    assert "!!" not in out


def test_format_quota_multiple_pipelines_all_listed():
    report = QuotaReport(
        results=[
            _result("alpha", 200, 100),
            _result("beta", 30, 100),
            _result("gamma", 101, 100),
        ]
    )
    out = format_quota(report)
    assert "alpha" in out
    assert "beta" in out
    assert "gamma" in out
