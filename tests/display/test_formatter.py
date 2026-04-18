"""Tests for display formatter."""
import pytest
from pipewatch.analysis.aggregator import PipelineSummary
from pipewatch.analysis.alert import Alert
from pipewatch.display.formatter import format_summary, format_alerts, render


def _summary(**kwargs) -> PipelineSummary:
    defaults = dict(total=100, errors=5, warnings=3, failure_rate=0.05, top_failing=[])
    defaults.update(kwargs)
    return PipelineSummary(**defaults)


def test_format_summary_contains_counts():
    s = _summary(total=50, errors=10, warnings=2)
    out = format_summary(s, use_color=False)
    assert "50" in out
    assert "10" in out
    assert "2" in out


def test_format_summary_failure_rate_percentage():
    s = _summary(failure_rate=0.25)
    out = format_summary(s, use_color=False)
    assert "25.0%" in out


def test_format_summary_top_failing():
    s = _summary(top_failing=[("etl_orders", 7), ("etl_users", 3)])
    out = format_summary(s, use_color=False)
    assert "etl_orders" in out
    assert "7" in out


def test_format_alerts_no_alerts():
    out = format_alerts([], use_color=False)
    assert "No alerts" in out


def test_format_alerts_with_alerts():
    alerts = [Alert(rule_name="high_failure", message="Failure rate too high")]
    out = format_alerts(alerts, use_color=False)
    assert "Failure rate too high" in out
    assert "ALERTS" in out


def test_render_combines_both():
    s = _summary()
    alerts = [Alert(rule_name="r", message="Something broke")]
    out = render(s, alerts, use_color=False)
    assert "Pipeline Summary" in out
    assert "Something broke" in out


def test_render_no_alerts_ok_message():
    s = _summary()
    out = render(s, [], use_color=False)
    assert "No alerts" in out
