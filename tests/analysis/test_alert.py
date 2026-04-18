"""Tests for pipewatch.analysis.alert."""
import pytest
from unittest.mock import MagicMock

from pipewatch.analysis.aggregator import PipelineSummary
from pipewatch.analysis.alert import AlertRule, evaluate_alerts, DEFAULT_RULES


def _summary(total: int = 10, failures: int = 0, warnings: int = 0) -> PipelineSummary:
    s = PipelineSummary(total=total, failures=failures, warnings=warnings)
    return s


def test_no_alerts_when_healthy():
    summary = _summary(total=100, failures=1)
    alerts = evaluate_alerts(summary)
    assert alerts == []


def test_high_failure_rate_triggers():
    summary = _summary(total=10, failures=2)  # 20% > 10%
    alerts = evaluate_alerts(summary)
    names = [a.rule_name for a in alerts]
    assert "high_failure_rate" in names


def test_absolute_failures_triggers():
    summary = _summary(total=100, failures=5)
    alerts = evaluate_alerts(summary)
    names = [a.rule_name for a in alerts]
    assert "absolute_failures" in names


def test_both_rules_can_trigger():
    summary = _summary(total=10, failures=5)  # 50% and >= 5
    alerts = evaluate_alerts(summary)
    assert len(alerts) == 2


def test_custom_rules_override_defaults():
    custom = [
        AlertRule(
            name="custom_rule",
            condition=lambda s: s.warnings > 0,
            message="Warnings found",
        )
    ]
    summary = _summary(total=5, warnings=1)
    alerts = evaluate_alerts(summary, rules=custom)
    assert len(alerts) == 1
    assert alerts[0].rule_name == "custom_rule"


def test_evaluate_alerts_returns_message():
    summary = _summary(total=10, failures=5)
    alerts = evaluate_alerts(summary)
    for alert in alerts:
        assert isinstance(alert.message, str)
        assert len(alert.message) > 0
