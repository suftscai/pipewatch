"""Simple threshold-based alerting for pipeline summaries."""
from dataclasses import dataclass
from typing import Callable

from pipewatch.analysis.aggregator import PipelineSummary


@dataclass
class AlertRule:
    name: str
    condition: Callable[[PipelineSummary], bool]
    message: str


@dataclass
class Alert:
    rule_name: str
    message: str


DEFAULT_RULES: list[AlertRule] = [
    AlertRule(
        name="high_failure_rate",
        condition=lambda s: s.failure_rate > 0.1,
        message="Failure rate exceeded 10%",
    ),
    AlertRule(
        name="absolute_failures",
        condition=lambda s: s.failures >= 5,
        message="5 or more failures detected",
    ),
]


def evaluate_alerts(
    summary: PipelineSummary,
    rules: list[AlertRule] | None = None,
) -> list[Alert]:
    """Return a list of triggered alerts for the given summary.

    Args:
        summary: The pipeline summary to evaluate.
        rules: Alert rules to check. Defaults to DEFAULT_RULES if not provided.

    Returns:
        A list of Alert objects for each rule whose condition was met.
    """
    if rules is None:
        rules = DEFAULT_RULES
    triggered: list[Alert] = []
    for rule in rules:
        if rule.condition(summary):
            triggered.append(Alert(rule_name=rule.name, message=rule.message))
    return triggered


def format_alerts(alerts: list[Alert]) -> str:
    """Return a human-readable string summarising triggered alerts.

    Returns an empty string if no alerts were triggered.
    """
    if not alerts:
        return ""
    lines = [f"[{alert.rule_name}] {alert.message}" for alert in alerts]
    return "\n".join(lines)
