"""Terminal display formatting for pipeline summ alerts."""
from dataclasses import dataclass
from typing import List

from pipewatch.analysis.aggregator import PipelineSummary
from pipewatch.analysis.alert import Alert

RED = "\033[31m"
YELLOW = "\033[33m"
GREEN = "\033[32m"
BOLD = "\033[1m"
RESET = "\033[0m"


def _color(text: str, code: str, use_color: bool = True) -> str:
    return f"{code}{text}{RESET}" if use_color else text


def _failure_rate_color(rate: float) -> str:
    """Return the appropriate color code for a given failure rate (0.0–1.0)."""
    if rate >= 0.20:
        return RED
    if rate >= 0.10:
        return YELLOW
    return GREEN


def format_summary(summary: PipelineSummary, use_color: bool = True) -> str:
    lines = []
    lines.append(_color("=== Pipeline Summary ===", BOLD, use_color))
    lines.append(f"  Total events : {summary.total}")
    lines.append(f"  Errors       : {_color(str(summary.errors), RED, use_color)}")
    lines.append(f"  Warnings     : {_color(str(summary.warnings), YELLOW, use_color)}")
    rate = summary.failure_rate
    rate_str = f"{rate * 100:.1f}%"
    color = _failure_rate_color(rate)
    lines.append(f"  Failure rate : {_color(rate_str, color, use_color)}")
    if summary.top_failing:
        lines.append("  Top failing pipelines:")
        for name, count in summary.top_failing:
            lines.append(f"    - {name}: {count} failure(s)")
    return "\n".join(lines)


def format_alerts(alerts: List[Alert], use_color: bool = True) -> str:
    if not alerts:
        return _color("[OK] No alerts triggered.", GREEN, use_color)
    lines = [_color("[!] ALERTS", RED, use_color)]
    for alert in alerts:
        lines.append(f"  \u2022 {alert.message}")
    return "\n".join(lines)


def render(summary: PipelineSummary, alerts: List[Alert], use_color: bool = True) -> str:
    parts = [
        format_summary(summary, use_color),
        "",
        format_alerts(alerts, use_color),
    ]
    return "\n".join(parts)
