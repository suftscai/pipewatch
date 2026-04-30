"""Escalation detector: flags pipelines whose failure rate has exceeded a
critical threshold for N or more consecutive history entries."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class EscalationResult:
    pipeline: str
    consecutive_periods: int
    latest_rate: float
    escalated: bool


@dataclass
class EscalationReport:
    results: List[EscalationResult] = field(default_factory=list)


def escalating(report: EscalationReport) -> List[EscalationResult]:
    return [r for r in report.results if r.escalated]


def has_escalations(report: EscalationReport) -> bool:
    return any(r.escalated for r in report.results)


def _rate(total: int, errors: int) -> float:
    return errors / total if total > 0 else 0.0


def _group_by_pipeline(entries: List[HistoryEntry]):
    groups: dict[str, List[HistoryEntry]] = {}
    for e in entries:
        for pipeline in e.top_failing:
            groups.setdefault(pipeline, []).append(e)
    return groups


def compute_escalation(
    entries: List[HistoryEntry],
    *,
    window: int = 20,
    min_periods: int = 3,
    rate_threshold: float = 0.5,
) -> EscalationReport:
    """Flag pipelines that have been above *rate_threshold* for at least
    *min_periods* consecutive entries within the last *window* entries."""
    if not entries:
        return EscalationReport()

    recent = entries[-window:]
    groups = _group_by_pipeline(recent)

    results: List[EscalationResult] = []
    for pipeline, pipe_entries in groups.items():
        # Count the current trailing streak above threshold
        streak = 0
        for e in reversed(pipe_entries):
            r = _rate(e.total_events, e.total_errors)
            if r >= rate_threshold:
                streak += 1
            else:
                break

        latest = pipe_entries[-1]
        latest_rate = _rate(latest.total_events, latest.total_errors)
        escalated = streak >= min_periods
        results.append(
            EscalationResult(
                pipeline=pipeline,
                consecutive_periods=streak,
                latest_rate=latest_rate,
                escalated=escalated,
            )
        )

    results.sort(key=lambda r: r.consecutive_periods, reverse=True)
    return EscalationReport(results=results)


def format_escalation(report: EscalationReport) -> str:
    lines = ["Escalation Report", "-" * 40]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)
    for r in report.results:
        flag = " [ESCALATED]" if r.escalated else ""
        lines.append(
            f"  {r.pipeline}: {r.consecutive_periods} periods at "
            f"{r.latest_rate:.1%}{flag}"
        )
    return "\n".join(lines)
