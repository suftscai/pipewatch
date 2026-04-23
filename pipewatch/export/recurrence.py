"""Detect pipelines with recurring (repeated) failure patterns."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class RecurrenceResult:
    pipeline: str
    total_entries: int
    failure_entries: int
    streak: int          # longest consecutive failure streak
    recurrence_rate: float  # fraction of entries that had failures


@dataclass
class RecurrenceReport:
    results: List[RecurrenceResult] = field(default_factory=list)

    def flagged(self, min_rate: float = 0.5, min_streak: int = 2) -> List[RecurrenceResult]:
        return [
            r for r in self.results
            if r.recurrence_rate >= min_rate and r.streak >= min_streak
        ]


def _group_by_pipeline(entries: List[HistoryEntry]) -> dict[str, List[HistoryEntry]]:
    groups: dict[str, List[HistoryEntry]] = {}
    for e in entries:
        for pipeline in e.top_failing:
            groups.setdefault(pipeline, []).append(e)
    return groups


def _longest_failure_streak(entries: List[HistoryEntry], pipeline: str) -> int:
    streak = max_streak = 0
    for e in entries:
        if e.top_failing.get(pipeline, 0) > 0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak


def compute_recurrence(
    entries: List[HistoryEntry],
    window: int = 20,
) -> RecurrenceReport:
    if not entries:
        return RecurrenceReport()

    recent = entries[-window:]
    groups = _group_by_pipeline(recent)
    results: List[RecurrenceResult] = []

    for pipeline, pipeline_entries in groups.items():
        total = len(pipeline_entries)
        failure_entries = sum(
            1 for e in pipeline_entries if e.top_failing.get(pipeline, 0) > 0
        )
        streak = _longest_failure_streak(pipeline_entries, pipeline)
        rate = failure_entries / total if total > 0 else 0.0
        results.append(RecurrenceResult(
            pipeline=pipeline,
            total_entries=total,
            failure_entries=failure_entries,
            streak=streak,
            recurrence_rate=rate,
        ))

    results.sort(key=lambda r: r.recurrence_rate, reverse=True)
    return RecurrenceReport(results=results)


def format_recurrence(report: RecurrenceReport, min_rate: float = 0.5, min_streak: int = 2) -> str:
    lines = ["=== Recurrence Report ==="]
    flagged = report.flagged(min_rate=min_rate, min_streak=min_streak)
    if not flagged:
        lines.append("No recurring failure patterns detected.")
        return "\n".join(lines)
    lines.append(f"{'Pipeline':<30} {'Rate':>8} {'Streak':>8} {'Failures':>10}")
    lines.append("-" * 60)
    for r in flagged:
        lines.append(
            f"{r.pipeline:<30} {r.recurrence_rate:>7.1%} {r.streak:>8} {r.failure_entries:>10}"
        )
    return "\n".join(lines)
