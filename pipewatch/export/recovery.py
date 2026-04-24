"""Recovery rate analysis: how often pipelines recover after failures."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class RecoveryResult:
    pipeline: str
    total_failures: int
    recoveries: int
    recovery_rate: float  # 0.0 – 1.0
    avg_recovery_window: float  # average entries between failure and next success


@dataclass
class RecoveryReport:
    results: List[RecoveryResult]

    def by_pipeline(self, name: str) -> RecoveryResult | None:
        return next((r for r in self.results if r.pipeline == name), None)


def _group_by_pipeline(entries: List[HistoryEntry]) -> dict[str, List[HistoryEntry]]:
    groups: dict[str, List[HistoryEntry]] = {}
    for e in entries:
        for pipeline in e.top_failing:
            groups.setdefault(pipeline, []).append(e)
    return groups


def compute_recovery(
    entries: List[HistoryEntry],
    window: int = 50,
) -> RecoveryReport:
    if not entries:
        return RecoveryReport(results=[])

    recent = entries[-window:]
    groups = _group_by_pipeline(recent)
    results: List[RecoveryResult] = []

    for pipeline, pipeline_entries in groups.items():
        total_failures = 0
        recoveries = 0
        recovery_gaps: List[int] = []
        in_failure = False
        failure_start = 0

        for i, entry in enumerate(pipeline_entries):
            is_failing = pipeline in entry.top_failing and entry.errors > 0
            if is_failing:
                if not in_failure:
                    in_failure = True
                    failure_start = i
                total_failures += 1
            else:
                if in_failure:
                    recoveries += 1
                    recovery_gaps.append(i - failure_start)
                    in_failure = False

        rate = recoveries / total_failures if total_failures > 0 else 0.0
        avg_gap = sum(recovery_gaps) / len(recovery_gaps) if recovery_gaps else 0.0

        results.append(
            RecoveryResult(
                pipeline=pipeline,
                total_failures=total_failures,
                recoveries=recoveries,
                recovery_rate=round(rate, 4),
                avg_recovery_window=round(avg_gap, 2),
            )
        )

    results.sort(key=lambda r: r.recovery_rate)
    return RecoveryReport(results=results)


def format_recovery(report: RecoveryReport) -> str:
    if not report.results:
        return "Recovery Report\n  No data available.\n"

    lines = ["Recovery Report", f"  {'Pipeline':<30} {'Failures':>8} {'Recoveries':>11} {'Rate':>7} {'Avg Gap':>8}"]
    lines.append("  " + "-" * 70)
    for r in report.results:
        rate_pct = f"{r.recovery_rate * 100:.1f}%"
        lines.append(
            f"  {r.pipeline:<30} {r.total_failures:>8} {r.recoveries:>11} {rate_pct:>7} {r.avg_recovery_window:>8.1f}"
        )
    return "\n".join(lines) + "\n"
