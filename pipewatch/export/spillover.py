"""Spillover detection: flags pipelines whose error volume exceeds a rolling
capacity threshold across consecutive history entries."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class SpilloverResult:
    pipeline: str
    total_errors: int
    total_events: int
    periods: int
    avg_errors_per_period: float
    exceeded: bool


@dataclass
class SpilloverReport:
    results: List[SpilloverResult] = field(default_factory=list)
    threshold: int = 50
    window: int = 10

    def spilled(self) -> List[SpilloverResult]:
        return [r for r in self.results if r.exceeded]

    def has_spillover(self) -> bool:
        return any(r.exceeded for r in self.results)


def _rate(errors: int, events: int) -> float:
    return errors / events if events else 0.0


def compute_spillover(
    history: List[HistoryEntry],
    threshold: int = 50,
    window: int = 10,
    min_periods: int = 2,
) -> SpilloverReport:
    report = SpilloverReport(threshold=threshold, window=window)
    if not history:
        return report

    recent = history[-window:]

    pipelines: dict[str, list[HistoryEntry]] = {}
    for entry in recent:
        for pipeline in entry.top_failing:
            pipelines.setdefault(pipeline, []).append(entry)

    all_pipelines: set[str] = set()
    for entry in recent:
        all_pipelines.update(entry.top_failing.keys())

    results = []
    for pipeline in sorted(all_pipelines):
        periods = sum(1 for e in recent if pipeline in e.top_failing)
        if periods < min_periods:
            continue
        total_errors = sum(
            e.top_failing.get(pipeline, 0) for e in recent
        )
        total_events = sum(e.total_events for e in recent if pipeline in e.top_failing)
        avg = total_errors / periods
        exceeded = total_errors > threshold
        results.append(
            SpilloverResult(
                pipeline=pipeline,
                total_errors=total_errors,
                total_events=total_events,
                periods=periods,
                avg_errors_per_period=round(avg, 2),
                exceeded=exceeded,
            )
        )

    report.results = sorted(results, key=lambda r: r.total_errors, reverse=True)
    return report


def format_spillover(report: SpilloverReport) -> str:
    lines = [f"=== Spillover Report (threshold={report.threshold}, window={report.window}) ==="]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)
    for r in report.results:
        status = "SPILLED" if r.exceeded else "ok"
        lines.append(
            f"  [{status}] {r.pipeline}: {r.total_errors} errors "
            f"over {r.periods} periods (avg {r.avg_errors_per_period:.1f}/period)"
        )
    return "\n".join(lines)
