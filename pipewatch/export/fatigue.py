"""Alert fatigue detection: identifies pipelines that are noisy but low-severity."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class FatigueResult:
    pipeline: str
    total_events: int
    error_count: int
    warning_count: int
    failure_rate: float
    noise_score: float  # high warnings + moderate errors = noisy
    is_fatiguing: bool


@dataclass
class FatigueReport:
    results: List[FatigueResult] = field(default_factory=list)

    def fatiguing(self) -> List[FatigueResult]:
        return [r for r in self.results if r.is_fatiguing]


def _rate(count: int, total: int) -> float:
    return count / total if total > 0 else 0.0


def compute_fatigue(
    history: List[HistoryEntry],
    window: int = 20,
    noise_threshold: float = 0.4,
    min_events: int = 5,
) -> FatigueReport:
    """Detect pipelines that generate excessive warnings relative to errors."""
    recent = history[-window:] if len(history) > window else history

    totals: dict[str, dict] = {}
    for entry in recent:
        for pipeline, counts in entry.per_pipeline.items():
            if pipeline not in totals:
                totals[pipeline] = {"events": 0, "errors": 0, "warnings": 0}
            totals[pipeline]["events"] += counts.get("total", 0)
            totals[pipeline]["errors"] += counts.get("errors", 0)
            totals[pipeline]["warnings"] += counts.get("warnings", 0)

    results: List[FatigueResult] = []
    for pipeline, data in sorted(totals.items()):
        total = data["events"]
        errors = data["errors"]
        warnings = data["warnings"]
        if total < min_events:
            continue
        failure_rate = _rate(errors, total)
        warning_rate = _rate(warnings, total)
        # noise score: weighted combination of warnings and low-severity errors
        noise_score = warning_rate * 0.6 + failure_rate * 0.4
        is_fatiguing = noise_score >= noise_threshold and warning_rate > failure_rate
        results.append(
            FatigueResult(
                pipeline=pipeline,
                total_events=total,
                error_count=errors,
                warning_count=warnings,
                failure_rate=round(failure_rate, 4),
                noise_score=round(noise_score, 4),
                is_fatiguing=is_fatiguing,
            )
        )

    results.sort(key=lambda r: r.noise_score, reverse=True)
    return FatigueReport(results=results)


def format_fatigue(report: FatigueReport) -> str:
    lines = ["=== Alert Fatigue Report ==="]
    if not report.results:
        lines.append("  No data available.")
        return "\n".join(lines)
    for r in report.results:
        flag = " [NOISY]" if r.is_fatiguing else ""
        lines.append(
            f"  {r.pipeline:<30} noise={r.noise_score:.2%}  "
            f"warn={r.warning_count}  err={r.error_count}  "
            f"total={r.total_events}{flag}"
        )
    return "\n".join(lines)
