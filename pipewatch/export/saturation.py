"""Saturation detection: flags pipelines where error volume is consistently
near or above a high-water mark across recent history windows."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class SaturationResult:
    pipeline: str
    avg_errors: float
    peak_errors: int
    threshold: int
    saturated: bool


@dataclass
class SaturationReport:
    results: List[SaturationResult] = field(default_factory=list)


def saturated(results: List[SaturationResult]) -> List[SaturationResult]:
    return [r for r in results if r.saturated]


def has_saturation(report: SaturationReport) -> bool:
    return any(r.saturated for r in report.results)


def _rate(entry: HistoryEntry) -> float:
    return entry.error_count / entry.total_events if entry.total_events else 0.0


def compute_saturation(
    history: List[HistoryEntry],
    window: int = 10,
    threshold: int = 50,
    saturation_rate: float = 0.8,
) -> SaturationReport:
    """Flag pipelines whose average error count exceeds `saturation_rate * threshold`
    across the most recent `window` entries."""
    if not history:
        return SaturationReport()

    recent = history[-window:]
    pipelines: dict[str, list[HistoryEntry]] = {}
    for entry in recent:
        for pipeline in entry.top_failing:
            pipelines.setdefault(pipeline, []).append(entry)

    # collect per-pipeline error counts from top_failing presence as proxy
    # actual counts come from entry error_count weighted by top_failing share
    pipeline_errors: dict[str, list[int]] = {}
    for entry in recent:
        share = 1 / max(len(entry.top_failing), 1) if entry.top_failing else 0
        for pipeline in entry.top_failing:
            pipeline_errors.setdefault(pipeline, []).append(
                int(entry.error_count * share)
            )

    results = []
    for pipeline, counts in pipeline_errors.items():
        avg = sum(counts) / len(counts)
        peak = max(counts)
        is_saturated = avg >= saturation_rate * threshold
        results.append(
            SaturationResult(
                pipeline=pipeline,
                avg_errors=round(avg, 2),
                peak_errors=peak,
                threshold=threshold,
                saturated=is_saturated,
            )
        )

    results.sort(key=lambda r: r.avg_errors, reverse=True)
    return SaturationReport(results=results)


def format_saturation(report: SaturationReport) -> str:
    lines = ["=== Saturation Report ==="]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)
    for r in report.results:
        flag = " [SATURATED]" if r.saturated else ""
        lines.append(
            f"  {r.pipeline}: avg_errors={r.avg_errors} peak={r.peak_errors}"
            f" threshold={r.threshold}{flag}"
        )
    return "\n".join(lines)
