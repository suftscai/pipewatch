"""Backpressure detection: identifies pipelines whose error rate is
consistently growing over successive history windows, suggesting
upstream load is overwhelming the pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class BackpressureResult:
    pipeline: str
    rates: List[float]          # failure rates per window period, oldest first
    slope: float                # average per-period increase in failure rate
    is_backpressured: bool


@dataclass
class BackpressureReport:
    results: List[BackpressureResult] = field(default_factory=list)

    def backpressured(self) -> List[BackpressureResult]:
        return [r for r in self.results if r.is_backpressured]

    def has_backpressure(self) -> bool:
        return any(r.is_backpressured for r in self.results)


def _rate(entry: HistoryEntry, pipeline: str) -> float:
    total = entry.total_events
    if total == 0:
        return 0.0
    errors = entry.errors_by_pipeline.get(pipeline, 0)
    return errors / total


def _slope(rates: List[float]) -> float:
    """Mean first-difference (rise per step)."""
    if len(rates) < 2:
        return 0.0
    diffs = [rates[i + 1] - rates[i] for i in range(len(rates) - 1)]
    return sum(diffs) / len(diffs)


def compute_backpressure(
    history: List[HistoryEntry],
    *,
    window: int = 10,
    min_slope: float = 0.02,
    min_periods: int = 3,
) -> BackpressureReport:
    """Detect pipelines with a consistently rising failure rate.

    Args:
        history:     Ordered history entries (oldest first).
        window:      Maximum number of recent entries to consider.
        min_slope:   Minimum average per-period rate increase to flag.
        min_periods: Minimum number of periods required to evaluate.
    """
    entries = history[-window:] if len(history) > window else history
    if not entries:
        return BackpressureReport()

    all_pipelines: set[str] = set()
    for e in entries:
        all_pipelines.update(e.errors_by_pipeline.keys())

    results: List[BackpressureResult] = []
    for pipeline in sorted(all_pipelines):
        rates = [_rate(e, pipeline) for e in entries]
        if len(rates) < min_periods:
            continue
        s = _slope(rates)
        flagged = s >= min_slope
        results.append(BackpressureResult(
            pipeline=pipeline,
            rates=rates,
            slope=round(s, 4),
            is_backpressured=flagged,
        ))

    results.sort(key=lambda r: r.slope, reverse=True)
    return BackpressureReport(results=results)


def format_backpressure(report: BackpressureReport) -> str:
    lines = ["=== Backpressure Report ==="]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)
    for r in report.results:
        flag = " [BACKPRESSURE]" if r.is_backpressured else ""
        lines.append(
            f"  {r.pipeline}: slope={r.slope:+.4f}{flag}"
        )
    return "\n".join(lines)
