"""Pipeline error-rate drift detection.

Compares the most-recent window of history against an earlier baseline
window to surface pipelines whose failure rate is drifting upward.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Sequence

from pipewatch.export.history import HistoryEntry


@dataclass
class DriftResult:
    pipeline: str
    baseline_rate: float   # failure rate in the baseline window
    recent_rate: float     # failure rate in the recent window
    delta: float           # recent_rate - baseline_rate
    is_drifting: bool


@dataclass
class DriftReport:
    results: List[DriftResult] = field(default_factory=list)
    baseline_window: int = 7
    recent_window: int = 3


def has_drift(report: DriftReport) -> bool:
    return any(r.is_drifting for r in report.results)


def _rate(entries: Sequence[HistoryEntry], pipeline: str) -> float:
    total = sum(e.pipeline_totals.get(pipeline, 0) for e in entries)
    errors = sum(e.pipeline_errors.get(pipeline, 0) for e in entries)
    return errors / total if total > 0 else 0.0


def compute_drift(
    history: List[HistoryEntry],
    baseline_window: int = 7,
    recent_window: int = 3,
    threshold: float = 0.10,
) -> DriftReport:
    """Detect pipelines drifting beyond *threshold* delta in failure rate."""
    report = DriftReport(baseline_window=baseline_window, recent_window=recent_window)
    if not history:
        return report

    recent_entries = history[-recent_window:]
    baseline_entries = history[-(baseline_window + recent_window): -recent_window] or history

    pipelines: set[str] = set()
    for e in history:
        pipelines.update(e.pipeline_totals.keys())

    for pipeline in sorted(pipelines):
        b_rate = _rate(baseline_entries, pipeline)
        r_rate = _rate(recent_entries, pipeline)
        delta = r_rate - b_rate
        report.results.append(
            DriftResult(
                pipeline=pipeline,
                baseline_rate=b_rate,
                recent_rate=r_rate,
                delta=delta,
                is_drifting=delta >= threshold,
            )
        )

    report.results.sort(key=lambda r: r.delta, reverse=True)
    return report


def format_drift(report: DriftReport) -> str:
    lines = [
        f"Drift Report  (baseline={report.baseline_window}d / recent={report.recent_window}d)",
        "-" * 60,
    ]
    if not report.results:
        lines.append("  No pipeline data available.")
        return "\n".join(lines)

    for r in report.results:
        flag = " [DRIFTING]" if r.is_drifting else ""
        lines.append(
            f"  {r.pipeline:<30}  baseline={r.baseline_rate:.1%}  "
            f"recent={r.recent_rate:.1%}  delta={r.delta:+.1%}{flag}"
        )
    return "\n".join(lines)
