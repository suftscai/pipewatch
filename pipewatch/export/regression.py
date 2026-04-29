"""Detect failure-rate regressions between two history windows."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.export.history import HistoryEntry


@dataclass
class RegressionResult:
    pipeline: str
    baseline_rate: float
    current_rate: float
    delta: float
    regressed: bool


@dataclass
class RegressionReport:
    results: List[RegressionResult] = field(default_factory=list)

    def regressions(self) -> List[RegressionResult]:
        return [r for r in self.results if r.regressed]

    def has_regressions(self) -> bool:
        return any(r.regressed for r in self.results)


def _rate(errors: int, total: int) -> float:
    return errors / total if total > 0 else 0.0


def compute_regression(
    history: List[HistoryEntry],
    baseline_window: int = 10,
    current_window: int = 5,
    threshold: float = 0.10,
) -> RegressionReport:
    """Compare recent failure rates against a baseline window.

    Args:
        history: Ordered history entries (oldest first).
        baseline_window: Number of entries to use for baseline.
        current_window: Number of entries to use for current period.
        threshold: Minimum delta to flag as a regression.
    """
    if not history:
        return RegressionReport()

    baseline_entries = history[:baseline_window]
    current_entries = history[-current_window:]

    pipelines: set[str] = set()
    for e in baseline_entries + current_entries:
        pipelines.update(e.failure_counts.keys())
        pipelines.update(e.event_counts.keys())

    results: List[RegressionResult] = []
    for pipeline in sorted(pipelines):
        b_errors = sum(e.failure_counts.get(pipeline, 0) for e in baseline_entries)
        b_total = sum(e.event_counts.get(pipeline, 0) for e in baseline_entries)
        c_errors = sum(e.failure_counts.get(pipeline, 0) for e in current_entries)
        c_total = sum(e.event_counts.get(pipeline, 0) for e in current_entries)

        baseline_rate = _rate(b_errors, b_total)
        current_rate = _rate(c_errors, c_total)
        delta = current_rate - baseline_rate
        regressed = delta >= threshold

        results.append(
            RegressionResult(
                pipeline=pipeline,
                baseline_rate=baseline_rate,
                current_rate=current_rate,
                delta=delta,
                regressed=regressed,
            )
        )

    return RegressionReport(results=results)


def format_regression(report: RegressionReport) -> str:
    lines = ["=== Regression Report ==="]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)

    for r in report.results:
        flag = " [REGRESSED]" if r.regressed else ""
        lines.append(
            f"  {r.pipeline}: baseline={r.baseline_rate:.1%}  "
            f"current={r.current_rate:.1%}  delta={r.delta:+.1%}{flag}"
        )
    return "\n".join(lines)
