"""Compare two history snapshots to surface pipeline regressions."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.export.history import HistoryEntry


@dataclass
class PipelineComparison:
    pipeline: str
    prev_failure_rate: float
    curr_failure_rate: float
    delta: float  # positive = got worse
    regression: bool


@dataclass
class ComparisonReport:
    comparisons: List[PipelineComparison]
    new_pipelines: List[str]   # present in current but not previous
    dropped_pipelines: List[str]  # present in previous but not current


def _rate(entry: HistoryEntry, pipeline: str) -> Optional[float]:
    counts = entry.top_failing
    total = entry.total_events
    if total == 0 or pipeline not in counts:
        return None
    return counts[pipeline] / total


def compare_entries(prev: HistoryEntry, curr: HistoryEntry,
                    regression_threshold: float = 0.05) -> ComparisonReport:
    """Compare two HistoryEntry snapshots.

    A regression is flagged when the failure rate for a pipeline increases
    by more than *regression_threshold* (default 5 percentage points).
    """
    prev_pipelines = set(prev.top_failing.keys())
    curr_pipelines = set(curr.top_failing.keys())

    new_pipelines = sorted(curr_pipelines - prev_pipelines)
    dropped_pipelines = sorted(prev_pipelines - curr_pipelines)
    common = prev_pipelines & curr_pipelines

    comparisons: List[PipelineComparison] = []
    for pipeline in sorted(common):
        prev_rate = (prev.top_failing[pipeline] / prev.total_events
                     if prev.total_events else 0.0)
        curr_rate = (curr.top_failing[pipeline] / curr.total_events
                     if curr.total_events else 0.0)
        delta = curr_rate - prev_rate
        comparisons.append(PipelineComparison(
            pipeline=pipeline,
            prev_failure_rate=prev_rate,
            curr_failure_rate=curr_rate,
            delta=delta,
            regression=delta > regression_threshold,
        ))

    return ComparisonReport(
        comparisons=comparisons,
        new_pipelines=new_pipelines,
        dropped_pipelines=dropped_pipelines,
    )


def format_comparison(report: ComparisonReport) -> str:
    lines: List[str] = ["=== Pipeline Comparison ==="]
    if not report.comparisons and not report.new_pipelines and not report.dropped_pipelines:
        lines.append("  No changes detected.")
        return "\n".join(lines)
    for c in report.comparisons:
        flag = " [REGRESSION]" if c.regression else ""
        lines.append(
            f"  {c.pipeline}: {c.prev_failure_rate:.1%} -> {c.curr_failure_rate:.1%}"
            f" (delta {c.delta:+.1%}){flag}"
        )
    for p in report.new_pipelines:
        lines.append(f"  {p}: NEW")
    for p in report.dropped_pipelines:
        lines.append(f"  {p}: DROPPED")
    return "\n".join(lines)
