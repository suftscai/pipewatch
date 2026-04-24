"""Throttle detection: identify pipelines that are emitting events too rapidly."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class ThrottleResult:
    pipeline: str
    events_per_hour: float
    threshold: float
    throttled: bool


@dataclass
class ThrottleReport:
    results: List[ThrottleResult] = field(default_factory=list)

    def throttled(self) -> List[ThrottleResult]:
        return [r for r in self.results if r.throttled]

    def has_throttled(self) -> bool:
        return any(r.throttled for r in self.results)


def _rate(entry: HistoryEntry) -> float:
    """Events per hour for a single history entry (assumes 1-hour buckets)."""
    return float(entry.total_events)


def compute_throttle(
    history: List[HistoryEntry],
    threshold: float = 500.0,
    window: int = 24,
) -> ThrottleReport:
    """Detect pipelines whose average hourly event rate exceeds *threshold*.

    Args:
        history: List of HistoryEntry records, newest last.
        threshold: Maximum allowed events per hour before flagging.
        window: Number of most-recent entries to consider.
    """
    if not history:
        return ThrottleReport()

    recent = history[-window:]

    # Accumulate per-pipeline totals.
    totals: dict[str, float] = {}
    counts: dict[str, int] = {}
    for entry in recent:
        for pipeline, events in entry.events_by_pipeline.items():
            totals[pipeline] = totals.get(pipeline, 0.0) + events
            counts[pipeline] = counts.get(pipeline, 0) + 1

    results: List[ThrottleResult] = []
    for pipeline, total in sorted(totals.items()):
        avg = total / counts[pipeline] if counts[pipeline] else 0.0
        results.append(
            ThrottleResult(
                pipeline=pipeline,
                events_per_hour=round(avg, 2),
                threshold=threshold,
                throttled=avg > threshold,
            )
        )

    return ThrottleReport(results=results)


def format_throttle(report: ThrottleReport) -> str:
    """Return a human-readable string summarising the throttle report."""
    lines = ["=== Throttle Report ==="]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)

    for r in report.results:
        status = "THROTTLED" if r.throttled else "ok"
        lines.append(
            f"  {r.pipeline:<30} {r.events_per_hour:>8.1f} ev/hr  "
            f"(limit {r.threshold:.0f})  [{status}]"
        )
    return "\n".join(lines)
