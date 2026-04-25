"""Congestion detection: flags pipelines with sustained high event volume."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict

from pipewatch.export.history import HistoryEntry


@dataclass
class CongestionResult:
    pipeline: str
    avg_events_per_entry: float
    peak_events: int
    congested: bool


@dataclass
class CongestionReport:
    results: List[CongestionResult] = field(default_factory=list)

    def congested(self) -> List[CongestionResult]:
        return [r for r in self.results if r.congested]

    def has_congestion(self) -> bool:
        return any(r.congested for r in self.results)


def _rate(entry: HistoryEntry) -> int:
    return entry.total_events


def compute_congestion(
    history: List[HistoryEntry],
    window: int = 24,
    threshold: float = 100.0,
    min_entries: int = 3,
) -> CongestionReport:
    """Flag pipelines whose average event volume exceeds *threshold*.

    Args:
        history: List of HistoryEntry records, newest last.
        window: Number of most-recent entries to consider.
        threshold: Average events-per-entry above which a pipeline is congested.
        min_entries: Minimum entries required before flagging.
    """
    if not history:
        return CongestionReport()

    recent = history[-window:]

    buckets: Dict[str, List[int]] = {}
    for entry in recent:
        for pipeline, counts in entry.per_pipeline.items():
            buckets.setdefault(pipeline, []).append(
                counts.get("total", 0)
            )

    results: List[CongestionResult] = []
    for pipeline, volumes in sorted(buckets.items()):
        if len(volumes) < min_entries:
            continue
        avg = sum(volumes) / len(volumes)
        peak = max(volumes)
        results.append(
            CongestionResult(
                pipeline=pipeline,
                avg_events_per_entry=round(avg, 2),
                peak_events=peak,
                congested=avg >= threshold,
            )
        )

    return CongestionReport(results=results)


def format_congestion(report: CongestionReport) -> str:
    lines = ["=== Congestion Report ==="]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)

    for r in sorted(report.results, key=lambda x: -x.avg_events_per_entry):
        flag = " [CONGESTED]" if r.congested else ""
        lines.append(
            f"  {r.pipeline}: avg={r.avg_events_per_entry:.1f} "
            f"peak={r.peak_events}{flag}"
        )
    return "\n".join(lines)
