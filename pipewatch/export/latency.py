"""Latency analysis: measures average time between events per pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class LatencyResult:
    pipeline: str
    avg_gap_seconds: float
    min_gap_seconds: float
    max_gap_seconds: float
    sample_count: int


@dataclass
class LatencyReport:
    results: List[LatencyResult]

    def by_pipeline(self, name: str) -> LatencyResult | None:
        for r in self.results:
            if r.pipeline == name:
                return r
        return None


def _group_timestamps(entries: List[HistoryEntry]) -> dict[str, List[float]]:
    groups: dict[str, List[float]] = {}
    for entry in entries:
        for pipeline in entry.top_failing:
            groups.setdefault(pipeline, [])
        # Use entry timestamp as the event time proxy
        ts = entry.timestamp
        for pipeline in entry.top_failing:
            groups[pipeline].append(ts)
    return groups


def compute_latency(
    entries: List[HistoryEntry],
    window: int = 50,
) -> LatencyReport:
    if not entries:
        return LatencyReport(results=[])

    recent = entries[-window:]
    groups = _group_timestamps(recent)

    results: List[LatencyResult] = []
    for pipeline, timestamps in groups.items():
        sorted_ts = sorted(timestamps)
        if len(sorted_ts) < 2:
            continue
        gaps = [
            sorted_ts[i + 1] - sorted_ts[i]
            for i in range(len(sorted_ts) - 1)
        ]
        results.append(
            LatencyResult(
                pipeline=pipeline,
                avg_gap_seconds=sum(gaps) / len(gaps),
                min_gap_seconds=min(gaps),
                max_gap_seconds=max(gaps),
                sample_count=len(gaps),
            )
        )

    results.sort(key=lambda r: r.avg_gap_seconds)
    return LatencyReport(results=results)


def format_latency(report: LatencyReport) -> str:
    if not report.results:
        return "Latency Report\n  No data available.\n"

    lines = ["Latency Report", f"  {'Pipeline':<30} {'Avg(s)':>8} {'Min(s)':>8} {'Max(s)':>8} {'Samples':>8}"]
    lines.append("  " + "-" * 66)
    for r in report.results:
        lines.append(
            f"  {r.pipeline:<30} {r.avg_gap_seconds:>8.2f} "
            f"{r.min_gap_seconds:>8.2f} {r.max_gap_seconds:>8.2f} {r.sample_count:>8}"
        )
    return "\n".join(lines) + "\n"
