"""Burst detection: identify time windows with unusually high error counts."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List
from pipewatch.export.history import HistoryEntry


@dataclass
class BurstWindow:
    pipeline: str
    hour: str
    error_count: int
    total_events: int
    rate: float


@dataclass
class BurstReport:
    bursts: List[BurstWindow] = field(default_factory=list)

    def has_bursts(self) -> bool:
        return len(self.bursts) > 0


def _hour_key(ts: str) -> str:
    return ts[:13] if len(ts) >= 13 else ts


def compute_burst(
    history: List[HistoryEntry],
    window: int = 24,
    min_rate: float = 0.5,
    min_errors: int = 3,
) -> BurstReport:
    if not history:
        return BurstReport()

    recent = history[-window:]
    buckets: dict[tuple[str, str], list[int, int]] = {}

    for entry in recent:
        for pipeline, counts in entry.per_pipeline.items():
            errors = counts.get("errors", 0)
            total = counts.get("total", 0)
            key = (pipeline, _hour_key(entry.timestamp))
            if key not in buckets:
                buckets[key] = [0, 0]
            buckets[key][0] += errors
            buckets[key][1] += total

    bursts = []
    for (pipeline, hour), (errors, total) in buckets.items():
        if total == 0:
            continue
        rate = errors / total
        if errors >= min_errors and rate >= min_rate:
            bursts.append(BurstWindow(
                pipeline=pipeline,
                hour=hour,
                error_count=errors,
                total_events=total,
                rate=rate,
            ))

    bursts.sort(key=lambda b: b.rate, reverse=True)
    return BurstReport(bursts=bursts)


def format_burst(report: BurstReport) -> str:
    if not report.has_bursts():
        return "No error bursts detected."
    lines = ["Error Bursts Detected:", ""]
    for b in report.bursts:
        lines.append(
            f"  [{b.hour}] {b.pipeline}: {b.error_count}/{b.total_events} errors "
            f"({b.rate * 100:.1f}%)"
        )
    return "\n".join(lines)
