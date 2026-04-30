"""Entropy analysis: measures unpredictability of failure patterns per pipeline."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class EntropyResult:
    pipeline: str
    entropy: float          # Shannon entropy of failure rate distribution
    periods: int            # number of history windows used
    high_entropy: bool      # True when entropy exceeds threshold


@dataclass
class EntropyReport:
    results: List[EntropyResult]
    window: int
    threshold: float

    def chaotic(self) -> List[EntropyResult]:
        return [r for r in self.results if r.high_entropy]

    def has_chaos(self) -> bool:
        return any(r.high_entropy for r in self.results)


def _rate(entry: HistoryEntry) -> float:
    if entry.total_events == 0:
        return 0.0
    return entry.error_count / entry.total_events


def _shannon_entropy(rates: List[float]) -> float:
    """Compute Shannon entropy over discretised buckets (0.1 width)."""
    if not rates:
        return 0.0
    buckets: dict[int, int] = {}
    for r in rates:
        bucket = min(int(r * 10), 9)
        buckets[bucket] = buckets.get(bucket, 0) + 1
    n = len(rates)
    entropy = 0.0
    for count in buckets.values():
        p = count / n
        if p > 0:
            entropy -= p * math.log2(p)
    return entropy


def compute_entropy(
    history: List[HistoryEntry],
    window: int = 24,
    threshold: float = 2.5,
    min_periods: int = 4,
) -> EntropyReport:
    if not history:
        return EntropyReport(results=[], window=window, threshold=threshold)

    recent = history[-window:]

    pipelines: dict[str, List[float]] = {}
    for entry in recent:
        for pipeline in entry.top_failing:
            pipelines.setdefault(pipeline, [])
        rate = _rate(entry)
        for pipeline in pipelines:
            pipelines[pipeline].append(rate)

    # Build per-pipeline rate series from entries that mention them
    pipeline_rates: dict[str, List[float]] = {}
    for entry in recent:
        for pipeline in (entry.top_failing or []):
            pipeline_rates.setdefault(pipeline, []).append(
                entry.error_count / entry.total_events
                if entry.total_events > 0 else 0.0
            )

    results: List[EntropyResult] = []
    for pipeline, rates in sorted(pipeline_rates.items()):
        if len(rates) < min_periods:
            continue
        h = _shannon_entropy(rates)
        results.append(EntropyResult(
            pipeline=pipeline,
            entropy=round(h, 4),
            periods=len(rates),
            high_entropy=h >= threshold,
        ))

    results.sort(key=lambda r: r.entropy, reverse=True)
    return EntropyReport(results=results, window=window, threshold=threshold)


def format_entropy(report: EntropyReport) -> str:
    lines = ["=== Failure Pattern Entropy ==="]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)
    lines.append(f"  {'Pipeline':<30} {'Entropy':>8}  {'Periods':>7}  Status")
    lines.append("  " + "-" * 60)
    for r in report.results:
        flag = "[CHAOTIC]" if r.high_entropy else ""
        lines.append(
            f"  {r.pipeline:<30} {r.entropy:>8.4f}  {r.periods:>7}  {flag}"
        )
    return "\n".join(lines)
