"""Spike detection: flag pipelines whose error count in the latest
history entry exceeds a configurable multiple of their rolling mean."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class SpikeResult:
    pipeline: str
    latest_errors: int
    mean_errors: float
    ratio: float
    is_spike: bool


@dataclass
class SpikeReport:
    results: List[SpikeResult] = field(default_factory=list)


def spikes(report: SpikeReport) -> List[SpikeResult]:
    return [r for r in report.results if r.is_spike]


def has_spikes(report: SpikeReport) -> bool:
    return any(r.is_spike for r in report.results)


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _errors_by_pipeline(entries: List[HistoryEntry]) -> dict:
    data: dict = {}
    for entry in entries:
        for pipeline, counts in entry.pipeline_counts.items():
            data.setdefault(pipeline, []).append(counts.get("errors", 0))
    return data


def compute_spike(
    history: List[HistoryEntry],
    window: int = 24,
    multiplier: float = 2.5,
    min_mean: float = 1.0,
) -> SpikeReport:
    if not history:
        return SpikeReport()

    recent = history[-window:] if len(history) > window else history
    by_pipeline = _errors_by_pipeline(recent)

    results: List[SpikeResult] = []
    for pipeline, error_counts in by_pipeline.items():
        if len(error_counts) < 2:
            continue
        latest = error_counts[-1]
        baseline = error_counts[:-1]
        mean_val = _mean([float(v) for v in baseline])
        if mean_val < min_mean:
            ratio = 0.0
            is_spike = False
        else:
            ratio = latest / mean_val
            is_spike = ratio >= multiplier
        results.append(
            SpikeResult(
                pipeline=pipeline,
                latest_errors=latest,
                mean_errors=round(mean_val, 3),
                ratio=round(ratio, 3),
                is_spike=is_spike,
            )
        )

    results.sort(key=lambda r: r.ratio, reverse=True)
    return SpikeReport(results=results)


def format_spike(report: SpikeReport) -> str:
    lines = ["Spike Detection Report", "=" * 40]
    if not report.results:
        lines.append("No data.")
        return "\n".join(lines)
    for r in report.results:
        flag = " [SPIKE]" if r.is_spike else ""
        lines.append(
            f"{r.pipeline}: latest={r.latest_errors} "
            f"mean={r.mean_errors:.2f} ratio={r.ratio:.2f}{flag}"
        )
    return "\n".join(lines)
