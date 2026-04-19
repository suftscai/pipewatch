"""Detect pipelines whose failure rates are statistical outliers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class OutlierResult:
    pipeline: str
    failure_rate: float
    mean: float
    std: float
    z_score: float


@dataclass
class OutlierReport:
    outliers: List[OutlierResult]

    def has_outliers(self) -> bool:
        return len(self.outliers) > 0


def _rate(entry: HistoryEntry, pipeline: str) -> float:
    total = entry.pipeline_totals.get(pipeline, 0)
    errors = entry.pipeline_errors.get(pipeline, 0)
    return errors / total if total > 0 else 0.0


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: List[float], mean: float) -> float:
    if len(values) < 2:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return variance ** 0.5


def compute_outliers(
    history: List[HistoryEntry],
    window: int = 20,
    threshold: float = 2.0,
) -> OutlierReport:
    if not history:
        return OutlierReport(outliers=[])

    recent = history[-window:]
    pipelines: set[str] = set()
    for entry in recent:
        pipelines.update(entry.pipeline_totals.keys())

    if not pipelines:
        return OutlierReport(outliers=[])

    rates = {p: [_rate(e, p) for e in recent] for p in pipelines}
    all_latest = [rates[p][-1] for p in pipelines]
    mean = _mean(all_latest)
    std = _std(all_latest, mean)

    outliers: List[OutlierResult] = []
    for pipeline in sorted(pipelines):
        latest = rates[pipeline][-1]
        z = (latest - mean) / std if std > 0 else 0.0
        if z >= threshold:
            outliers.append(
                OutlierResult(
                    pipeline=pipeline,
                    failure_rate=latest,
                    mean=mean,
                    std=std,
                    z_score=z,
                )
            )

    outliers.sort(key=lambda r: r.z_score, reverse=True)
    return OutlierReport(outliers=outliers)


def format_outliers(report: OutlierReport) -> str:
    lines = ["=== Outlier Pipelines ==="]
    if not report.has_outliers():
        lines.append("No outliers detected.")
        return "\n".join(lines)
    lines.append(f"  {'Pipeline':<30} {'Failure Rate':>12} {'Z-Score':>8}")
    for r in report.outliers:
        lines.append(
            f"  {r.pipeline:<30} {r.failure_rate * 100:>11.1f}% {r.z_score:>8.2f}"
        )
    return "\n".join(lines)
