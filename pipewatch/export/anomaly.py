"""Detect anomalous failure rates compared to a rolling baseline."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.export.history import HistoryEntry


@dataclass
class AnomalyResult:
    pipeline: str
    current_rate: float
    baseline_rate: float
    delta: float
    is_anomaly: bool


@dataclass
class AnomalyReport:
    anomalies: List[AnomalyResult]
    threshold: float

    @property
    def has_anomalies(self) -> bool:
        return any(r.is_anomaly for r in self.anomalies)


def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def detect_anomalies(
    history: List[HistoryEntry],
    current: HistoryEntry,
    threshold: float = 0.15,
    window: int = 10,
) -> AnomalyReport:
    """Compare current pipeline failure rates against rolling mean of history."""
    recent = history[-window:] if len(history) > window else history

    # Build per-pipeline rate history
    pipeline_rates: dict[str, List[float]] = {}
    for entry in recent:
        for pipeline, failures in entry.failures_by_pipeline.items():
            total = entry.events_by_pipeline.get(pipeline, 0)
            rate = failures / total if total > 0 else 0.0
            pipeline_rates.setdefault(pipeline, []).append(rate)

    results: List[AnomalyResult] = []
    for pipeline, failures in current.failures_by_pipeline.items():
        total = current.events_by_pipeline.get(pipeline, 0)
        current_rate = failures / total if total > 0 else 0.0
        baseline_rate = _mean(pipeline_rates.get(pipeline, [])) or 0.0
        delta = current_rate - baseline_rate
        results.append(
            AnomalyResult(
                pipeline=pipeline,
                current_rate=current_rate,
                baseline_rate=baseline_rate,
                delta=delta,
                is_anomaly=delta > threshold,
            )
        )

    return AnomalyReport(anomalies=results, threshold=threshold)


def format_anomaly(report: AnomalyReport) -> str:
    if not report.anomalies:
        return "No anomaly data available."
    lines = [f"Anomaly Detection (threshold: {report.threshold:.0%})"]
    for r in sorted(report.anomalies, key=lambda x: -x.delta):
        flag = " [ANOMALY]" if r.is_anomaly else ""
        lines.append(
            f"  {r.pipeline}: current={r.current_rate:.1%} "
            f"baseline={r.baseline_rate:.1%} delta=+{r.delta:.1%}{flag}"
        )
    return "\n".join(lines)
