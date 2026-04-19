"""Measures pipeline event throughput (events per hour) over a rolling window."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class VelocityPoint:
    pipeline: str
    hour: str          # ISO hour string  e.g. "2024-06-01T14"
    total_events: int
    error_events: int

    @property
    def error_rate(self) -> float:
        return self.error_events / self.total_events if self.total_events else 0.0


@dataclass
class VelocityReport:
    points: List[VelocityPoint]
    window: int

    @property
    def pipelines(self) -> List[str]:
        return sorted({p.pipeline for p in self.points})


def _hour_key(ts: str) -> str:
    """Return 'YYYY-MM-DDTHH' from an ISO timestamp string."""
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        return ts[:13]
    return dt.strftime("%Y-%m-%dT%H")


def compute_velocity(history: List[HistoryEntry], window: int = 24) -> VelocityReport:
    """Aggregate event counts per pipeline per hour over the last *window* entries."""
    recent = history[-window:] if len(history) > window else history

    # { (pipeline, hour): [total, errors] }
    buckets: dict[tuple[str, str], list[int]] = {}

    for entry in recent:
        for pipeline, counts in entry.per_pipeline.items():
            hour = _hour_key(entry.timestamp)
            key = (pipeline, hour)
            if key not in buckets:
                buckets[key] = [0, 0]
            buckets[key][0] += counts.get("total", 0)
            buckets[key][1] += counts.get("errors", 0)

    points = [
        VelocityPoint(pipeline=pl, hour=hr, total_events=v[0], error_events=v[1])
        for (pl, hr), v in sorted(buckets.items())
    ]
    return VelocityReport(points=points, window=window)


def format_velocity(report: VelocityReport) -> str:
    if not report.points:
        return "No velocity data available."

    lines = ["Pipeline Velocity Report", "=" * 40]
    current_pipeline = None
    for pt in report.points:
        if pt.pipeline != current_pipeline:
            current_pipeline = pt.pipeline
            lines.append(f"\n[{pt.pipeline}]")
        lines.append(
            f"  {pt.hour}  total={pt.total_events:>4}  errors={pt.error_events:>4}"
            f"  rate={pt.error_rate:.1%}"
        )
    return "\n".join(lines)
