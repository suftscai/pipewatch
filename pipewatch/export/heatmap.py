"""Pipeline failure heatmap: failure counts bucketed by hour-of-day."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict

from pipewatch.export.history import HistoryEntry


@dataclass
class HeatmapReport:
    # pipeline -> hour (0-23) -> failure count
    data: Dict[str, Dict[int, int]] = field(default_factory=dict)
    total_hours: int = 24


def compute_heatmap(entries: List[HistoryEntry]) -> HeatmapReport:
    """Aggregate failure counts per pipeline per hour-of-day."""
    report = HeatmapReport()
    for entry in entries:
        hour = entry.timestamp.hour
        for pipeline, failures in entry.failures_by_pipeline.items():
            if pipeline not in report.data:
                report.data[pipeline] = {h: 0 for h in range(24)}
            report.data[pipeline][hour] += failures
    return report


def format_heatmap(report: HeatmapReport) -> str:
    if not report.data:
        return "No heatmap data available."

    hours_header = "".join(f"{h:>3}" for h in range(24))
    lines = [f"Pipeline Failure Heatmap (by hour UTC)", f"{'Pipeline':<30} {hours_header}"]
    lines.append("-" * (30 + 1 + 24 * 3))

    for pipeline in sorted(report.data):
        row = report.data[pipeline]
        counts = "".join(_cell(row.get(h, 0)) for h in range(24))
        lines.append(f"{pipeline:<30} {counts}")

    return "\n".join(lines)


def _cell(value: int) -> str:
    if value == 0:
        return "  ."
    if value < 5:
        return f"{value:>3}"
    return "  !"  # highlight hot cells
