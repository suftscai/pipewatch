"""Uptime tracking: computes per-pipeline uptime percentage from history."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List
from pipewatch.export.history import HistoryEntry


@dataclass
class UptimeResult:
    pipeline: str
    total_events: int
    error_events: int
    uptime_pct: float  # 0.0 – 100.0


@dataclass
class UptimeReport:
    results: List[UptimeResult]
    window: int


def _rate(errors: int, total: int) -> float:
    return (errors / total) if total else 0.0


def compute_uptime(history: List[HistoryEntry], window: int = 30) -> UptimeReport:
    """Compute uptime for each pipeline over the last *window* entries."""
    recent = history[-window:] if len(history) > window else history

    totals: dict[str, int] = {}
    errors: dict[str, int] = {}

    for entry in recent:
        for pipeline, count in entry.error_counts.items():
            totals[pipeline] = totals.get(pipeline, 0) + entry.total_events_by_pipeline.get(pipeline, 0)
            errors[pipeline] = errors.get(pipeline, 0) + count

    results: List[UptimeResult] = []
    for pipeline in sorted(totals):
        t = totals[pipeline]
        e = errors.get(pipeline, 0)
        uptime = (1.0 - _rate(e, t)) * 100.0
        results.append(UptimeResult(pipeline=pipeline, total_events=t, error_events=e, uptime_pct=round(uptime, 2)))

    return UptimeReport(results=results, window=window)


def format_uptime(report: UptimeReport) -> str:
    if not report.results:
        return "No uptime data available."
    lines = [f"Uptime Report (window={report.window}):", ""]
    for r in report.results:
        bar = "#" * int(r.uptime_pct / 5)
        lines.append(f"  {r.pipeline:<30} {r.uptime_pct:6.2f}%  [{bar:<20}]  errors={r.error_events}/{r.total_events}")
    return "\n".join(lines)
