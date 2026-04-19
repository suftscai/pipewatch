"""Burndown: track open vs resolved failures over time."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List
from pipewatch.export.history import HistoryEntry


@dataclass
class BurndownPoint:
    timestamp: str
    pipeline: str
    opened: int
    resolved: int
    net: int  # opened - resolved (cumulative delta)


@dataclass
class BurndownReport:
    points: List[BurndownPoint]
    total_opened: int
    total_resolved: int


def compute_burndown(history: List[HistoryEntry], window: int = 20) -> BurndownReport:
    entries = history[-window:] if window else history
    if not entries:
        return BurndownReport(points=[], total_opened=0, total_resolved=0)

    prev_failures: dict[str, int] = {}
    points: List[BurndownPoint] = []
    total_opened = 0
    total_resolved = 0

    for entry in entries:
        for pipeline, failures in entry.failures_by_pipeline.items():
            prev = prev_failures.get(pipeline, 0)
            delta = failures - prev
            opened = max(delta, 0)
            resolved = max(-delta, 0)
            total_opened += opened
            total_resolved += resolved
            points.append(BurndownPoint(
                timestamp=entry.timestamp,
                pipeline=pipeline,
                opened=opened,
                resolved=resolved,
                net=delta,
            ))
            prev_failures[pipeline] = failures

    return BurndownReport(points=points, total_opened=total_opened, total_resolved=total_resolved)


def format_burndown(report: BurndownReport) -> str:
    if not report.points:
        return "Burndown: no data available."
    lines = ["=== Failure Burndown ==="]
    lines.append(f"Total Opened : {report.total_opened}")
    lines.append(f"Total Resolved: {report.total_resolved}")
    lines.append("")
    lines.append(f"{'Timestamp':<26} {'Pipeline':<20} {'Opened':>7} {'Resolved':>9} {'Net':>5}")
    lines.append("-" * 72)
    for p in report.points:
        lines.append(f"{p.timestamp:<26} {p.pipeline:<20} {p.opened:>7} {p.resolved:>9} {p.net:>+5}")
    return "\n".join(lines)
