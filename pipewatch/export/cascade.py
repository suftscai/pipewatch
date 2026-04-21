"""Cascade failure detection: identify pipelines whose failures cluster together in time."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class CascadeWindow:
    start: str
    end: str
    pipelines: List[str]
    total_errors: int


@dataclass
class CascadeReport:
    windows: List[CascadeWindow] = field(default_factory=list)

    def has_cascades(self) -> bool:
        return len(self.windows) > 0


def _ts(entry: HistoryEntry) -> datetime:
    return datetime.fromisoformat(entry.timestamp).replace(tzinfo=timezone.utc)


def compute_cascade(
    history: List[HistoryEntry],
    window_minutes: int = 5,
    min_pipelines: int = 2,
    recent: int = 50,
) -> CascadeReport:
    """Detect time windows where multiple pipelines failed simultaneously."""
    entries = [e for e in history[-recent:] if e.errors > 0]
    if not entries:
        return CascadeReport()

    entries = sorted(entries, key=lambda e: e.timestamp)
    from datetime import timedelta

    windows: List[CascadeWindow] = []
    i = 0
    while i < len(entries):
        anchor = _ts(entries[i])
        cutoff = anchor + timedelta(minutes=window_minutes)
        group = [e for e in entries[i:] if _ts(e) <= cutoff]
        pipelines = list({e.pipeline for e in group})
        if len(pipelines) >= min_pipelines:
            total = sum(e.errors for e in group)
            windows.append(
                CascadeWindow(
                    start=entries[i].timestamp,
                    end=group[-1].timestamp,
                    pipelines=sorted(pipelines),
                    total_errors=total,
                )
            )
            i += len(group)
        else:
            i += 1

    return CascadeReport(windows=windows)


def format_cascade(report: CascadeReport) -> str:
    lines = ["=== Cascade Failure Report ==="]
    if not report.has_cascades():
        lines.append("No cascade failures detected.")
        return "\n".join(lines)
    for w in report.windows:
        pipes = ", ".join(w.pipelines)
        lines.append(f"  [{w.start} -> {w.end}]  errors={w.total_errors}  pipelines=({pipes})")
    return "\n".join(lines)
