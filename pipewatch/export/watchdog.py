"""Watchdog: detect pipelines that have gone silent (no events recently)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.export.history import HistoryEntry


@dataclass
class SilentPipeline:
    pipeline: str
    last_seen: datetime
    silent_for_seconds: float


@dataclass
class WatchdogReport:
    silent: List[SilentPipeline] = field(default_factory=list)
    checked_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def has_silent(self) -> bool:
        return bool(self.silent)


def _build_latest_seen(entries: List[HistoryEntry]) -> dict[str, datetime]:
    """Return a mapping of pipeline name to its most recent timestamp."""
    latest: dict[str, datetime] = {}
    for entry in entries:
        ts = entry.timestamp
        for pipeline in entry.top_failing:
            if pipeline not in latest or ts > latest[pipeline]:
                latest[pipeline] = ts
    return latest


def detect_silent(
    entries: List[HistoryEntry],
    threshold_seconds: float = 300.0,
    now: Optional[datetime] = None,
) -> WatchdogReport:
    """Return pipelines whose last event is older than threshold_seconds."""
    if now is None:
        now = datetime.utcnow()

    latest = _build_latest_seen(entries)

    silent: List[SilentPipeline] = []
    cutoff = now - timedelta(seconds=threshold_seconds)
    for pipeline, last_seen in latest.items():
        if last_seen < cutoff:
            delta = (now - last_seen).total_seconds()
            silent.append(SilentPipeline(pipeline=pipeline, last_seen=last_seen, silent_for_seconds=delta))

    silent.sort(key=lambda s: s.silent_for_seconds, reverse=True)
    return WatchdogReport(silent=silent, checked_at=now)


def format_watchdog(report: WatchdogReport) -> str:
    lines = ["=== Watchdog Report ==="]
    if not report.has_silent:
        lines.append("All pipelines active — no silent pipelines detected.")
    else:
        lines.append(f"{len(report.silent)} silent pipeline(s):")
        for s in report.silent:
            mins = s.silent_for_seconds / 60
            lines.append(f"  {s.pipeline}: last seen {s.last_seen.isoformat()} ({mins:.1f} min ago)")
    return "\n".join(lines)
