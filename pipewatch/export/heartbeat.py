"""Heartbeat monitor: detects pipelines that have stopped emitting events entirely."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class HeartbeatResult:
    pipeline: str
    last_seen: datetime
    seconds_silent: float
    expected_interval_s: float
    missed_beats: int

    @property
    def is_flatline(self) -> bool:
        return self.missed_beats >= 2


@dataclass
class HeartbeatReport:
    results: List[HeartbeatResult] = field(default_factory=list)
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def flatlines(self) -> List[HeartbeatResult]:
        return [r for r in self.results if r.is_flatline]

    def has_flatlines(self) -> bool:
        return any(r.is_flatline for r in self.results)


def _last_seen_per_pipeline(entries: List[HistoryEntry]) -> dict[str, datetime]:
    seen: dict[str, datetime] = {}
    for entry in entries:
        ts = entry.timestamp
        for pipeline in entry.top_failing:
            if pipeline not in seen or ts > seen[pipeline]:
                seen[pipeline] = ts
    return seen


def compute_heartbeat(
    entries: List[HistoryEntry],
    expected_interval_s: float = 300.0,
    window: int = 50,
    now: datetime | None = None,
) -> HeartbeatReport:
    if not entries:
        return HeartbeatReport()

    recent = entries[-window:]
    if now is None:
        now = datetime.now(timezone.utc)

    last_seen = _last_seen_per_pipeline(recent)
    results: List[HeartbeatResult] = []

    for pipeline, ts in sorted(last_seen.items()):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        delta = (now - ts).total_seconds()
        missed = int(delta // expected_interval_s)
        results.append(
            HeartbeatResult(
                pipeline=pipeline,
                last_seen=ts,
                seconds_silent=round(delta, 1),
                expected_interval_s=expected_interval_s,
                missed_beats=missed,
            )
        )

    results.sort(key=lambda r: r.seconds_silent, reverse=True)
    return HeartbeatReport(results=results)


def format_heartbeat(report: HeartbeatReport) -> str:
    lines = ["=== Heartbeat Monitor ==="]
    if not report.results:
        lines.append("  No pipeline data available.")
        return "\n".join(lines)

    for r in report.results:
        status = "FLATLINE" if r.is_flatline else "ok"
        lines.append(
            f"  {r.pipeline:<30} last={r.seconds_silent:>8.1f}s ago  "
            f"missed={r.missed_beats}  [{status}]"
        )
    return "\n".join(lines)
