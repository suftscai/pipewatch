"""Detect pipelines that have gone completely silent (zero events) over a window."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.export.history import HistoryEntry


@dataclass
class SilenceResult:
    pipeline: str
    last_seen: Optional[datetime]  # None if never seen in window
    hours_silent: float
    flagged: bool


@dataclass
class SilenceReport:
    results: List[SilenceResult]
    window: int  # hours
    threshold_hours: float

    @property
    def has_silent(self) -> bool:
        return any(r.flagged for r in self.results)


def _all_pipelines(entries: List[HistoryEntry]) -> List[str]:
    seen: dict[str, None] = {}
    for e in entries:
        for p in e.top_failing:
            seen[p] = None
    return list(seen.keys())


def _last_seen_for(pipeline: str, entries: List[HistoryEntry]) -> Optional[datetime]:
    """Return the most recent timestamp where the pipeline appeared."""
    for e in reversed(entries):
        if pipeline in e.top_failing:
            return e.timestamp
    return None


def compute_silence(
    entries: List[HistoryEntry],
    window: int = 24,
    threshold_hours: float = 6.0,
    now: Optional[datetime] = None,
) -> SilenceReport:
    if now is None:
        now = datetime.now(timezone.utc)

    cutoff = now.timestamp() - window * 3600
    recent = [e for e in entries if e.timestamp.timestamp() >= cutoff]

    pipelines = _all_pipelines(entries)  # use full history to know all pipelines
    results: List[SilenceResult] = []

    for pipeline in pipelines:
        last = _last_seen_for(pipeline, recent)
        if last is None:
            # pipeline known but no events in window
            hours_silent = float(window)
        else:
            hours_silent = (now.timestamp() - last.timestamp()) / 3600.0

        flagged = hours_silent >= threshold_hours
        results.append(
            SilenceResult(
                pipeline=pipeline,
                last_seen=last,
                hours_silent=round(hours_silent, 2),
                flagged=flagged,
            )
        )

    results.sort(key=lambda r: r.hours_silent, reverse=True)
    return SilenceReport(results=results, window=window, threshold_hours=threshold_hours)


def format_silence(report: SilenceReport) -> str:
    lines = [
        f"=== Silence Report (window={report.window}h, threshold={report.threshold_hours}h) ==="
    ]
    if not report.results:
        lines.append("  No pipeline data available.")
        return "\n".join(lines)

    for r in report.results:
        flag = " [SILENT]" if r.flagged else ""
        last_str = r.last_seen.strftime("%Y-%m-%d %H:%M:%S") if r.last_seen else "never"
        lines.append(
            f"  {r.pipeline:<30} last_seen={last_str}  silent={r.hours_silent:.1f}h{flag}"
        )
    return "\n".join(lines)
