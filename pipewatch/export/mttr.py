"""Mean Time To Recovery (MTTR) analysis across pipeline history."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict
from pipewatch.export.history import HistoryEntry


@dataclass
class MTTREntry:
    pipeline: str
    incident_count: int
    mean_recovery_minutes: float
    max_failure_rate: float


@dataclass
class MTTRReport:
    entries: List[MTTREntry]
    window: int


def _group_by_pipeline(history: List[HistoryEntry]) -> Dict[str, List[HistoryEntry]]:
    groups: Dict[str, List[HistoryEntry]] = {}
    for entry in history:
        for pipeline in entry.top_failing:
            groups.setdefault(pipeline, []).append(entry)
    return groups


def compute_mttr(history: List[HistoryEntry], window: int = 24) -> MTTRReport:
    """Estimate MTTR by measuring runs where failure rate spikes then recovers."""
    recent = history[-window:] if len(history) > window else history
    groups = _group_by_pipeline(recent)
    entries: List[MTTREntry] = []

    for pipeline, runs in groups.items():
        incidents = 0
        recovery_spans: List[float] = []
        in_incident = False
        incident_start = 0
        max_rate = 0.0

        for i, run in enumerate(runs):
            rate = run.failure_rate
            max_rate = max(max_rate, rate)
            if not in_incident and rate >= 0.5:
                in_incident = True
                incident_start = i
            elif in_incident and rate < 0.2:
                incidents += 1
                span = (i - incident_start) * 60.0  # assume ~60 min between snapshots
                recovery_spans.append(span)
                in_incident = False

        if in_incident:
            incidents += 1

        mean_recovery = sum(recovery_spans) / len(recovery_spans) if recovery_spans else 0.0
        entries.append(MTTREntry(
            pipeline=pipeline,
            incident_count=incidents,
            mean_recovery_minutes=round(mean_recovery, 1),
            max_failure_rate=round(max_rate, 4),
        ))

    entries.sort(key=lambda e: e.mean_recovery_minutes, reverse=True)
    return MTTRReport(entries=entries, window=window)


def format_mttr(report: MTTRReport) -> str:
    if not report.entries:
        return "No MTTR data available."
    lines = [f"MTTR Report (last {report.window} snapshots):", ""]
    for e in report.entries:
        lines.append(
            f"  {e.pipeline}: {e.incident_count} incident(s), "
            f"avg recovery {e.mean_recovery_minutes} min, "
            f"peak failure rate {e.max_failure_rate:.1%}"
        )
    return "\n".join(lines)
