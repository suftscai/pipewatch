"""Append-only history log for pipeline summaries."""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from pipewatch.analysis.aggregator import PipelineSummary


@dataclass
class HistoryEntry:
    timestamp: str
    total_events: int
    total_failures: int
    failure_rate: float
    failures_by_pipeline: dict = field(default_factory=dict)
    events_by_pipeline: dict = field(default_factory=dict)


def _entry_from_summary(summary: PipelineSummary) -> HistoryEntry:
    total = summary.total_events
    rate = summary.total_failures / total if total > 0 else 0.0
    return HistoryEntry(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_events=total,
        total_failures=summary.total_failures,
        failure_rate=rate,
        failures_by_pipeline=dict(summary.failures_by_pipeline),
        events_by_pipeline=dict(summary.events_by_pipeline),
    )


def append_entry(path: Path, summary: PipelineSummary) -> HistoryEntry:
    entry = _entry_from_summary(summary)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write(json.dumps(asdict(entry)) + "\n")
    return entry


def load_history(path: Path) -> List[HistoryEntry]:
    if not path.exists():
        return []
    entries: List[HistoryEntry] = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                entries.append(HistoryEntry(**data))
            except (json.JSONDecodeError, TypeError):
                continue
    return entries


def recent_failure_trend(path: Path, window: int = 10) -> List[float]:
    entries = load_history(path)
    recent = entries[-window:] if len(entries) > window else entries
    return [e.failure_rate for e in recent]
