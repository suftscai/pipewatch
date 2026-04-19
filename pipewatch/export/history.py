"""History entry storage and retrieval for pipeline summaries."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict
import json
import os
from pipewatch.analysis.aggregator import PipelineSummary


@dataclass
class HistoryEntry:
    timestamp: str
    total_events: int
    error_count: int
    warning_count: int
    pipeline_errors: Dict[str, int] = field(default_factory=dict)
    error_messages: Dict[str, List[str]] = field(default_factory=dict)


def _entry_from_summary(summary: PipelineSummary, timestamp: str) -> HistoryEntry:
    return HistoryEntry(
        timestamp=timestamp,
        total_events=summary.total_events,
        error_count=summary.error_count,
        warning_count=summary.warning_count,
        pipeline_errors=dict(summary.errors_by_pipeline),
        error_messages={
            p: list(msgs) for p, msgs in getattr(summary, "messages_by_pipeline", {}).items()
        },
    )


def append_entry(path: str, entry: HistoryEntry) -> None:
    entries = load_history(path)
    entries.append(entry)
    with open(path, "w") as f:
        json.dump([e.__dict__ for e in entries], f)


def load_history(path: str) -> List[HistoryEntry]:
    if not os.path.exists(path):
        return []
    with open(path) as f:
        raw = json.load(f)
    return [HistoryEntry(**r) for r in raw]


def recent_failure_trend(history: List[HistoryEntry], window: int = 10) -> List[float]:
    recent = history[-window:]
    rates = []
    for e in recent:
        if e.total_events > 0:
            rates.append(e.error_count / e.total_events)
        else:
            rates.append(0.0)
    return rates
