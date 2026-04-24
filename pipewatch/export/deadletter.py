"""Dead-letter queue: track events that repeatedly fail without recovery."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict

from pipewatch.export.history import HistoryEntry


@dataclass
class DeadLetterEntry:
    pipeline: str
    consecutive_failures: int
    first_failure_ts: str
    last_failure_ts: str
    flagged: bool


@dataclass
class DeadLetterReport:
    entries: List[DeadLetterEntry] = field(default_factory=list)

    def flagged(self) -> List[DeadLetterEntry]:
        return [e for e in self.entries if e.flagged]

    def has_dead_letters(self) -> bool:
        return any(e.flagged for e in self.entries)


def _group_by_pipeline(history: List[HistoryEntry]) -> Dict[str, List[HistoryEntry]]:
    groups: Dict[str, List[HistoryEntry]] = {}
    for entry in history:
        for pipeline in entry.top_failing:
            groups.setdefault(pipeline, []).append(entry)
    return groups


def compute_deadletter(
    history: List[HistoryEntry],
    min_consecutive: int = 3,
    window: int = 20,
) -> DeadLetterReport:
    if not history:
        return DeadLetterReport()

    recent = history[-window:]
    groups = _group_by_pipeline(recent)
    entries: List[DeadLetterEntry] = []

    for pipeline, pipeline_entries in groups.items():
        sorted_entries = sorted(pipeline_entries, key=lambda e: e.timestamp)
        streak = 0
        first_ts = ""
        last_ts = ""
        for e in sorted_entries:
            rate = e.failure_rate
            if rate > 0.0:
                streak += 1
                last_ts = e.timestamp
                if streak == 1:
                    first_ts = e.timestamp
            else:
                streak = 0
                first_ts = ""
                last_ts = ""

        flagged = streak >= min_consecutive
        entries.append(
            DeadLetterEntry(
                pipeline=pipeline,
                consecutive_failures=streak,
                first_failure_ts=first_ts,
                last_failure_ts=last_ts,
                flagged=flagged,
            )
        )

    entries.sort(key=lambda e: e.consecutive_failures, reverse=True)
    return DeadLetterReport(entries=entries)


def format_deadletter(report: DeadLetterReport) -> str:
    lines = ["=== Dead-Letter Pipelines ==="]
    if not report.entries:
        lines.append("  No data.")
        return "\n".join(lines)

    for e in report.entries:
        status = "[DEAD]" if e.flagged else "[ok]  "
        lines.append(
            f"  {status} {e.pipeline:<30} "
            f"streak={e.consecutive_failures}  "
            f"since={e.first_failure_ts or '-'}  "
            f"last={e.last_failure_ts or '-'}"
        )
    return "\n".join(lines)
