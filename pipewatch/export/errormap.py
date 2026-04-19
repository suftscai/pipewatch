"""Error message frequency map across pipelines."""
from __future__ import annotations
from dataclasses import dataclass, field
from collections import defaultdict
from typing import List, Dict, Tuple
from pipewatch.export.history import HistoryEntry


@dataclass
class ErrorMapEntry:
    pipeline: str
    message: str
    count: int


@dataclass
class ErrorMapReport:
    entries: List[ErrorMapEntry] = field(default_factory=list)
    top_n: int = 10


def compute_errormap(
    history: List[HistoryEntry],
    window: int = 50,
    top_n: int = 10,
) -> ErrorMapReport:
    recent = history[-window:] if window else history
    counts: Dict[Tuple[str, str], int] = defaultdict(int)
    for entry in recent:
        for pipeline, messages in entry.error_messages.items():
            for msg in messages:
                counts[(pipeline, msg)] += 1
    entries = [
        ErrorMapEntry(pipeline=p, message=m, count=c)
        for (p, m), c in sorted(counts.items(), key=lambda x: -x[1])
    ][:top_n]
    return ErrorMapReport(entries=entries, top_n=top_n)


def format_errormap(report: ErrorMapReport) -> str:
    if not report.entries:
        return "Error Map: no error messages recorded."
    lines = ["Error Message Frequency Map:", ""]
    for e in report.entries:
        lines.append(f"  [{e.pipeline}] ({e.count}x) {e.message}")
    return "\n".join(lines)
