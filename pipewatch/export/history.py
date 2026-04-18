"""Track and query historical pipeline summaries across report cycles."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from pipewatch.analysis.aggregator import PipelineSummary
from pipewatch.export.reporter import summary_to_dict


@dataclass
class HistoryEntry:
    timestamp: str
    total: int
    errors: int
    warnings: int
    failure_rate: float
    top_failing: List[str]


def _entry_from_summary(summary: PipelineSummary) -> HistoryEntry:
    from pipewatch.analysis.aggregator import failure_rate, top_failing

    rate = failure_rate(summary)
    top = [stage for stage, _ in top_failing(summary, n=3)]
    return HistoryEntry(
        timestamp=datetime.utcnow().isoformat(timespec="seconds"),
        total=summary.total,
        errors=summary.errors,
        warnings=summary.warnings,
        failure_rate=round(rate, 4),
        top_failing=top,
    )


def append_entry(path: Path, summary: PipelineSummary) -> HistoryEntry:
    """Append a new history entry derived from *summary* to the JSONL file at *path*."""
    entry = _entry_from_summary(summary)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry.__dict__) + "\n")
    return entry


def load_history(path: Path) -> List[HistoryEntry]:
    """Load all history entries from a JSONL file."""
    if not path.exists():
        return []
    entries: List[HistoryEntry] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            entries.append(HistoryEntry(**data))
    return entries


def recent_failure_trend(path: Path, n: int = 5) -> List[float]:
    """Return the failure rates of the last *n* entries."""
    entries = load_history(path)
    return [e.failure_rate for e in entries[-n:]]
