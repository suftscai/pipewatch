"""Weekly/daily digest: summarise history entries into a human-readable report."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class DigestReport:
    period_label: str
    total_events: int
    total_errors: int
    total_warnings: int
    overall_failure_rate: float
    most_failing_pipeline: str | None
    entry_count: int


def compute_digest(entries: List[HistoryEntry], period_label: str = "recent") -> DigestReport:
    """Aggregate a list of history entries into a single digest."""
    if not entries:
        return DigestReport(
            period_label=period_label,
            total_events=0,
            total_errors=0,
            total_warnings=0,
            overall_failure_rate=0.0,
            most_failing_pipeline=None,
            entry_count=0,
        )

    total_events = sum(e.total_events for e in entries)
    total_errors = sum(e.total_errors for e in entries)
    total_warnings = sum(e.total_warnings for e in entries)
    failure_rate = total_errors / total_events if total_events else 0.0

    # Accumulate per-pipeline error counts across entries
    pipeline_errors: dict[str, int] = {}
    for entry in entries:
        for pipeline, count in entry.top_failing:
            pipeline_errors[pipeline] = pipeline_errors.get(pipeline, 0) + count

    most_failing = max(pipeline_errors, key=pipeline_errors.get) if pipeline_errors else None

    return DigestReport(
        period_label=period_label,
        total_events=total_events,
        total_errors=total_errors,
        total_warnings=total_warnings,
        overall_failure_rate=round(failure_rate, 4),
        most_failing_pipeline=most_failing,
        entry_count=len(entries),
    )


def format_digest(report: DigestReport) -> str:
    """Return a plain-text summary of the digest."""
    lines = [
        f"=== Digest: {report.period_label} ({report.entry_count} snapshots) ===",
        f"  Total events  : {report.total_events}",
        f"  Errors        : {report.total_errors}",
        f"  Warnings      : {report.total_warnings}",
        f"  Failure rate  : {report.overall_failure_rate * 100:.1f}%",
        f"  Most failing  : {report.most_failing_pipeline or 'n/a'}",
    ]
    return "\n".join(lines)
