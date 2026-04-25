"""Stall detection: identify pipelines whose event rate has dropped sharply
compared to their historical average, suggesting they may have stalled."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class StallResult:
    pipeline: str
    historical_avg: float   # events per window slice
    recent_count: int
    drop_pct: float         # 0-100
    stalled: bool


@dataclass
class StallReport:
    results: List[StallResult] = field(default_factory=list)

    @property
    def has_stalls(self) -> bool:
        return any(r.stalled for r in self.results)


def _events_by_pipeline(entries: List[HistoryEntry]) -> dict[str, List[int]]:
    out: dict[str, List[int]] = {}
    for e in entries:
        for pipeline, counts in e.error_counts.items():
            total = counts + e.warning_counts.get(pipeline, 0)
            out.setdefault(pipeline, []).append(total)
    return out


def compute_stall(
    history: List[HistoryEntry],
    window: int = 20,
    recent_slices: int = 3,
    drop_threshold: float = 60.0,
    min_avg: float = 2.0,
) -> StallReport:
    """Detect pipelines whose recent event count dropped sharply.

    Args:
        history: Full history list (oldest first).
        window: Maximum number of entries to consider.
        recent_slices: How many trailing entries count as "recent".
        drop_threshold: Percentage drop required to flag a stall (0-100).
        min_avg: Minimum historical average to bother checking.
    """
    entries = history[-window:] if len(history) > window else history
    if not entries:
        return StallReport()

    by_pipeline = _events_by_pipeline(entries)
    results: List[StallResult] = []

    for pipeline, counts in by_pipeline.items():
        if len(counts) <= recent_slices:
            historical = counts
            recent = counts[-recent_slices:]
        else:
            historical = counts[:-recent_slices]
            recent = counts[-recent_slices:]

        hist_avg = sum(historical) / len(historical) if historical else 0.0
        recent_total = sum(recent)

        if hist_avg < min_avg:
            continue

        drop_pct = max(0.0, (hist_avg - recent_total / len(recent)) / hist_avg * 100)
        stalled = drop_pct >= drop_threshold

        results.append(
            StallResult(
                pipeline=pipeline,
                historical_avg=round(hist_avg, 2),
                recent_count=recent_total,
                drop_pct=round(drop_pct, 1),
                stalled=stalled,
            )
        )

    results.sort(key=lambda r: r.drop_pct, reverse=True)
    return StallReport(results=results)


def format_stall(report: StallReport) -> str:
    lines = ["=== Stall Detection ==="]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)
    for r in report.results:
        flag = " [STALLED]" if r.stalled else ""
        lines.append(
            f"  {r.pipeline}: hist_avg={r.historical_avg:.1f}  "
            f"recent={r.recent_count}  drop={r.drop_pct:.1f}%{flag}"
        )
    return "\n".join(lines)
