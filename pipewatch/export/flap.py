"""Flap detection: pipelines that repeatedly switch between healthy and failing states."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class FlapResult:
    pipeline: str
    transitions: int          # number of health-state changes
    total_periods: int
    flap_rate: float          # transitions / (total_periods - 1)
    flagged: bool


@dataclass
class FlapReport:
    results: List[FlapResult] = field(default_factory=list)

    def flapping(self) -> List[FlapResult]:
        return [r for r in self.results if r.flagged]

    def has_flaps(self) -> bool:
        return any(r.flagged for r in self.results)


def _rate(transitions: int, periods: int) -> float:
    if periods <= 1:
        return 0.0
    return transitions / (periods - 1)


def _group_by_pipeline(entries: List[HistoryEntry]) -> dict:
    groups: dict = {}
    for e in entries:
        for pipeline, counts in e.per_pipeline.items():
            groups.setdefault(pipeline, []).append(counts)
    return groups


def compute_flap(
    entries: List[HistoryEntry],
    window: int = 20,
    min_transitions: int = 3,
    min_flap_rate: float = 0.4,
) -> FlapReport:
    """Detect pipelines that flip between healthy and failing frequently."""
    if not entries:
        return FlapReport()

    recent = entries[-window:]
    groups = _group_by_pipeline(recent)

    results: List[FlapResult] = []
    for pipeline, period_counts in groups.items():
        if len(period_counts) < 2:
            continue

        # Determine healthy (False) vs failing (True) per period
        states = [counts.get("errors", 0) > 0 for counts in period_counts]

        transitions = sum(
            1 for i in range(1, len(states)) if states[i] != states[i - 1]
        )
        rate = _rate(transitions, len(states))
        flagged = transitions >= min_transitions and rate >= min_flap_rate

        results.append(
            FlapResult(
                pipeline=pipeline,
                transitions=transitions,
                total_periods=len(states),
                flap_rate=round(rate, 4),
                flagged=flagged,
            )
        )

    results.sort(key=lambda r: r.flap_rate, reverse=True)
    return FlapReport(results=results)


def format_flap(report: FlapReport) -> str:
    lines = ["=== Flap Detection ==="]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)

    for r in report.results:
        flag = " [FLAPPING]" if r.flagged else ""
        lines.append(
            f"  {r.pipeline}: {r.transitions} transitions / "
            f"{r.total_periods} periods  "
            f"(flap_rate={r.flap_rate:.1%}){flag}"
        )
    return "\n".join(lines)
