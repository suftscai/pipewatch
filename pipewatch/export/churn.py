"""Pipeline churn detection: identifies pipelines with rapidly alternating failure/recovery cycles."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class ChurnResult:
    pipeline: str
    transitions: int          # number of failure<->ok state flips
    total_entries: int
    churn_rate: float         # transitions / total_entries
    is_churning: bool


@dataclass
class ChurnReport:
    results: List[ChurnResult] = field(default_factory=list)

    @property
    def churning(self) -> List[ChurnResult]:
        return [r for r in self.results if r.is_churning]

    @property
    def has_churn(self) -> bool:
        return bool(self.churning)


def _group_by_pipeline(entries: List[HistoryEntry]) -> dict[str, List[HistoryEntry]]:
    groups: dict[str, List[HistoryEntry]] = {}
    for e in entries:
        for pipeline in e.top_failing:
            groups.setdefault(pipeline, []).append(e)
    return groups


def _count_transitions(entries: List[HistoryEntry], pipeline: str) -> int:
    """Count how many times a pipeline flips between failing and not-failing."""
    states = []
    for e in sorted(entries, key=lambda x: x.timestamp):
        rate = e.top_failing.get(pipeline, 0) / e.total_events if e.total_events else 0
        states.append(rate > 0)
    transitions = sum(1 for i in range(1, len(states)) if states[i] != states[i - 1])
    return transitions


def compute_churn(
    history: List[HistoryEntry],
    window: int = 24,
    min_transitions: int = 3,
    min_churn_rate: float = 0.4,
) -> ChurnReport:
    if not history:
        return ChurnReport()

    recent = sorted(history, key=lambda e: e.timestamp, reverse=True)[:window]
    groups = _group_by_pipeline(recent)

    results: List[ChurnResult] = []
    for pipeline, entries in groups.items():
        n = len(entries)
        if n < 2:
            continue
        transitions = _count_transitions(entries, pipeline)
        rate = transitions / n
        results.append(ChurnResult(
            pipeline=pipeline,
            transitions=transitions,
            total_entries=n,
            churn_rate=rate,
            is_churning=(transitions >= min_transitions and rate >= min_churn_rate),
        ))

    results.sort(key=lambda r: r.churn_rate, reverse=True)
    return ChurnReport(results=results)


def format_churn(report: ChurnReport) -> str:
    lines = ["=== Pipeline Churn Report ==="]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)
    lines.append(f"  {'Pipeline':<30} {'Transitions':>11} {'Entries':>7} {'Rate':>7} {'Status':>10}")
    lines.append("  " + "-" * 68)
    for r in report.results:
        status = "CHURNING" if r.is_churning else "ok"
        lines.append(
            f"  {r.pipeline:<30} {r.transitions:>11} {r.total_entries:>7} "
            f"{r.churn_rate * 100:>6.1f}% {status:>10}"
        )
    return "\n".join(lines)
