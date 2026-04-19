"""SLA compliance tracking per pipeline."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict
from pipewatch.export.history import HistoryEntry


@dataclass
class SLAResult:
    pipeline: str
    total_events: int
    error_events: int
    failure_rate: float
    compliant: bool
    threshold: float


@dataclass
class SLAReport:
    results: List[SLAResult] = field(default_factory=list)
    threshold: float = 0.05

    @property
    def all_compliant(self) -> bool:
        return all(r.compliant for r in self.results)

    @property
    def violations(self) -> List[SLAResult]:
        return [r for r in self.results if not r.compliant]


def compute_sla(
    history: List[HistoryEntry],
    threshold: float = 0.05,
    window: int = 20,
) -> SLAReport:
    recent = history[-window:] if len(history) > window else history
    totals: Dict[str, int] = {}
    errors: Dict[str, int] = {}
    for entry in recent:
        for pipeline, count in entry.error_counts.items():
            totals[pipeline] = totals.get(pipeline, 0) + entry.total_events
            errors[pipeline] = errors.get(pipeline, 0) + count
    results = []
    for pipeline in totals:
        total = totals[pipeline]
        err = errors[pipeline]
        rate = err / total if total > 0 else 0.0
        results.append(SLAResult(
            pipeline=pipeline,
            total_events=total,
            error_events=err,
            failure_rate=rate,
            compliant=rate <= threshold,
            threshold=threshold,
        ))
    results.sort(key=lambda r: r.failure_rate, reverse=True)
    return SLAReport(results=results, threshold=threshold)


def format_sla(report: SLAReport) -> str:
    if not report.results:
        return "SLA Report: no data."
    lines = [f"SLA Report (threshold={report.threshold:.1%})"]
    for r in report.results:
        status = "OK" if r.compliant else "VIOLATION"
        lines.append(
            f"  [{status}] {r.pipeline}: {r.failure_rate:.1%} "
            f"({r.error_events}/{r.total_events} errors)"
        )
    return "\n".join(lines)
