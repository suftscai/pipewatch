"""Pipeline pressure detection: identifies pipelines under sustained high error load."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class PressureResult:
    pipeline: str
    avg_failure_rate: float
    peak_failure_rate: float
    periods_above_threshold: int
    total_periods: int
    under_pressure: bool


@dataclass
class PressureReport:
    results: List[PressureResult] = field(default_factory=list)

    def pressured(self) -> List[PressureResult]:
        return [r for r in self.results if r.under_pressure]

    def has_pressure(self) -> bool:
        return any(r.under_pressure for r in self.results)


def _rate(errors: int, total: int) -> float:
    return errors / total if total > 0 else 0.0


def _group_by_pipeline(entries: List[HistoryEntry]) -> dict:
    groups: dict = {}
    for entry in entries:
        for pipeline, counts in entry.per_pipeline.items():
            groups.setdefault(pipeline, []).append(counts)
    return groups


def compute_pressure(
    history: List[HistoryEntry],
    window: int = 24,
    rate_threshold: float = 0.3,
    min_periods: int = 3,
    pressure_ratio: float = 0.5,
) -> PressureReport:
    """Flag pipelines where failure rate exceeds threshold in >= pressure_ratio of recent periods."""
    if not history:
        return PressureReport()

    recent = history[-window:]
    groups = _group_by_pipeline(recent)
    results: List[PressureResult] = []

    for pipeline, period_counts in groups.items():
        if len(period_counts) < min_periods:
            continue
        rates = [_rate(c.get("errors", 0), c.get("total", 0)) for c in period_counts]
        above = sum(1 for r in rates if r >= rate_threshold)
        avg = sum(rates) / len(rates)
        peak = max(rates)
        ratio = above / len(rates)
        results.append(
            PressureResult(
                pipeline=pipeline,
                avg_failure_rate=round(avg, 4),
                peak_failure_rate=round(peak, 4),
                periods_above_threshold=above,
                total_periods=len(rates),
                under_pressure=ratio >= pressure_ratio,
            )
        )

    results.sort(key=lambda r: r.avg_failure_rate, reverse=True)
    return PressureReport(results=results)


def format_pressure(report: PressureReport) -> str:
    lines = ["Pipeline Pressure Report", "=" * 40]
    if not report.results:
        lines.append("No data.")
        return "\n".join(lines)
    for r in report.results:
        flag = " [PRESSURE]" if r.under_pressure else ""
        lines.append(
            f"{r.pipeline}{flag}: avg={r.avg_failure_rate:.1%} "
            f"peak={r.peak_failure_rate:.1%} "
            f"({r.periods_above_threshold}/{r.total_periods} periods above threshold)"
        )
    return "\n".join(lines)
