"""Trend analysis over historical pipeline data."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class TrendReport:
    window: int
    total_runs: int
    avg_failure_rate: float
    max_failure_rate: float
    total_errors: int
    total_warnings: int
    is_degrading: bool  # True if failure rate is consistently rising


def _failure_rates(entries: List[HistoryEntry]) -> List[float]:
    return [
        e.error_count / e.total_events if e.total_events > 0 else 0.0
        for e in entries
    ]


def compute_trend(entries: List[HistoryEntry], window: int = 10) -> TrendReport:
    """Compute a trend report from the most recent *window* history entries."""
    recent = entries[-window:] if len(entries) > window else entries

    if not recent:
        return TrendReport(
            window=window,
            total_runs=0,
            avg_failure_rate=0.0,
            max_failure_rate=0.0,
            total_errors=0,
            total_warnings=0,
            is_degrading=False,
        )

    rates = _failure_rates(recent)
    avg_rate = sum(rates) / len(rates)
    max_rate = max(rates)
    total_errors = sum(e.error_count for e in recent)
    total_warnings = sum(e.warning_count for e in recent)

    # Degrading: each half of the window has a higher avg than the first half
    is_degrading = False
    if len(rates) >= 4:
        mid = len(rates) // 2
        first_half_avg = sum(rates[:mid]) / mid
        second_half_avg = sum(rates[mid:]) / (len(rates) - mid)
        is_degrading = second_half_avg > first_half_avg * 1.1

    return TrendReport(
        window=window,
        total_runs=len(recent),
        avg_failure_rate=round(avg_rate, 4),
        max_failure_rate=round(max_rate, 4),
        total_errors=total_errors,
        total_warnings=total_warnings,
        is_degrading=is_degrading,
    )


def format_trend(report: TrendReport) -> str:
    """Return a human-readable summary of the trend report."""
    lines = [
        f"Trend Report (last {report.total_runs} runs)",
        f"  Avg failure rate : {report.avg_failure_rate * 100:.1f}%",
        f"  Max failure rate : {report.max_failure_rate * 100:.1f}%",
        f"  Total errors     : {report.total_errors}",
        f"  Total warnings   : {report.total_warnings}",
        f"  Degrading        : {'YES ⚠' if report.is_degrading else 'no'}",
    ]
    return "\n".join(lines)
