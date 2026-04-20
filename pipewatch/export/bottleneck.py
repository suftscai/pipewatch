"""Bottleneck detection: identify pipelines with consistently high error counts."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class BottleneckResult:
    pipeline: str
    total_errors: int
    total_events: int
    failure_rate: float
    occurrences: int  # number of history windows where errors > 0


@dataclass
class BottleneckReport:
    results: List[BottleneckResult]
    window: int

    def has_bottlenecks(self) -> bool:
        return len(self.results) > 0


def _rate(errors: int, total: int) -> float:
    return errors / total if total > 0 else 0.0


def compute_bottleneck(
    history: List[HistoryEntry],
    window: int = 10,
    min_occurrences: int = 2,
    min_failure_rate: float = 0.1,
) -> BottleneckReport:
    """Find pipelines that repeatedly appear as error sources."""
    entries = history[-window:] if len(history) > window else history

    totals: dict[str, list[int]] = {}
    errors: dict[str, list[int]] = {}

    for entry in entries:
        for pipeline, count in entry.error_counts.items():
            totals.setdefault(pipeline, [])
            errors.setdefault(pipeline, [])
            event_count = entry.event_counts.get(pipeline, count)
            totals[pipeline].append(event_count)
            errors[pipeline].append(count)

    results: List[BottleneckResult] = []
    for pipeline in errors:
        err_list = errors[pipeline]
        tot_list = totals[pipeline]
        occurrences = sum(1 for e in err_list if e > 0)
        total_err = sum(err_list)
        total_evt = sum(tot_list)
        rate = _rate(total_err, total_evt)
        if occurrences >= min_occurrences and rate >= min_failure_rate:
            results.append(
                BottleneckResult(
                    pipeline=pipeline,
                    total_errors=total_err,
                    total_events=total_evt,
                    failure_rate=rate,
                    occurrences=occurrences,
                )
            )

    results.sort(key=lambda r: r.failure_rate, reverse=True)
    return BottleneckReport(results=results, window=window)


def format_bottleneck(report: BottleneckReport) -> str:
    lines = ["=== Bottleneck Report ==="]
    if not report.has_bottlenecks():
        lines.append("No bottlenecks detected.")
        return "\n".join(lines)
    lines.append(f"{'Pipeline':<30} {'Errors':>8} {'Events':>8} {'Rate':>8} {'Occurrences':>12}")
    lines.append("-" * 70)
    for r in report.results:
        lines.append(
            f"{r.pipeline:<30} {r.total_errors:>8} {r.total_events:>8} "
            f"{r.failure_rate:>7.1%} {r.occurrences:>12}"
        )
    return "\n".join(lines)
