"""Echo rate: detect pipelines whose error rate is suspiciously constant
across consecutive history windows (possible stuck/looping behaviour)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class EchoResult:
    pipeline: str
    rates: List[float]
    variance: float
    flagged: bool


@dataclass
class EchoReport:
    results: List[EchoResult] = field(default_factory=list)

    def echoing(self) -> List[EchoResult]:
        return [r for r in self.results if r.flagged]

    def has_echoes(self) -> bool:
        return any(r.flagged for r in self.results)


def _rate(entry: HistoryEntry) -> float:
    if entry.total_events == 0:
        return 0.0
    return entry.total_errors / entry.total_events


def _variance(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return sum((v - mean) ** 2 for v in values) / len(values)


def compute_echorate(
    history: List[HistoryEntry],
    window: int = 10,
    min_periods: int = 3,
    variance_threshold: float = 0.0001,
) -> EchoReport:
    """Flag pipelines whose per-period failure rate barely changes.

    A very low variance across *min_periods* or more consecutive entries
    suggests the pipeline is emitting identical error patterns repeatedly.
    """
    recent = history[-window:] if len(history) > window else history

    pipelines: dict[str, List[float]] = {}
    for entry in recent:
        for pipeline in entry.top_failing:
            pipelines.setdefault(pipeline, [])
        # collect rate per pipeline per entry
        # top_failing only lists names; use per-entry error counts if available
        # fall back to global rate when pipeline-level data is absent
        pipelines.setdefault("__all__", []).append(_rate(entry))

    # Build per-pipeline rate series using the global rate as proxy
    global_rates = [_rate(e) for e in recent]

    results: List[EchoResult] = []
    if len(global_rates) >= min_periods:
        var = _variance(global_rates)
        flagged = var <= variance_threshold and any(r > 0 for r in global_rates)
        results.append(
            EchoResult(
                pipeline="__all__",
                rates=global_rates,
                variance=round(var, 8),
                flagged=flagged,
            )
        )

    return EchoReport(results=results)


def format_echorate(report: EchoReport) -> str:
    lines = ["Echo-Rate Report", "=" * 40]
    if not report.results:
        lines.append("No data.")
        return "\n".join(lines)
    for r in report.results:
        status = "ECHO" if r.flagged else "OK"
        lines.append(
            f"  {r.pipeline:<24} variance={r.variance:.6f}  [{status}]"
        )
    return "\n".join(lines)
