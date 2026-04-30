"""Jitter detection: identifies pipelines with high variance in failure rates across periods."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.export.history import HistoryEntry


@dataclass
class JitterResult:
    pipeline: str
    mean_rate: float
    std_dev: float
    cv: float          # coefficient of variation = std_dev / mean_rate
    periods: int
    jittery: bool


@dataclass
class JitterReport:
    results: List[JitterResult]
    window: int
    cv_threshold: float
    min_periods: int


def jittering(report: JitterReport) -> List[JitterResult]:
    return [r for r in report.results if r.jittery]


def has_jitter(report: JitterReport) -> bool:
    return any(r.jittery for r in report.results)


def _rate(entry: HistoryEntry, pipeline: str) -> float:
    total = entry.totals.get(pipeline, 0)
    errors = entry.errors.get(pipeline, 0)
    return errors / total if total > 0 else 0.0


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std_dev(values: List[float], mean: float) -> float:
    if len(values) < 2:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return variance ** 0.5


def compute_jitter(
    history: List[HistoryEntry],
    window: int = 10,
    cv_threshold: float = 0.5,
    min_periods: int = 3,
) -> JitterReport:
    recent = history[-window:] if len(history) > window else history

    pipelines: set[str] = set()
    for entry in recent:
        pipelines.update(entry.totals.keys())

    results: List[JitterResult] = []
    for pipeline in sorted(pipelines):
        rates = [_rate(e, pipeline) for e in recent if pipeline in e.totals]
        if len(rates) < min_periods:
            continue
        mean = _mean(rates)
        std = _std_dev(rates, mean)
        cv = std / mean if mean > 0 else 0.0
        results.append(JitterResult(
            pipeline=pipeline,
            mean_rate=round(mean, 4),
            std_dev=round(std, 4),
            cv=round(cv, 4),
            periods=len(rates),
            jittery=cv >= cv_threshold,
        ))

    results.sort(key=lambda r: r.cv, reverse=True)
    return JitterReport(results=results, window=window, cv_threshold=cv_threshold, min_periods=min_periods)


def format_jitter(report: JitterReport) -> str:
    lines = ["=== Jitter Report ==="]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)
    lines.append(f"  {'Pipeline':<30} {'Mean':>7} {'StdDev':>8} {'CV':>7} {'Periods':>8} {'Flag':>6}")
    lines.append("  " + "-" * 72)
    for r in report.results:
        flag = "JITTER" if r.jittery else "ok"
        lines.append(
            f"  {r.pipeline:<30} {r.mean_rate:>7.2%} {r.std_dev:>8.4f} {r.cv:>7.3f} {r.periods:>8}  {flag:>6}"
        )
    return "\n".join(lines)
