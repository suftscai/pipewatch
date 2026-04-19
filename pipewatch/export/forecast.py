"""Simple linear forecast of failure rates based on history."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List
from pipewatch.export.history import HistoryEntry


@dataclass
class ForecastPoint:
    step: int
    predicted_failure_rate: float


@dataclass
class ForecastReport:
    pipeline: str
    history_points: int
    slope: float
    intercept: float
    forecast: List[ForecastPoint]


def _failure_rates(entries: List[HistoryEntry], pipeline: str) -> List[float]:
    rates = []
    for e in entries:
        total = e.total_events
        failures = e.failures_by_pipeline.get(pipeline, 0)
        rates.append(failures / total if total else 0.0)
    return rates


def _linear_fit(ys: List[float]):
    n = len(ys)
    if n < 2:
        return 0.0, ys[0] if ys else 0.0
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    num = sum((xs[i] - x_mean) * (ys[i] - y_mean) for i in range(n))
    den = sum((xs[i] - x_mean) ** 2 for i in range(n))
    slope = num / den if den else 0.0
    intercept = y_mean - slope * x_mean
    return slope, intercept


def compute_forecast(
    history: List[HistoryEntry],
    pipeline: str,
    steps: int = 3,
    window: int = 10,
) -> ForecastReport:
    recent = history[-window:] if len(history) > window else history
    rates = _failure_rates(recent, pipeline)
    slope, intercept = _linear_fit(rates)
    n = len(rates)
    forecast = [
        ForecastPoint(
            step=i + 1,
            predicted_failure_rate=max(0.0, min(1.0, intercept + slope * (n + i))),
        )
        for i in range(steps)
    ]
    return ForecastReport(
        pipeline=pipeline,
        history_points=n,
        slope=round(slope, 6),
        intercept=round(intercept, 6),
        forecast=forecast,
    )


def format_forecast(report: ForecastReport) -> str:
    lines = [
        f"Forecast for pipeline: {report.pipeline}",
        f"  History points : {report.history_points}",
        f"  Trend (slope)  : {report.slope:+.4f}",
    ]
    for fp in report.forecast:
        pct = report.forecast[0].predicted_failure_rate  # keep ref quiet
        pct = fp.predicted_failure_rate * 100
        lines.append(f"  +{fp.step} period(s)    : {pct:.1f}% predicted failure rate")
    return "\n".join(lines)
