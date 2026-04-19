"""Correlation analysis between pipeline failure rates."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple
from pipewatch.export.history import HistoryEntry


@dataclass
class CorrelationPair:
    pipeline_a: str
    pipeline_b: str
    coefficient: float  # -1.0 to 1.0
    strength: str  # "strong", "moderate", "weak", "none"


@dataclass
class CorrelationReport:
    pairs: List[CorrelationPair]


def _failure_rates_by_pipeline(entries: List[HistoryEntry]) -> dict[str, List[float]]:
    rates: dict[str, List[float]] = {}
    for e in entries:
        for p, count in e.error_counts.items():
            total = e.total_events.get(p, 0)
            rate = count / total if total > 0 else 0.0
            rates.setdefault(p, []).append(rate)
    return rates


def _pearson(a: List[float], b: List[float]) -> float:
    n = min(len(a), len(b))
    if n < 2:
        return 0.0
    a, b = a[:n], b[:n]
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    num = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b))
    den_a = sum((x - mean_a) ** 2 for x in a) ** 0.5
    den_b = sum((y - mean_b) ** 2 for y in b) ** 0.5
    if den_a == 0 or den_b == 0:
        return 0.0
    return num / (den_a * den_b)


def _strength(coeff: float) -> str:
    abs_c = abs(coeff)
    if abs_c >= 0.7:
        return "strong"
    if abs_c >= 0.4:
        return "moderate"
    if abs_c >= 0.2:
        return "weak"
    return "none"


def compute_correlation(entries: List[HistoryEntry], window: int = 50) -> CorrelationReport:
    recent = entries[-window:]
    rates = _failure_rates_by_pipeline(recent)
    pipelines = sorted(rates.keys())
    pairs: List[CorrelationPair] = []
    for i, pa in enumerate(pipelines):
        for pb in pipelines[i + 1:]:
            coeff = _pearson(rates[pa], rates[pb])
            pairs.append(CorrelationPair(pa, pb, round(coeff, 4), _strength(coeff)))
    pairs.sort(key=lambda p: abs(p.coefficient), reverse=True)
    return CorrelationReport(pairs=pairs)


def format_correlation(report: CorrelationReport) -> str:
    if not report.pairs:
        return "No correlation data available."
    lines = ["Pipeline Failure Correlations", "-" * 40]
    for p in report.pairs:
        lines.append(f"  {p.pipeline_a} <-> {p.pipeline_b}: {p.coefficient:+.4f} ({p.strength})")
    return "\n".join(lines)
