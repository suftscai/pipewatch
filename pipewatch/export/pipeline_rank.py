"""Rank pipelines by failure rate over a history window."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List
from pipewatch.export.history import HistoryEntry


@dataclass
class PipelineRank:
    pipeline: str
    total_events: int
    error_count: int
    failure_rate: float
    rank: int


@dataclass
class RankReport:
    window: int
    rankings: List[PipelineRank]


def _rate(errors: int, total: int) -> float:
    return errors / total if total > 0 else 0.0


def compute_rank(history: List[HistoryEntry], window: int = 20) -> RankReport:
    recent = history[-window:] if len(history) > window else history

    totals: dict[str, int] = {}
    errors: dict[str, int] = {}

    for entry in recent:
        for pipeline, count in entry.error_counts.items():
            errors[pipeline] = errors.get(pipeline, 0) + count
        for pipeline, count in entry.event_counts.items():
            totals[pipeline] = totals.get(pipeline, 0) + count

    all_pipelines = set(totals) | set(errors)
    ranked = sorted(
        all_pipelines,
        key=lambda p: _rate(errors.get(p, 0), totals.get(p, 0)),
        reverse=True,
    )

    rankings = [
        PipelineRank(
            pipeline=p,
            total_events=totals.get(p, 0),
            error_count=errors.get(p, 0),
            failure_rate=_rate(errors.get(p, 0), totals.get(p, 0)),
            rank=i + 1,
        )
        for i, p in enumerate(ranked)
    ]
    return RankReport(window=window, rankings=rankings)


def format_rank(report: RankReport) -> str:
    if not report.rankings:
        return "No pipeline data available."
    lines = [f"Pipeline Rankings (window={report.window}):", ""]
    for r in report.rankings:
        bar = "#" * int(r.failure_rate * 20)
        lines.append(
            f"  #{r.rank:>2} {r.pipeline:<30} "
            f"{r.failure_rate*100:5.1f}%  [{bar:<20}]  "
            f"({r.error_count}/{r.total_events} errors)"
        )
    return "\n".join(lines)
