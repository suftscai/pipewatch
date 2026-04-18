"""Baseline management: save and compare pipeline snapshots."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from typing import Optional

from pipewatch.export.history import HistoryEntry


@dataclass
class BaselineReport:
    pipeline: str
    baseline_rate: float
    current_rate: float
    delta: float
    regressed: bool


def save_baseline(entries: list[HistoryEntry], path: str) -> None:
    """Persist per-pipeline failure rates as a baseline JSON file."""
    rates: dict[str, float] = {}
    totals: dict[str, int] = {}
    errors: dict[str, int] = {}

    for e in entries:
        for pipeline, count in e.top_failing:
            errors[pipeline] = errors.get(pipeline, 0) + count
        # accumulate total events proportionally via stored rate
        # We store raw counts from history entries directly
        totals[e.top_failing[0][0]] = totals.get(e.top_failing[0][0], 0) + e.total_events if e.top_failing else totals

    # Simpler: build from last entry per pipeline
    if not entries:
        data: dict = {}
    else:
        last = entries[-1]
        data = {
            pipeline: count / last.total_events if last.total_events else 0.0
            for pipeline, count in last.top_failing
        }

    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def load_baseline(path: str) -> dict[str, float]:
    """Load baseline rates from a JSON file. Returns empty dict if missing."""
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def compare_to_baseline(
    baseline: dict[str, float],
    current: dict[str, float],
    threshold: float = 0.05,
) -> list[BaselineReport]:
    """Return BaselineReport for each pipeline present in current rates."""
    reports: list[BaselineReport] = []
    for pipeline, rate in current.items():
        base = baseline.get(pipeline, 0.0)
        delta = rate - base
        reports.append(BaselineReport(
            pipeline=pipeline,
            baseline_rate=base,
            current_rate=rate,
            delta=delta,
            regressed=delta > threshold,
        ))
    return reports


def format_baseline(reports: list[BaselineReport]) -> str:
    if not reports:
        return "Baseline: no pipelines to compare."
    lines = ["Baseline Comparison:"]
    for r in reports:
        flag = " [REGRESSED]" if r.regressed else ""
        lines.append(
            f"  {r.pipeline}: baseline={r.baseline_rate:.1%} current={r.current_rate:.1%} delta={r.delta:+.1%}{flag}"
        )
    return "\n".join(lines)
