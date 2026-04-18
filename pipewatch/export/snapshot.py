"""Point-in-time snapshot capture and comparison for pipeline state."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

from pipewatch.analysis.aggregator import PipelineSummary
from pipewatch.analysis.alert import Alert
from pipewatch.export.reporter import summary_to_dict


@dataclass
class Snapshot:
    timestamp: float
    summary: dict
    alerts: list[dict]
    label: str = ""


def capture(summary: PipelineSummary, alerts: list[Alert], label: str = "") -> Snapshot:
    return Snapshot(
        timestamp=time.time(),
        summary=summary_to_dict(summary, alerts),
        alerts=[{"pipeline": a.pipeline, "message": a.message} for a in alerts],
        label=label,
    )


def save_snapshot(snapshot: Snapshot, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(asdict(snapshot), f, indent=2)


def load_snapshot(path: Path) -> Optional[Snapshot]:
    if not path.exists():
        return None
    with open(path) as f:
        data = json.load(f)
    return Snapshot(**data)


def diff_snapshots(old: Snapshot, new: Snapshot) -> dict:
    """Return a simple diff of error counts between two snapshots."""
    old_counts = old.summary.get("error_counts", {})
    new_counts = new.summary.get("error_counts", {})
    pipelines = set(old_counts) | set(new_counts)
    changes = {}
    for p in pipelines:
        delta = new_counts.get(p, 0) - old_counts.get(p, 0)
        if delta != 0:
            changes[p] = delta
    return changes


def format_snapshot_diff(diff: dict) -> str:
    if not diff:
        return "No changes between snapshots."
    lines = ["Snapshot diff (error count changes):"]
    for pipeline, delta in sorted(diff.items()):
        sign = "+" if delta > 0 else ""
        lines.append(f"  {pipeline}: {sign}{delta}")
    return "\n".join(lines)
