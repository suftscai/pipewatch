"""Replay historical pipeline events for post-mortem analysis."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.export.history import HistoryEntry, load_history
from pipewatch.analysis.aggregator import PipelineSummary


@dataclass
class ReplayFrame:
    index: int
    entry: HistoryEntry
    delta_errors: int
    delta_warnings: int


@dataclass
class ReplayReport:
    frames: List[ReplayFrame]
    window: int


def compute_replay(history_path: str, window: int = 10) -> ReplayReport:
    """Build a sequence of ReplayFrames from recent history entries."""
    entries = load_history(history_path)
    entries = entries[-window:]

    frames: List[ReplayFrame] = []
    for i, entry in enumerate(entries):
        if i == 0:
            delta_errors = entry.total_errors
            delta_warnings = entry.total_warnings
        else:
            prev = entries[i - 1]
            delta_errors = entry.total_errors - prev.total_errors
            delta_warnings = entry.total_warnings - prev.total_warnings
        frames.append(ReplayFrame(
            index=i,
            entry=entry,
            delta_errors=delta_errors,
            delta_warnings=delta_warnings,
        ))
    return ReplayReport(frames=frames, window=window)


def format_replay(report: ReplayReport) -> str:
    """Render a replay report as a human-readable string."""
    if not report.frames:
        return "No history available for replay."

    lines = [f"Replay — last {report.window} snapshots", "-" * 42]
    for frame in report.frames:
        ts = frame.entry.timestamp[:19].replace("T", " ")
        sign_e = "+" if frame.delta_errors >= 0 else ""
        sign_w = "+" if frame.delta_warnings >= 0 else ""
        lines.append(
            f"[{frame.index:2d}] {ts}  "
            f"errors={frame.entry.total_errors} ({sign_e}{frame.delta_errors})  "
            f"warnings={frame.entry.total_warnings} ({sign_w}{frame.delta_warnings})"
        )
    return "\n".join(lines)
