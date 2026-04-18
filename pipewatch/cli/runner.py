"""CLI entry point for pipewatch — wires ingestion, analysis, and display."""

from __future__ import annotations

import time
from pathlib import Path
from typing import List

from pipewatch.analysis.aggregator import aggregate
from pipewatch.analysis.alert import Alert, AlertRule, evaluate_alerts
from pipewatch.display.formatter import render
from pipewatch.ingestion import parse_lines
from pipewatch.ingestion.watcher import watch_file_burst


def _default_rules() -> List[AlertRule]:
    return [
        AlertRule(name="High failure rate", failure_rate_threshold=0.25),
        AlertRule(name="Too many errors", absolute_failure_threshold=10),
    ]


def run_once(
    log_path: str,
    *,
    tail_n: int = 200,
    rules: List[AlertRule] | None = None,
) -> str:
    """Parse the last *tail_n* lines and return a rendered snapshot."""
    from pipewatch.ingestion.tail import tail_lines

    lines = tail_lines(log_path, tail_n)
    events = list(parse_lines(lines))
    summary = aggregate(events)
    alerts: List[Alert] = evaluate_alerts(summary, rules or _default_rules())
    return render(summary, alerts)


def run_watch(
    log_path: str,
    *,
    interval: float = 2.0,
    window: int = 200,
    rules: List[AlertRule] | None = None,
    max_cycles: int | None = None,
) -> None:
    """Continuously watch *log_path* and print a refreshed dashboard."""
    import os

    active_rules = rules or _default_rules()
    cycles = 0
    try:
        for burst in watch_file_burst(log_path, interval=interval, replay_existing=False):
            _ = burst  # new lines trigger a full re-render from tail
            output = run_once(log_path, tail_n=window, rules=active_rules)
            os.system("clear" if os.name != "nt" else "cls")
            print(output)
            cycles += 1
            if max_cycles is not None and cycles >= max_cycles:
                break
    except KeyboardInterrupt:
        pass
