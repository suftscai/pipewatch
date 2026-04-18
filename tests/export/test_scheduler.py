"""Tests for the periodic report scheduler."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from pipewatch.analysis.aggregator import PipelineSummary
from pipewatch.analysis.alert import Alert
from pipewatch.export.scheduler import ReportScheduler


def _summary() -> PipelineSummary:
    return PipelineSummary(
        total=10,
        errors=2,
        warnings=1,
        pipeline_counts={"etl": 3},
    )


def _alerts() -> list[Alert]:
    return []


def test_scheduler_writes_file(tmp_path: Path) -> None:
    out = tmp_path / "report.json"
    calls: list[int] = []

    def get_state():
        calls.append(1)
        return _summary(), _alerts()

    sched = ReportScheduler(str(out), "json", interval=0.05, get_state=get_state)
    sched.start()
    time.sleep(0.18)
    sched.stop()

    assert out.exists(), "report file should have been written"
    data = json.loads(out.read_text())
    assert data["total"] == 10
    # Should have fired at least twice (start + one interval)
    assert len(calls) >= 2


def test_scheduler_final_flush(tmp_path: Path) -> None:
    out = tmp_path / "report.json"

    sched = ReportScheduler(
        str(out), "json", interval=10.0, get_state=lambda: (_summary(), _alerts())
    )
    sched.start()
    # Stop almost immediately — final flush should still write
    time.sleep(0.02)
    sched.stop()

    assert out.exists()


def test_scheduler_does_not_crash_on_bad_state(tmp_path: Path) -> None:
    out = tmp_path / "report.json"

    def bad_state():
        raise RuntimeError("boom")

    sched = ReportScheduler(str(out), "json", interval=0.05, get_state=bad_state)
    sched.start()
    time.sleep(0.12)
    sched.stop()  # Should not raise


def test_make_scheduler_returns_instance() -> None:
    from pipewatch.export.scheduler import make_scheduler

    sched = make_scheduler("/tmp/x.json", "json", 5.0, lambda: (_summary(), []))
    assert isinstance(sched, ReportScheduler)
    assert sched.interval == 5.0
    assert sched.fmt == "json"
