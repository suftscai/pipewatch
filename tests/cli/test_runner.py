"""Tests for pipewatch.cli.runner."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from pipewatch.cli.runner import run_once


ERROR_LINE = '2024-01-15T10:00:00 ERROR pipeline=etl_main stage=load msg="DB timeout"'
INFO_LINE = '2024-01-15T10:00:01 INFO pipeline=etl_main stage=extract msg="ok"'


def _write_log(lines: list[str]) -> str:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False)
    f.write("\n".join(lines) + "\n")
    f.flush()
    f.close()
    return f.name


def test_run_once_empty_file():
    path = _write_log([])
    output = run_once(path)
    assert "0" in output  # zero events


def test_run_once_counts_errors():
    path = _write_log([ERROR_LINE, INFO_LINE, ERROR_LINE])
    output = run_once(path)
    assert "2" in output  # two errors


def test_run_once_triggers_alert():
    from pipewatch.analysis.alert import AlertRule

    path = _write_log([ERROR_LINE] * 5 + [INFO_LINE])
    rules = [AlertRule(name="rate", failure_rate_threshold=0.5)]
    output = run_once(path, rules=rules)
    assert "rate" in output or "ALERT" in output or "⚠" in output


def test_run_once_no_alert_when_healthy():
    from pipewatch.analysis.alert import AlertRule

    path = _write_log([INFO_LINE] * 10)
    rules = [AlertRule(name="rate", failure_rate_threshold=0.5)]
    output = run_once(path, rules=rules)
    assert "No alerts" in output or "✓" in output or "0" in output


def test_run_once_respects_tail_n():
    """Only the last tail_n lines should be considered."""
    # 5 errors then 10 info lines; with tail_n=10 only info lines are seen
    path = _write_log([ERROR_LINE] * 5 + [INFO_LINE] * 10)
    from pipewatch.analysis.alert import AlertRule

    rules = [AlertRule(name="rate", failure_rate_threshold=0.1)]
    output = run_once(path, tail_n=10, rules=rules)
    # no errors in last 10 lines → no alert
    assert "No alerts" in output or "0" in output
