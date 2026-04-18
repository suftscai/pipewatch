"""Tests for pipewatch.ingestion.watcher."""

from __future__ import annotations

import time
import threading
from pathlib import Path

import pytest

from pipewatch.ingestion.watcher import watch_file, watch_file_burst


VALID_LINE = '2024-01-15T10:00:00 ERROR pipeline=etl_main stage=load msg="DB timeout"\n'
INFO_LINE  = '2024-01-15T10:01:00 INFO  pipeline=etl_main stage=extract msg="ok"\n'
BAD_LINE   = 'not a valid log line\n'


def _append_after(path: Path, lines: list[str], delay: float = 0.05) -> None:
    """Append *lines* to *path* after *delay* seconds (runs in a thread)."""
    def _write():
        time.sleep(delay)
        with path.open("a", encoding="utf-8") as fh:
            fh.writelines(lines)
    threading.Thread(target=_write, daemon=True).start()


def test_watch_file_skips_existing(tmp_path: Path) -> None:
    log = tmp_path / "pipeline.log"
    log.write_text(VALID_LINE)  # pre-existing line should be skipped

    _append_after(log, [VALID_LINE])

    events = []
    for event in watch_file(log, poll_interval=0.05, skip_existing=True):
        events.append(event)
        break  # stop after first new event

    assert len(events) == 1
    assert events[0].level == "ERROR"


def test_watch_file_replays_existing(tmp_path: Path) -> None:
    log = tmp_path / "pipeline.log"
    log.write_text(VALID_LINE)

    events = []
    for event in watch_file(log, poll_interval=0.05, skip_existing=False):
        events.append(event)
        break

    assert len(events) == 1


def test_watch_file_skips_invalid_lines(tmp_path: Path) -> None:
    log = tmp_path / "pipeline.log"
    log.write_text("")

    _append_after(log, [BAD_LINE, VALID_LINE])

    events = []
    for event in watch_file(log, poll_interval=0.02, skip_existing=True):
        events.append(event)
        break

    assert events[0].level == "ERROR"


def test_watch_file_burst_yields_batch(tmp_path: Path) -> None:
    log = tmp_path / "pipeline.log"
    log.write_text("")

    _append_after(log, [VALID_LINE, VALID_LINE, INFO_LINE], delay=0.02)

    for batch in watch_file_burst(log, burst_timeout=0.3, poll_interval=0.02, skip_existing=True):
        # First burst should contain the appended events
        assert isinstance(batch, list)
        assert len(batch) >= 2
        break
