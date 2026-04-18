"""Periodic report scheduler for exporting pipeline summaries on an interval."""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Callable, Optional

from pipewatch.analysis.aggregator import PipelineSummary
from pipewatch.analysis.alert import Alert
from pipewatch.export.reporter import write_report


class ReportScheduler:
    """Runs write_report on a fixed interval in a background thread."""

    def __init__(
        self,
        output_path: str,
        fmt: str,
        interval: float,
        get_state: Callable[[], tuple[PipelineSummary, list[Alert]]],
    ) -> None:
        self.output_path = output_path
        self.fmt = fmt
        self.interval = interval
        self.get_state = get_state
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the background reporting thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Signal the background thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval + 1)

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self._export()
            self._stop_event.wait(timeout=self.interval)
        # Final flush on stop
        self._export()

    def _export(self) -> None:
        try:
            summary, alerts = self.get_state()
            write_report(summary, alerts, self.output_path, self.fmt)
        except Exception:  # noqa: BLE001
            pass  # Never crash the scheduler loop


def make_scheduler(
    output_path: str,
    fmt: str,
    interval: float,
    get_state: Callable[[], tuple[PipelineSummary, list[Alert]]],
) -> ReportScheduler:
    """Factory helper used by the CLI."""
    return ReportScheduler(output_path, fmt, interval, get_state)
