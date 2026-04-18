"""File watcher that continuously monitors a log file and emits PipelineEvents."""

from __future__ import annotations

import time
from collections.abc import Generator
from pathlib import Path

from pipewatch.ingestion.log_parser import PipelineEvent, parse_line


def watch_file(
    path: str | Path,
    poll_interval: float = 0.5,
    skip_existing: bool = True,
) -> Generator[PipelineEvent, None, None]:
    """Yield PipelineEvents as new lines are appended to *path*.

    Args:
        path: Path to the log file to watch.
        poll_interval: Seconds to sleep between read attempts.
        skip_existing: If True, seek to end of file before watching so that
            only *new* lines are emitted.  If False, replay from the start.
    """
    path = Path(path)

    with path.open("r", encoding="utf-8") as fh:
        if skip_existing:
            fh.seek(0, 2)  # seek to end

        while True:
            line = fh.readline()
            if line:
                event = parse_line(line)
                if event is not None:
                    yield event
            else:
                time.sleep(poll_interval)


def watch_file_burst(
    path: str | Path,
    burst_timeout: float = 1.0,
    poll_interval: float = 0.1,
    skip_existing: bool = True,
) -> Generator[list[PipelineEvent], None, None]:
    """Yield batches of PipelineEvents collected during *burst_timeout* seconds.

    Useful for feeding the aggregator periodically rather than event-by-event.
    """
    path = Path(path)

    with path.open("r", encoding="utf-8") as fh:
        if skip_existing:
            fh.seek(0, 2)

        while True:
            deadline = time.monotonic() + burst_timeout
            batch: list[PipelineEvent] = []

            while time.monotonic() < deadline:
                line = fh.readline()
                if line:
                    event = parse_line(line)
                    if event is not None:
                        batch.append(event)
                else:
                    time.sleep(poll_interval)

            yield batch
