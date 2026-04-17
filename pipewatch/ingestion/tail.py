"""Tail a log file and emit new lines in real time."""

import time
from collections.abc import Generator
from pathlib import Path


def tail_file(
    path: str | Path,
    poll_interval: float = 0.5,
    from_start: bool = False,
) -> Generator[str, None, None]:
    """Yield new lines appended to *path* as they arrive.

    Args:
        path: Path to the log file to watch.
        poll_interval: Seconds between read attempts.
        from_start: If True, read the file from the beginning first.
    """
    path = Path(path)
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        if not from_start:
            fh.seek(0, 2)  # seek to end

        while True:
            line = fh.readline()
            if line:
                yield line
            else:
                time.sleep(poll_interval)


def tail_lines(
    path: str | Path,
    n: int = 20,
) -> list[str]:
    """Return the last *n* lines of *path* without blocking."""
    path = Path(path)
    lines: list[str] = []
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            lines.append(line)
            if len(lines) > n:
                lines.pop(0)
    return lines
