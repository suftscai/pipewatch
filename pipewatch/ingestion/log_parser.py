"""Parse ETL pipeline log lines into structured events."""

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

LOG_PATTERN = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})\s+"
    r"(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+"
    r"(?P<pipeline>[\w\-]+)\s+"
    r"(?P<message>.+)"
)


@dataclass
class PipelineEvent:
    timestamp: datetime
    level: str
    pipeline: str
    message: str
    raw: str = field(repr=False)
    is_failure: bool = False

    def __post_init__(self):
        self.is_failure = self.level in ("ERROR", "CRITICAL")


def parse_line(line: str) -> Optional[PipelineEvent]:
    """Parse a single log line. Returns None if line doesn't match."""
    line = line.strip()
    match = LOG_PATTERN.match(line)
    if not match:
        return None

    ts_str = match.group("timestamp").replace(" ", "T")
    try:
        timestamp = datetime.fromisoformat(ts_str)
    except ValueError:
        return None

    return PipelineEvent(
        timestamp=timestamp,
        level=match.group("level"),
        pipeline=match.group("pipeline"),
        message=match.group("message"),
        raw=line,
    )


def parse_lines(lines: list[str]) -> list[PipelineEvent]:
    """Parse multiple log lines, skipping unparseable ones."""
    events = []
    for line in lines:
        event = parse_line(line)
        if event:
            events.append(event)
    return events
