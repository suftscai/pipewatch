"""Tests for pipewatch.ingestion.log_parser."""

from datetime import datetime

import pytest

from pipewatch.ingestion.log_parser import PipelineEvent, parse_line, parse_lines


VALID_LINE = "2024-05-01T12:00:00 ERROR orders-etl Failed to connect to database"
INFO_LINE  = "2024-05-01T12:01:00 INFO  sales-pipeline Stage completed successfully"
BAD_LINE   = "this is not a valid log line"


def test_parse_line_error():
    event = parse_line(VALID_LINE)
    assert isinstance(event, PipelineEvent)
    assert event.level == "ERROR"
    assert event.pipeline == "orders-etl"
    assert "database" in event.message
    assert event.is_failure is True
    assert event.timestamp == datetime(2024, 5, 1, 12, 0, 0)


def test_parse_line_info_not_failure():
    event = parse_line(INFO_LINE)
    assert event is not None
    assert event.is_failure is False
    assert event.level == "INFO"


def test_parse_line_invalid_returns_none():
    assert parse_line(BAD_LINE) is None
    assert parse_line("") is None
    assert parse_line("   ") is None


def test_parse_lines_skips_bad():
    lines = [VALID_LINE, BAD_LINE, INFO_LINE]
    events = parse_lines(lines)
    assert len(events) == 2
    assert events[0].pipeline == "orders-etl"
    assert events[1].pipeline == "sales-pipeline"


def test_parse_lines_empty():
    assert parse_lines([]) == []


def test_raw_preserved():
    event = parse_line(VALID_LINE)
    assert event.raw == VALID_LINE.strip()


@pytest.mark.parametrize("level,expected_failure", [
    ("ERROR", True),
    ("WARN",  True),
    ("INFO",  False),
    ("DEBUG", False),
])
def test_is_failure_by_level(level, expected_failure):
    """Verify that is_failure reflects the log level correctly."""
    line = f"2024-05-01T12:00:00 {level:<5} orders-etl Some message here"
    event = parse_line(line)
    assert event is not None, f"Expected parse_line to succeed for level {level!r}"
    assert event.is_failure is expected_failure
