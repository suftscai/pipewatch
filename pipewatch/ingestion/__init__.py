"""Ingestion sub-package: log parsing, tailing, and file watching."""

from pipewatch.ingestion.log_parser import PipelineEvent, parse_line, parse_lines
from pipewatch.ingestion.tail import tail_file, tail_lines
from pipewatch.ingestion.watcher import watch_file, watch_file_burst

__all__ = [
    "PipelineEvent",
    "parse_line",
    "parse_lines",
    "tail_file",
    "tail_lines",
    "watch_file",
    "watch_file_burst",
]
