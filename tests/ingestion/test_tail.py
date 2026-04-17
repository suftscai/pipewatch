"""Tests for pipewatch.ingestion.tail."""

import tempfile
from pathlib import Path

import pytest

from pipewatch.ingestion.tail import tail_lines


def _write_lines(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_tail_lines_fewer_than_n(tmp_path):
    log = tmp_path / "pipeline.log"
    _write_lines(log, ["line1", "line2", "line3"])
    result = tail_lines(log, n=10)
    assert len(result) == 3
    assert "line1\n" in result


def test_tail_lines_more_than_n(tmp_path):
    log = tmp_path / "pipeline.log"
    all_lines = [f"line{i}" for i in range(50)]
    _write_lines(log, all_lines)
    result = tail_lines(log, n=20)
    assert len(result) == 20
    assert "line49\n" in result
    assert "line0\n" not in result


def test_tail_lines_empty_file(tmp_path):
    log = tmp_path / "empty.log"
    log.write_text("")
    result = tail_lines(log, n=10)
    assert result == []


def test_tail_lines_exact_n(tmp_path):
    log = tmp_path / "pipeline.log"
    _write_lines(log, [f"line{i}" for i in range(5)])
    result = tail_lines(log, n=5)
    assert len(result) == 5
