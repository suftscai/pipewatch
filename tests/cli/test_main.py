"""Tests for pipewatch.cli.main (argument parsing & dispatch)."""

from __future__ import annotations

import tempfile
from unittest.mock import patch

import pytest

from pipewatch.cli.main import build_parser, main

INFO_LINE = '2024-01-15T10:00:00 INFO pipeline=p stage=s msg="ok"'


def _log(lines: list[str]) -> str:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False)
    f.write("\n".join(lines) + "\n")
    f.flush()
    f.close()
    return f.name


def test_parser_defaults():
    parser = build_parser()
    args = parser.parse_args(["some.log"])
    assert args.log_file == "some.log"
    assert args.watch is False
    assert args.interval == 2.0
    assert args.tail == 200
    assert args.failure_rate == 0.25
    assert args.max_failures == 10


def test_parser_watch_flag():
    parser = build_parser()
    args = parser.parse_args(["some.log", "--watch", "--interval", "5"])
    assert args.watch is True
    assert args.interval == 5.0


def test_parser_custom_thresholds():
    parser = build_parser()
    args = parser.parse_args(["x.log", "--failure-rate", "0.1", "--max-failures", "3"])
    assert args.failure_rate == 0.1
    assert args.max_failures == 3


def test_main_snapshot_prints_output(capsys):
    path = _log([INFO_LINE] * 3)
    main([path])
    captured = capsys.readouterr()
    assert len(captured.out) > 0


def test_main_watch_calls_run_watch():
    path = _log([INFO_LINE])
    with patch("pipewatch.cli.main.run_watch") as mock_watch:
        main([path, "--watch", "--interval", "1"])
        mock_watch.assert_called_once()
        _, kwargs = mock_watch.call_args
        assert kwargs["interval"] == 1.0
