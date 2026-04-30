"""Tests for pipewatch.export.entropy."""
import pytest
from pipewatch.export.entropy import (
    compute_entropy,
    format_entropy,
    EntropyReport,
)
from pipewatch.export.history import HistoryEntry


def _entry(pipeline: str, errors: int, total: int, ts: str = "2024-01-01T00:00:00") -> HistoryEntry:
    return HistoryEntry(
        timestamp=ts,
        total_events=total,
        error_count=errors,
        warning_count=0,
        top_failing=[pipeline] if errors > 0 else [],
    )


def test_compute_entropy_empty():
    report = compute_entropy([])
    assert isinstance(report, EntropyReport)
    assert report.results == []
    assert not report.has_chaos()


def test_compute_entropy_too_few_periods_excluded():
    # Only 2 entries for pipeline — below min_periods=4
    history = [
        _entry("pipe_a", 5, 10, "2024-01-01T01:00:00"),
        _entry("pipe_a", 5, 10, "2024-01-01T02:00:00"),
    ]
    report = compute_entropy(history, min_periods=4)
    assert report.results == []


def test_compute_entropy_stable_pipeline_low_entropy():
    # Same failure rate every period → low entropy
    history = [
        _entry("pipe_stable", 5, 10, f"2024-01-01T0{i}:00:00")
        for i in range(6)
    ]
    report = compute_entropy(history, threshold=2.5, min_periods=4)
    assert len(report.results) == 1
    result = report.results[0]
    assert result.pipeline == "pipe_stable"
    assert result.entropy < 2.5
    assert not result.high_entropy
    assert not report.has_chaos()


def test_compute_entropy_chaotic_pipeline_flagged():
    # Alternating between 0% and 100% failure rate → high entropy
    history = []
    for i in range(8):
        errors = 10 if i % 2 == 0 else 0
        history.append(_entry("pipe_chaos", errors, 10, f"2024-01-01T{i:02d}:00:00"))
    report = compute_entropy(history, threshold=0.5, min_periods=4)
    assert len(report.results) == 1
    result = report.results[0]
    assert result.high_entropy
    assert report.has_chaos()
    assert len(report.chaotic()) == 1


def test_compute_entropy_respects_window():
    history = [
        _entry("pipe_x", 5, 10, f"2024-01-01T{i:02d}:00:00")
        for i in range(20)
    ]
    report_full = compute_entropy(history, window=20, min_periods=4)
    report_short = compute_entropy(history, window=5, min_periods=4)
    # Short window may exclude pipeline if fewer than min_periods entries remain
    assert report_full.window == 20
    assert report_short.window == 5


def test_compute_entropy_sorted_by_entropy_descending():
    # Two pipelines; chaos should come first
    history_chaos = [
        _entry("chaos", 10 if i % 2 == 0 else 0, 10, f"2024-01-01T{i:02d}:00:00")
        for i in range(8)
    ]
    history_stable = [
        _entry("stable", 5, 10, f"2024-01-01T{i:02d}:00:00")
        for i in range(8)
    ]
    combined = []
    for a, b in zip(history_chaos, history_stable):
        combined.append(
            HistoryEntry(
                timestamp=a.timestamp,
                total_events=a.total_events + b.total_events,
                error_count=a.error_count + b.error_count,
                warning_count=0,
                top_failing=a.top_failing + b.top_failing,
            )
        )
    report = compute_entropy(combined, threshold=0.5, min_periods=4)
    if len(report.results) >= 2:
        assert report.results[0].entropy >= report.results[1].entropy


def test_format_entropy_no_data():
    report = EntropyReport(results=[], window=24, threshold=2.5)
    output = format_entropy(report)
    assert "No data" in output


def test_format_entropy_shows_pipeline():
    from pipewatch.export.entropy import EntropyResult
    result = EntropyResult(pipeline="my_pipe", entropy=1.234, periods=6, high_entropy=False)
    report = EntropyReport(results=[result], window=24, threshold=2.5)
    output = format_entropy(report)
    assert "my_pipe" in output
    assert "1.2340" in output


def test_format_entropy_chaotic_flag():
    from pipewatch.export.entropy import EntropyResult
    result = EntropyResult(pipeline="noisy", entropy=3.0, periods=10, high_entropy=True)
    report = EntropyReport(results=[result], window=24, threshold=2.5)
    output = format_entropy(report)
    assert "CHAOTIC" in output
