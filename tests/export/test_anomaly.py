"""Tests for pipewatch.export.anomaly."""
import pytest
from pipewatch.export.anomaly import (
    detect_anomalies,
    format_anomaly,
    AnomalyReport,
)
from pipewatch.export.history import HistoryEntry


def _entry(failures_by_pipeline, events_by_pipeline, ts="2024-01-01T00:00:00"):
    return HistoryEntry(
        timestamp=ts,
        total_events=sum(events_by_pipeline.values()),
        total_failures=sum(failures_by_pipeline.values()),
        failure_rate=0.0,
        failures_by_pipeline=failures_by_pipeline,
        events_by_pipeline=events_by_pipeline,
    )


def test_no_anomaly_when_stable():
    history = [_entry({"etl": 1}, {"etl": 10}) for _ in range(5)]
    current = _entry({"etl": 1}, {"etl": 10})
    report = detect_anomalies(history, current, threshold=0.15)
    assert not report.has_anomalies
    assert len(report.anomalies) == 1
    assert report.anomalies[0].pipeline == "etl"


def test_anomaly_detected_on_spike():
    history = [_entry({"etl": 0}, {"etl": 10}) for _ in range(5)]
    current = _entry({"etl": 5}, {"etl": 10})  # 50% failure rate
    report = detect_anomalies(history, current, threshold=0.15)
    assert report.has_anomalies
    assert report.anomalies[0].is_anomaly
    assert report.anomalies[0].delta == pytest.approx(0.5)


def test_empty_history_no_anomaly():
    current = _entry({"etl": 3}, {"etl": 10})
    report = detect_anomalies([], current, threshold=0.15)
    # baseline_rate is 0, delta = 0.3 > 0.15 => anomaly
    assert report.has_anomalies


def test_window_limits_history():
    old = [_entry({"etl": 9}, {"etl": 10}) for _ in range(20)]
    recent = [_entry({"etl": 0}, {"etl": 10}) for _ in range(5)]
    current = _entry({"etl": 1}, {"etl": 10})
    report = detect_anomalies(old + recent, current, threshold=0.15, window=5)
    # baseline from recent 5 = 0%, current = 10%, delta = 0.10 < 0.15
    assert not report.has_anomalies


def test_format_anomaly_empty():
    report = AnomalyReport(anomalies=[], threshold=0.15)
    assert "No anomaly" in format_anomaly(report)


def test_format_anomaly_shows_flag():
    history = [_entry({"pipe": 0}, {"pipe": 10})]
    current = _entry({"pipe": 8}, {"pipe": 10})
    report = detect_anomalies(history, current, threshold=0.15)
    text = format_anomaly(report)
    assert "[ANOMALY]" in text
    assert "pipe" in text
