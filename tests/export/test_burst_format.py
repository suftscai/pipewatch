"""Additional formatting tests for burst report."""
from pipewatch.export.burst import BurstWindow, BurstReport, format_burst


def test_format_burst_shows_header():
    report = BurstReport(bursts=[
        BurstWindow(pipeline="etl", hour="2024-01-01T12", error_count=5,
                    total_events=8, rate=0.625)
    ])
    text = format_burst(report)
    assert "Error Bursts Detected" in text


def test_format_burst_shows_rate_percentage():
    report = BurstReport(bursts=[
        BurstWindow(pipeline="etl", hour="2024-01-01T12", error_count=5,
                    total_events=8, rate=0.625)
    ])
    text = format_burst(report)
    assert "62.5%" in text


def test_format_burst_shows_all_entries():
    report = BurstReport(bursts=[
        BurstWindow(pipeline="a", hour="2024-01-01T10", error_count=4,
                    total_events=6, rate=0.667),
        BurstWindow(pipeline="b", hour="2024-01-01T11", error_count=3,
                    total_events=5, rate=0.6),
    ])
    text = format_burst(report)
    assert "a" in text
    assert "b" in text


def test_format_burst_sorted_by_rate():
    report = BurstReport(bursts=[
        BurstWindow(pipeline="low", hour="h", error_count=3, total_events=5, rate=0.6),
        BurstWindow(pipeline="high", hour="h", error_count=9, total_events=10, rate=0.9),
    ])
    # Already sorted by caller; format preserves order
    text = format_burst(report)
    assert text.index("low") > text.index("high")
