"""Export package: reporting, scheduling, history, trends, comparison, digest, baseline."""
from pipewatch.export.reporter import summary_to_dict, to_json, to_csv, write_report
from pipewatch.export.scheduler import ReportScheduler
from pipewatch.export.history import (
    HistoryEntry,
    append_entry,
    load_history,
    recent_failure_trend,
)
from pipewatch.export.trend import TrendReport, compute_trend, format_trend
from pipewatch.export.comparison import (
    PipelineComparison,
    ComparisonReport,
    compare_entries,
    format_comparison,
)
from pipewatch.export.digest import DigestReport, compute_digest, format_digest
from pipewatch.export.baseline import (
    BaselineReport,
    save_baseline,
    load_baseline,
    compare_to_baseline,
    format_baseline,
)

__all__ = [
    "summary_to_dict", "to_json", "to_csv", "write_report",
    "ReportScheduler",
    "HistoryEntry", "append_entry", "load_history", "recent_failure_trend",
    "TrendReport", "compute_trend", "format_trend",
    "PipelineComparison", "ComparisonReport", "compare_entries", "format_comparison",
    "DigestReport", "compute_digest", "format_digest",
    "BaselineReport", "save_baseline", "load_baseline", "compare_to_baseline", "format_baseline",
]
