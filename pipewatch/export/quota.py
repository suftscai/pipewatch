"""Pipeline error quota tracking — flag pipelines that exceed allowed error counts."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipewatch.export.history import HistoryEntry


@dataclass
class QuotaResult:
    pipeline: str
    total_errors: int
    quota: int
    exceeded: bool
    overage: int  # errors above quota (0 if compliant)


@dataclass
class QuotaReport:
    results: List[QuotaResult] = field(default_factory=list)

    @property
    def violations(self) -> List[QuotaResult]:
        return [r for r in self.results if r.exceeded]

    @property
    def compliant(self) -> bool:
        return len(self.violations) == 0


def compute_quota(
    history: List[HistoryEntry],
    quota: int = 100,
    window: int = 24,
) -> QuotaReport:
    """Count errors per pipeline over the last *window* entries and compare to *quota*."""
    if not history:
        return QuotaReport()

    recent = history[-window:]
    totals: Dict[str, int] = {}
    for entry in recent:
        for pipeline, counts in entry.error_counts.items():
            totals[pipeline] = totals.get(pipeline, 0) + counts

    results = []
    for pipeline, total in sorted(totals.items()):
        exceeded = total > quota
        results.append(
            QuotaResult(
                pipeline=pipeline,
                total_errors=total,
                quota=quota,
                exceeded=exceeded,
                overage=max(0, total - quota),
            )
        )

    return QuotaReport(results=results)


def format_quota(report: QuotaReport) -> str:
    lines = ["=== Error Quota Report ==="]
    if not report.results:
        lines.append("  No data.")
        return "\n".join(lines)

    status = "OK" if report.compliant else "VIOLATIONS DETECTED"
    lines.append(f"  Status : {status}")
    lines.append("")
    lines.append(f"  {'Pipeline':<30} {'Errors':>8} {'Quota':>8} {'Overage':>8}")
    lines.append("  " + "-" * 58)
    for r in report.results:
        flag = " !!" if r.exceeded else ""
        lines.append(
            f"  {r.pipeline:<30} {r.total_errors:>8} {r.quota:>8} {r.overage:>8}{flag}"
        )
    return "\n".join(lines)
