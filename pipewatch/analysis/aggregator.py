"""Aggregate pipeline events into summary statistics."""
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Iterable

from pipewatch.ingestion.log_parser import PipelineEvent


@dataclass
class PipelineSummary:
    total: int = 0
    failures: int = 0
    warnings: int = 0
    by_pipeline: dict = field(default_factory=lambda: defaultdict(lambda: {"total": 0, "failures": 0}))

    @property
    def failure_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.failures / self.total

    def top_failing(self, n: int = 5) -> list[tuple[str, int]]:
        """Return top-n pipelines by failure count."""
        ranked = sorted(
            self.by_pipeline.items(),
            key=lambda kv: kv[1]["failures"],
            reverse=True,
        )
        return [(name, data["failures"]) for name, data in ranked[:n]]


def aggregate(events: Iterable[PipelineEvent]) -> PipelineSummary:
    """Compute a PipelineSummary from an iterable of PipelineEvents."""
    summary = PipelineSummary()
    for event in events:
        summary.total += 1
        summary.by_pipeline[event.pipeline]["total"] += 1
        if event.level == "ERROR":
            summary.failures += 1
            summary.by_pipeline[event.pipeline]["failures"] += 1
        elif event.level == "WARNING":
            summary.warnings += 1
    return summary
