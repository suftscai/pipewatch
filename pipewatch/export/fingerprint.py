"""Failure fingerprinting: group recurring error messages into canonical patterns."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List
import re

from pipewatch.export.history import HistoryEntry


# Tokens that vary per-run and should be normalised away
_VARIABLE_PATTERNS = [
    (re.compile(r'\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b'), '<TIMESTAMP>'),
    (re.compile(r'\b(?:[0-9a-fA-F]{8}-){3}[0-9a-fA-F]{12}\b'), '<UUID>'),
    (re.compile(r'\b\d+\.\d+\.\d+\.\d+\b'), '<IP>'),
    (re.compile(r'\b\d{5,}\b'), '<NUM>'),
    (re.compile(r"'[^']*'"), "'<VAL>'"),
    (re.compile(r'"[^"]*"'), '"<VAL>"'),
]


def _fingerprint(message: str) -> str:
    """Return a normalised fingerprint for an error message."""
    fp = message
    for pattern, replacement in _VARIABLE_PATTERNS:
        fp = pattern.sub(replacement, fp)
    # Collapse repeated whitespace
    fp = re.sub(r'\s+', ' ', fp).strip()
    return fp


@dataclass
class FingerprintEntry:
    fingerprint: str
    count: int
    pipelines: List[str] = field(default_factory=list)
    examples: List[str] = field(default_factory=list)


@dataclass
class FingerprintReport:
    entries: List[FingerprintEntry] = field(default_factory=list)

    @property
    def total_groups(self) -> int:
        return len(self.entries)


def compute_fingerprint(
    history: List[HistoryEntry],
    window: int = 50,
    max_examples: int = 3,
) -> FingerprintReport:
    """Group error messages from recent history into fingerprint clusters."""
    recent = history[-window:] if len(history) > window else history

    groups: dict[str, FingerprintEntry] = {}

    for entry in recent:
        for msg in entry.top_errors:
            fp = _fingerprint(msg)
            if fp not in groups:
                groups[fp] = FingerprintEntry(fingerprint=fp, count=0)
            groups[fp].count += 1
            if entry.pipeline not in groups[fp].pipelines:
                groups[fp].pipelines.append(entry.pipeline)
            if len(groups[fp].examples) < max_examples and msg not in groups[fp].examples:
                groups[fp].examples.append(msg)

    sorted_entries = sorted(groups.values(), key=lambda e: e.count, reverse=True)
    return FingerprintReport(entries=sorted_entries)


def format_fingerprint(report: FingerprintReport) -> str:
    """Return a human-readable summary of fingerprinted failure patterns."""
    lines = ["=== Failure Fingerprints ==="]
    if not report.entries:
        lines.append("  No error patterns found.")
        return "\n".join(lines)

    for i, entry in enumerate(report.entries, 1):
        pipelines = ", ".join(entry.pipelines) if entry.pipelines else "unknown"
        lines.append(f"  [{i}] count={entry.count}  pipelines={pipelines}")
        lines.append(f"      pattern : {entry.fingerprint}")
        for ex in entry.examples:
            lines.append(f"      example : {ex}")
    return "\n".join(lines)
