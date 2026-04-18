"""Export pipeline summaries and alerts to JSON or CSV reports."""
from __future__ import annotations

import csv
import json
import io
from dataclasses import asdict
from typing import List

from pipewatch.analysis.aggregator import PipelineSummary
from pipewatch.analysis.alert import Alert


def summary_to_dict(summary: PipelineSummary) -> dict:
    return {
        "total": summary.total,
        "errors": summary.errors,
        "warnings": summary.warnings,
        "failure_rate": round(summary.failure_rate, 4),
        "top_failing": summary.top_failing,
    }


def to_json(summary: PipelineSummary, alerts: List[Alert], indent: int = 2) -> str:
    payload = {
        "summary": summary_to_dict(summary),
        "alerts": [
            {"rule": a.rule, "message": a.message} for a in alerts
        ],
    }
    return json.dumps(payload, indent=indent)


def to_csv(summary: PipelineSummary, alerts: List[Alert]) -> str:
    buf = io.StringIO()

    writer = csv.writer(buf)
    writer.writerow(["section", "key", "value"])

    d = summary_to_dict(summary)
    for key, value in d.items():
        if key == "top_failing":
            for stage, count in value.items():
                writer.writerow(["top_failing", stage, count])
        else:
            writer.writerow(["summary", key, value])

    for alert in alerts:
        writer.writerow(["alert", alert.rule, alert.message])

    return buf.getvalue()


def write_report(path: str, summary: PipelineSummary, alerts: List[Alert], fmt: str = "json") -> None:
    if fmt == "json":
        content = to_json(summary, alerts)
    elif fmt == "csv":
        content = to_csv(summary, alerts)
    else:
        raise ValueError(f"Unsupported format: {fmt!r}. Use 'json' or 'csv'.")

    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
