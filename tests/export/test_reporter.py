"""Tests for pipewatch.export.reporter."""
import json
import csv
import io
import os
import tempfile

import pytest

from pipewatch.analysis.aggregator import PipelineSummary
from pipewatch.analysis.alert import Alert
from pipewatch.export.reporter import to_json, to_csv, write_report


def _summary(**kwargs):
    defaults = dict(total=10, errors=3, warnings=2, failure_rate=0.3, top_failing={"etl.load": 2})
    defaults.update(kwargs)
    return PipelineSummary(**defaults)


def _alerts():
    return [Alert(rule="high_failure_rate", message="Rate 30.0% exceeds 20.0%")]


def test_to_json_structure():
    out = to_json(_summary(), _alerts())
    data = json.loads(out)
    assert "summary" in data
    assert "alerts" in data
    assert data["summary"]["total"] == 10
    assert data["summary"]["errors"] == 3
    assert data["summary"]["failure_rate"] == 0.3


def test_to_json_no_alerts():
    out = to_json(_summary(), [])
    data = json.loads(out)
    assert data["alerts"] == []


def test_to_json_alert_fields():
    out = to_json(_summary(), _alerts())
    data = json.loads(out)
    alert = data["alerts"][0]
    assert alert["rule"] == "high_failure_rate"
    assert "30.0%" in alert["message"]


def test_to_csv_contains_summary_rows():
    out = to_csv(_summary(), [])
    reader = csv.reader(io.StringIO(out))
    rows = list(reader)
    keys = [r[1] for r in rows if r[0] == "summary"]
    assert "total" in keys
    assert "errors" in keys
    assert "failure_rate" in keys


def test_to_csv_top_failing_rows():
    out = to_csv(_summary(), [])
    reader = csv.reader(io.StringIO(out))
    rows = list(reader)
    tf_rows = [r for r in rows if r[0] == "top_failing"]
    assert any(r[1] == "etl.load" for r in tf_rows)


def test_to_csv_alert_rows():
    out = to_csv(_summary(), _alerts())
    reader = csv.reader(io.StringIO(out))
    rows = list(reader)
    alert_rows = [r for r in rows if r[0] == "alert"]
    assert len(alert_rows) == 1
    assert alert_rows[0][1] == "high_failure_rate"


def test_write_report_json(tmp_path):
    path = str(tmp_path / "report.json")
    write_report(path, _summary(), _alerts(), fmt="json")
    with open(path) as f:
        data = json.load(f)
    assert data["summary"]["total"] == 10


def test_write_report_csv(tmp_path):
    path = str(tmp_path / "report.csv")
    write_report(path, _summary(), [], fmt="csv")
    assert os.path.exists(path)
    with open(path) as f:
        content = f.read()
    assert "summary" in content


def test_write_report_invalid_fmt():
    with pytest.raises(ValueError, match="Unsupported format"):
        write_report("/tmp/x", _summary(), [], fmt="xml")
