"""Tests for ObserveAirflowHook and DuckDBMetricsSink."""

from __future__ import annotations

import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, "/usr/local/airflow/plugins")
sys.path.insert(0, "airflow/plugins")

from duckdb_metrics_sink import DuckDBMetricsSink  # type: ignore
from observe.report import CheckResult, CheckStatus, ObservabilityReport


def _make_report() -> ObservabilityReport:
    report = ObservabilityReport(
        pipeline_name="pipe", table_name="tbl", row_count=100, duration_ms=12.0
    )
    report.check_results.append(
        CheckResult(
            check_name="RowCountCheck",
            check_params={"min": 10},
            status=CheckStatus.PASS,
            metric_value=100,
            threshold=10,
            message="ok",
            evaluated_at=datetime.now(timezone.utc),
        )
    )
    report.check_results.append(
        CheckResult(
            check_name="NullRateCheck",
            check_params={"column": "user_id"},
            status=CheckStatus.FAIL,
            metric_value=0.5,
            threshold=0.01,
            message="too many",
            column="user_id",
            evaluated_at=datetime.now(timezone.utc),
        )
    )
    return report


def test_duckdb_sink_creates_table(tmp_path):
    db = tmp_path / "x.duckdb"
    sink = DuckDBMetricsSink(db_path=str(db))
    import duckdb

    conn = duckdb.connect(str(db))
    try:
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
    finally:
        conn.close()
    assert "observe_reports" in tables


def test_duckdb_sink_writes_report(tmp_path):
    db = tmp_path / "x.duckdb"
    sink = DuckDBMetricsSink(db_path=str(db))
    sink.write(_make_report())
    import duckdb

    conn = duckdb.connect(str(db))
    try:
        n = conn.execute("SELECT COUNT(*) FROM observe_reports").fetchone()[0]
    finally:
        conn.close()
    assert n == 2  # one row per CheckResult


def test_duckdb_sink_is_idempotent(tmp_path):
    db = tmp_path / "x.duckdb"
    sink = DuckDBMetricsSink(db_path=str(db))
    report = _make_report()
    sink.write(report)
    sink.write(report)  # same run_id → no new rows
    import duckdb

    conn = duckdb.connect(str(db))
    try:
        n = conn.execute("SELECT COUNT(*) FROM observe_reports").fetchone()[0]
    finally:
        conn.close()
    assert n == 2
