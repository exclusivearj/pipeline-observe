"""DuckDBMetricsSink — writes ObservabilityReport rows to a local DuckDB table.

Lives outside sentinel_airflow_hook.py so it can be imported and tested
without Apache Airflow installed locally.
"""

from __future__ import annotations

import json
import os

from sentinel.report import ObservabilityReport
from sentinel.sinks.base import BaseSink


_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    run_id TEXT,
    pipeline_name TEXT,
    table_name TEXT,
    overall_status TEXT,
    check_name TEXT,
    column_name TEXT,
    metric_value TEXT,
    threshold TEXT,
    status TEXT,
    message TEXT,
    evaluated_at TIMESTAMP,
    row_count BIGINT,
    duration_ms DOUBLE,
    PRIMARY KEY (run_id, check_name, column_name)
)
"""


class DuckDBMetricsSink(BaseSink):
    """Writes ObservabilityReport to a local DuckDB table.

    One row per CheckResult, keyed on (run_id, check_name, column_name).
    Re-writes of the same report are idempotent (`INSERT OR IGNORE`).
    """

    def __init__(self, db_path: str, table: str = "sentinel_reports") -> None:
        self.db_path = db_path
        self.table = table
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._ensure_table()

    def _connect(self):
        import duckdb

        return duckdb.connect(self.db_path)

    def _ensure_table(self) -> None:
        conn = self._connect()
        try:
            conn.execute(_TABLE_DDL.format(table=self.table))
        finally:
            conn.close()

    def write(self, report: ObservabilityReport) -> None:
        rows = []
        for r in report.check_results:
            rows.append(
                (
                    report.run_id,
                    report.pipeline_name,
                    report.table_name,
                    report.overall_status.value,
                    r.check_name,
                    r.column or "",
                    json.dumps(r.metric_value, default=str),
                    json.dumps(r.threshold, default=str),
                    r.status.value,
                    r.message,
                    r.evaluated_at,
                    report.row_count,
                    report.duration_ms,
                )
            )
        if not rows:
            return
        conn = self._connect()
        try:
            conn.executemany(
                f"INSERT OR IGNORE INTO {self.table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
        finally:
            conn.close()
