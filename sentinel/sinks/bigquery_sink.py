"""BigQuerySink: streaming insert of CheckResults into a BigQuery table."""

from __future__ import annotations

import logging
from typing import Optional

from sentinel.report import ObservabilityReport
from sentinel.sinks.base import BaseSink


def _coerce_metric(value: object) -> Optional[float]:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _coerce_threshold(value: object) -> Optional[float]:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


class BigQuerySink(BaseSink):
    def __init__(
        self,
        project: str,
        dataset: str,
        table: str = "sentinel_metrics",
        credentials_path: Optional[str] = None,
    ) -> None:
        self.project = project
        self.dataset = dataset
        self.table = table
        self.credentials_path = credentials_path
        self._logger = logging.getLogger(__name__)

    def _client(self):
        try:
            from google.cloud import bigquery  # type: ignore
        except ImportError:
            self._logger.warning(
                "BigQuerySink: google-cloud-bigquery not installed; skipping write. "
                "Install with `pip install pipeline-sentinel[gcp]`."
            )
            return None
        if self.credentials_path:
            from google.oauth2 import service_account  # type: ignore

            creds = service_account.Credentials.from_service_account_file(self.credentials_path)
            return bigquery.Client(project=self.project, credentials=creds)
        return bigquery.Client(project=self.project)

    def write(self, report: ObservabilityReport) -> None:
        client = self._client()
        if client is None:
            return
        table_ref = f"{self.project}.{self.dataset}.{self.table}"

        rows = []
        for result in report.check_results:
            rows.append(
                {
                    "run_id": report.run_id,
                    "pipeline_name": report.pipeline_name,
                    "table_name": report.table_name,
                    "overall_status": report.overall_status.value,
                    "check_name": result.check_name,
                    "column": result.column,
                    "metric_value": _coerce_metric(result.metric_value),
                    "threshold": _coerce_threshold(result.threshold),
                    "status": result.status.value,
                    "message": result.message,
                    "evaluated_at": result.evaluated_at.isoformat(),
                    "row_count": report.row_count,
                }
            )
        if not rows:
            return
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            self._logger.error("BigQuerySink: insert errors: %s", errors)
