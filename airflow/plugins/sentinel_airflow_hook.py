"""Bridge between Airflow Connections and pipeline-observe sinks."""

from __future__ import annotations

import os
from typing import Optional

from airflow.hooks.base import BaseHook

from sentinel.sinks import LogSink, SlackSink
from sentinel.sinks.base import BaseSink

from duckdb_metrics_sink import DuckDBMetricsSink


class SentinelAirflowHook(BaseHook):
    """Bridge between Airflow Connections and sentinel sinks.

    Methods return configured sink instances so DAGs don't need to know
    where credentials live.
    """

    conn_name_attr = "sentinel_conn_id"
    default_conn_name = "sentinel_metrics_db"
    conn_type = "sentinel"
    hook_name = "Sentinel"

    def __init__(self, sentinel_conn_id: str = "sentinel_metrics_db") -> None:
        super().__init__()
        self.sentinel_conn_id = sentinel_conn_id

    def get_slack_sink(
        self,
        conn_id: str = "slack_webhook",
        only_on_failure: bool = True,
    ) -> Optional[SlackSink]:
        try:
            conn = self.get_connection(conn_id)
        except Exception:
            self.log.warning("Slack connection %s not configured; skipping SlackSink", conn_id)
            return None
        webhook = conn.password or conn.host or ""
        if not webhook.startswith("http"):
            self.log.warning("Slack webhook URL not set in connection %s; skipping", conn_id)
            return None
        return SlackSink(webhook_url=webhook, only_on_failure=only_on_failure)

    def get_duckdb_sink(
        self,
        db_path: Optional[str] = None,
        table: str = "sentinel_reports",
    ) -> DuckDBMetricsSink:
        path = db_path or os.environ.get(
            "SENTINEL_DUCKDB_PATH", "/usr/local/airflow/data/sentinel_reports.duckdb"
        )
        return DuckDBMetricsSink(db_path=path, table=table)

    def get_default_sinks(
        self,
        include_slack: bool = True,
        slack_conn_id: str = "slack_webhook",
    ) -> list[BaseSink]:
        sinks: list[BaseSink] = [LogSink()]
        if include_slack:
            slack = self.get_slack_sink(conn_id=slack_conn_id)
            if slack:
                sinks.append(slack)
        sinks.append(self.get_duckdb_sink())
        return sinks
