"""DAG: ratings_etl_with_sentinel — daily 3am full ETL using @observe.

Primary showcase of pipeline-sentinel integrated into Airflow.

Each task is a PythonOperator-style @task that:
  1. resolves the standard set of sinks from SentinelAirflowHook
  2. invokes the @observe-decorated transform from include.etl_transforms
  3. injects the sinks at call time by rebuilding the function's
     `__sentinel_sinks__` attribute (so DAGs override defaults centrally)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

# Make plugins/include importable when running locally.
sys.path.insert(0, "/usr/local/airflow/plugins")
sys.path.insert(0, "/usr/local/airflow/include")

from etl_transforms import (  # noqa: E402
    aggregate_by_movie,
    clean_and_enrich_ratings,
    ingest_raw_ratings,
)
from sentinel_airflow_hook import SentinelAirflowHook  # noqa: E402

DATA_DIR = Path(os.environ.get("SENTINEL_DATA_DIR", "/usr/local/airflow/data"))
RATINGS_CSV = DATA_DIR / "ratings_sample.csv"


def _attach_sinks(fn):
    """Replace the function's sinks with the standard production set."""
    hook = SentinelAirflowHook()
    fn.__sentinel_sinks__ = hook.get_default_sinks(include_slack=True)
    return fn


@dag(
    dag_id="ratings_etl_with_sentinel",
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["project3", "sentinel", "etl"],
    description="Full ratings ETL pipeline using pipeline-sentinel @observe decorator.",
)
def ratings_etl_with_sentinel():
    @task
    def validate_data_files_exist() -> str:
        if not RATINGS_CSV.exists():
            raise AirflowException(
                f"Required CSV missing: {RATINGS_CSV}. "
                "Drop a MovieLens-style ratings.csv into airflow/data/ratings_sample.csv."
            )
        size_mb = RATINGS_CSV.stat().st_size / 1_000_000
        print(f"{RATINGS_CSV} exists ({size_mb:.1f} MB)")
        return str(RATINGS_CSV)

    @task
    def ingest(source_path: str):
        _attach_sinks(ingest_raw_ratings)
        df = ingest_raw_ratings(source_path)
        out = DATA_DIR / "_raw_ratings.parquet"
        df.to_parquet(out, index=False)
        return str(out)

    @task
    def clean(raw_path: str):
        import pandas as pd

        _attach_sinks(clean_and_enrich_ratings)
        df = pd.read_parquet(raw_path)
        clean_df = clean_and_enrich_ratings(df)
        out = DATA_DIR / "_clean_ratings.parquet"
        clean_df.to_parquet(out, index=False)
        return str(out)

    @task
    def aggregate(clean_path: str):
        import pandas as pd

        _attach_sinks(aggregate_by_movie)
        df = pd.read_parquet(clean_path)
        agg = aggregate_by_movie(df)
        out = DATA_DIR / "ratings_aggregated.parquet"
        agg.to_parquet(out, index=False)
        return str(out)

    @task
    def log_sentinel_report_summary() -> dict:
        import duckdb

        db_path = os.environ.get(
            "SENTINEL_DUCKDB_PATH", "/usr/local/airflow/data/sentinel_reports.duckdb"
        )
        if not os.path.exists(db_path):
            print("No sentinel_reports.duckdb yet; first run.")
            return {"checks_today": 0}
        conn = duckdb.connect(db_path)
        try:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS checks,
                    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS pass_n,
                    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS fail_n,
                    SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) AS warn_n
                FROM sentinel_reports
                WHERE evaluated_at >= now() - INTERVAL 1 DAY
                """
            ).fetchone()
        finally:
            conn.close()
        summary = {
            "checks_today": row[0] or 0,
            "pass": row[1] or 0,
            "fail": row[2] or 0,
            "warn": row[3] or 0,
        }
        print(f"Sentinel summary (last 24h): {summary}")
        return summary

    @task
    def notify_pipeline_complete(summary: dict) -> None:
        msg = (
            f":white_check_mark: ratings_etl_with_sentinel complete — "
            f"{summary.get('pass', 0)} pass, {summary.get('fail', 0)} fail, "
            f"{summary.get('warn', 0)} warn"
        )
        print(msg)

    csv = validate_data_files_exist()
    raw = ingest(csv)
    cleaned = clean(raw)
    agg = aggregate(cleaned)
    summary = log_sentinel_report_summary()
    cleaned >> summary  # ensure summary runs after cleaning even if agg fails
    agg >> summary
    notify_pipeline_complete(summary)


dag = ratings_etl_with_sentinel()
