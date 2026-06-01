"""Example: pipeline-observe inside an Airflow DAG via PythonOperator.

This module is illustrative — paste it into an Airflow project's `dags/`
folder (or import the decorated functions). The library is non-invasive:
all `@observe` does is wrap your existing callable.

Wire SlackSink via Airflow Connections:
    Connection ID: slack_webhook
    Type: HTTP
    Password: <your webhook URL>
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

# These imports work in any environment with airflow installed.
# They are deferred to runtime so this file remains importable without airflow.

from sentinel import NullRateCheck, RowCountCheck, SchemaCheck, observe
from sentinel.sinks import LogSink


SCHEMA = {
    "user_id": "object",
    "movie_id": "object",
    "rating": "float64",
    "rated_at": "datetime64[ns]",
}


@observe(
    pipeline_name="daily_ratings_etl",
    table_name="fact_viewership",
    checks=[
        RowCountCheck(min=10_000),
        NullRateCheck("user_id", threshold=0.001),
        SchemaCheck(expected=SCHEMA),
    ],
    sinks=[LogSink()],
    on_failure="raise",
)
def run_etl(**context) -> pd.DataFrame:
    """Airflow PythonOperator callable. Reads source, returns clean DataFrame."""
    source_path = context["params"]["source_path"]
    df = pd.read_csv(source_path)
    df = df.astype({"user_id": object, "movie_id": object, "rated_at": "datetime64[ns]"})
    return df


def _build_dag():
    """Lazy DAG factory so this file imports cleanly outside Airflow."""
    from airflow import DAG  # type: ignore
    from airflow.operators.python import PythonOperator  # type: ignore

    default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}
    with DAG(
        dag_id="ratings_etl_with_sentinel",
        start_date=datetime(2024, 1, 1),
        schedule="0 3 * * *",
        catchup=False,
        default_args=default_args,
        params={"source_path": "/data/ratings.csv"},
        tags=["sentinel", "etl"],
    ) as dag:
        PythonOperator(
            task_id="transform_ratings",
            python_callable=run_etl,
        )
    return dag


# Airflow DAG factory style: define `dag` only when imported by Airflow.
try:
    dag = _build_dag()
except Exception:  # pragma: no cover
    dag = None
