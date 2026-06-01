"""ETL transform functions, each decorated with @observe.

These are the actual callables used as Airflow PythonOperator targets.
This file is the primary showcase of pipeline-observe in real usage.

Note: the @observe decorator's `sinks` argument is set at *function definition*
time, before Airflow runs. That's fine here because we resolve sinks lazily
via `SentinelAirflowHook` inside the DAG task wrappers — see
`dags/ratings_etl_pipeline.py` for the pattern.
"""

from __future__ import annotations

import pandas as pd

from sentinel import observe
from sentinel.checks import (
    DistributionCheck,
    FreshnessCheck,
    NullRateCheck,
    RangeCheck,
    RowCountCheck,
    SchemaCheck,
    UniquenessCheck,
)
from sentinel.sinks import LogSink


RAW_RATINGS_SCHEMA = {
    "user_id": "object",
    "movie_id": "object",
    "rating": "float64",
    "rated_at": "datetime64[ns]",
}

CLEAN_RATINGS_SCHEMA = {
    "user_id": "object",
    "movie_id": "object",
    "rating_value": "float64",
    "is_positive": "bool",
    "rated_at": "datetime64[ns]",
    "rating_year": "int64",
}


@observe(
    pipeline_name="ratings_etl",
    table_name="raw_ratings_ingested",
    checks=[
        RowCountCheck(min=10_000),
        NullRateCheck("user_id", threshold=0.001),
        NullRateCheck("rating", threshold=0.0),
        SchemaCheck(expected=RAW_RATINGS_SCHEMA),
        FreshnessCheck("rated_at", max_lag_hours=48),
    ],
    sinks=[LogSink()],
    on_failure="warn",
)
def ingest_raw_ratings(source_path: str) -> pd.DataFrame:
    """Read MovieLens-style ratings CSV; cast types; return canonical DataFrame."""
    df = pd.read_csv(source_path)
    rename = {"userId": "user_id", "movieId": "movie_id", "timestamp": "rated_at"}
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    df["user_id"] = df["user_id"].astype(str)
    df["movie_id"] = df["movie_id"].astype(str)
    df["rating"] = df["rating"].astype(float)
    if pd.api.types.is_numeric_dtype(df["rated_at"]):
        df["rated_at"] = pd.to_datetime(df["rated_at"], unit="s")
    else:
        df["rated_at"] = pd.to_datetime(df["rated_at"])
    df["rated_at"] = df["rated_at"].astype("datetime64[ns]")
    df["user_id"] = df["user_id"].astype(object)
    df["movie_id"] = df["movie_id"].astype(object)
    return df[["user_id", "movie_id", "rating", "rated_at"]]


@observe(
    pipeline_name="ratings_etl",
    table_name="clean_ratings",
    checks=[
        RowCountCheck(min=10_000),
        NullRateCheck("rating_value", threshold=0.0),
        SchemaCheck(expected=CLEAN_RATINGS_SCHEMA),
        RangeCheck("rating_value", min_val=0.5, max_val=5.0),
        UniquenessCheck("user_id", threshold=1.0),
        DistributionCheck("rating_value", baseline_mean=3.5, z_score_threshold=3.0),
    ],
    sinks=[LogSink()],
    on_failure="raise",
)
def clean_and_enrich_ratings(df: pd.DataFrame) -> pd.DataFrame:
    """Drop bad rows, add derived columns."""
    df = df.rename(columns={"rating": "rating_value"})
    df = df[df["rating_value"].between(0.5, 5.0)].copy()
    df["is_positive"] = df["rating_value"] >= 3.5
    df["rating_year"] = df["rated_at"].dt.year.astype("int64")
    return df[["user_id", "movie_id", "rating_value", "is_positive", "rated_at", "rating_year"]]


@observe(
    pipeline_name="ratings_etl",
    table_name="ratings_aggregated",
    checks=[
        RowCountCheck(min=100),
        NullRateCheck("avg_rating", threshold=0.0),
        RangeCheck("avg_rating", min_val=0.5, max_val=5.0),
        UniquenessCheck("movie_id", threshold=0.0),
    ],
    sinks=[LogSink()],
    on_failure="raise",
)
def aggregate_by_movie(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate ratings by movie."""
    return (
        df.groupby("movie_id")
        .agg(
            avg_rating=("rating_value", "mean"),
            rating_count=("rating_value", "count"),
            pct_positive=("is_positive", "mean"),
        )
        .reset_index()
    )
