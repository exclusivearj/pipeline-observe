"""Test DataFrames for the sentinel regression suite DAG.

Each fixture is paired with the expected check outcome.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def make_clean_df(n: int = 10_000, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    now = datetime.utcnow().replace(microsecond=0)
    df = pd.DataFrame(
        {
            "user_id": [f"u_{i:06d}" for i in rng.integers(0, 50_000, n)],
            "movie_id": [f"m_{i:05d}" for i in rng.integers(0, 5_000, n)],
            "rating": rng.choice(np.arange(0.5, 5.5, 0.5), size=n).astype(float),
            "rated_at": [now - timedelta(minutes=int(m)) for m in rng.integers(1, 60, n)],
        }
    )
    df["user_id"] = df["user_id"].astype(object)
    df["movie_id"] = df["movie_id"].astype(object)
    df["rated_at"] = df["rated_at"].astype("datetime64[ns]")
    return df


def make_null_heavy_df(null_col: str = "user_id", null_rate: float = 0.15) -> pd.DataFrame:
    df = make_clean_df()
    null_idx = np.random.default_rng(0).choice(len(df), size=int(len(df) * null_rate), replace=False)
    df.loc[null_idx, null_col] = None
    return df


def make_stale_df(lag_hours: float = 30.0) -> pd.DataFrame:
    df = make_clean_df()
    stale_ts = datetime.utcnow().replace(microsecond=0) - timedelta(hours=lag_hours)
    df["rated_at"] = stale_ts
    df["rated_at"] = df["rated_at"].astype("datetime64[ns]")
    return df


def make_schema_drift_df() -> pd.DataFrame:
    df = make_clean_df()
    df["rating"] = df["rating"].astype(str)  # drift float -> str
    return df


def make_distribution_shift_df() -> pd.DataFrame:
    df = make_clean_df()
    df["rating"] = 0.5  # collapse mean to 0.5 (>> 5 stddev from baseline 3.5)
    return df


def make_out_of_range_df(col: str = "rating", bad_value: float = 999.0) -> pd.DataFrame:
    df = make_clean_df()
    df.loc[df.index[:50], col] = bad_value
    return df


def make_duplicated_id_df() -> pd.DataFrame:
    df = make_clean_df()
    df["row_id"] = 1  # all duplicates
    return df


def make_small_df(n: int = 10) -> pd.DataFrame:
    return make_clean_df(n=n)


def baseline_history() -> list[float]:
    """Return a stable rolling baseline (mean=10_000, low variance) for AnomalyCheck."""
    return [9_950, 10_050, 10_010, 9_990, 10_000, 10_005, 9_995]
