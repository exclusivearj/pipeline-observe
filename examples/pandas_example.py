"""Runnable example: applying @observe to a pandas transform.

Run with: `python examples/pandas_example.py`
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from observe import (
    DistributionCheck,
    FreshnessCheck,
    NullRateCheck,
    RowCountCheck,
    SchemaCheck,
    observe,
)
from observe.sinks import LogSink


EXPECTED_SCHEMA = {
    "user_id": "object",
    "movie_id": "object",
    "rating": "float64",
    "rated_at": "datetime64[ns]",
}


def make_input_df(n: int = 5_000) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    now = datetime.utcnow().replace(microsecond=0)
    return pd.DataFrame(
        {
            "user_id": [f"u_{i:05d}" for i in rng.integers(0, 1000, n)],
            "movie_id": [f"m_{i:04d}" for i in rng.integers(0, 500, n)],
            "rating": rng.choice(np.arange(0.5, 5.5, 0.5), size=n).astype(float),
            "rated_at": [now - timedelta(minutes=int(m)) for m in rng.integers(1, 60, n)],
        }
    ).astype({"user_id": object, "movie_id": object, "rated_at": "datetime64[ns]"})


@observe(
    pipeline_name="ratings_etl",
    table_name="clean_ratings",
    checks=[
        RowCountCheck(min=1_000),
        NullRateCheck("user_id", threshold=0.0),
        NullRateCheck("rating", threshold=0.0),
        SchemaCheck(expected=EXPECTED_SCHEMA),
        FreshnessCheck("rated_at", max_lag_hours=4),
        DistributionCheck("rating", baseline_mean=2.75, z_score_threshold=3.0),
    ],
    sinks=[LogSink(level="INFO")],
    on_failure="warn",
)
def transform_ratings(df: pd.DataFrame) -> pd.DataFrame:
    """Identity transform that benefits from observe monitoring."""
    return df


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    df = make_input_df()
    out = transform_ratings(df)
    print(f"\nOutput rows: {len(out)}")
    print(out.head())


if __name__ == "__main__":
    main()
