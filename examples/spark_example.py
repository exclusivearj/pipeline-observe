"""Example: applying @observe to a PySpark transform.

Run with: `python examples/spark_example.py`
Requires: `pip install pipeline-sentinel[spark]`
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sentinel import (
    NullRateCheck,
    RangeCheck,
    RowCountCheck,
    UniquenessCheck,
    observe,
)
from sentinel.sinks import LogSink


def main() -> None:
    try:
        from pyspark.sql import SparkSession  # type: ignore
    except ImportError:
        print(
            "PySpark not installed. Run: pip install 'pipeline-sentinel[spark]'"
        )
        return

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    spark = SparkSession.builder.master("local[*]").appName("sentinel-spark-example").getOrCreate()

    now = datetime.utcnow().replace(microsecond=0)
    rows = [
        (f"u_{i:05d}", f"m_{i % 100:03d}", float((i % 9) * 0.5 + 0.5), now - timedelta(minutes=i))
        for i in range(2_000)
    ]
    df = spark.createDataFrame(rows, ["user_id", "movie_id", "rating", "rated_at"])

    @observe(
        pipeline_name="ratings_etl_spark",
        table_name="clean_ratings",
        checks=[
            RowCountCheck(min=100),
            NullRateCheck("user_id", threshold=0.0),
            RangeCheck("rating", min_val=0.5, max_val=5.0),
            UniquenessCheck("movie_id", threshold=1.0),
        ],
        sinks=[LogSink()],
        on_failure="warn",
    )
    def transform(df):
        return df.filter("rating >= 0.5")

    out = transform(df)
    print(f"Output count: {out.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
