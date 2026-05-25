"""DAG: sentinel_regression_suite — daily midnight regression test.

Runs every check type against a known-clean fixture (expect PASS) and a
known-bad fixture (expect FAIL or SKIP per design). Any unexpected
outcome fails the DAG.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/usr/local/airflow/include")
sys.path.insert(0, "/usr/local/airflow/plugins")

from fixture_datasets import (  # noqa: E402
    baseline_history,
    make_clean_df,
    make_distribution_shift_df,
    make_null_heavy_df,
    make_out_of_range_df,
    make_schema_drift_df,
    make_small_df,
    make_stale_df,
)
from sentinel.checks import (  # noqa: E402
    AnomalyCheck,
    DistributionCheck,
    FreshnessCheck,
    NullRateCheck,
    RangeCheck,
    RowCountCheck,
    SchemaCheck,
    UniquenessCheck,
)
from sentinel.report import CheckStatus  # noqa: E402

EXPECTED_SCHEMA = {
    "user_id": "object",
    "movie_id": "object",
    "rating": "float64",
    "rated_at": "datetime64[ns]",
}


def _assert(check, df, expected_status: CheckStatus, label: str) -> None:
    result = check._safe_evaluate(df)
    if result.status != expected_status:
        raise AirflowException(
            f"REGRESSION [{check.__class__.__name__}/{label}]: "
            f"expected {expected_status.value}, got {result.status.value} "
            f"({result.message})"
        )


@dag(
    dag_id="sentinel_regression_suite",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
    default_args={"retries": 0},
    tags=["project3", "sentinel", "testing"],
    description="Nightly regression tests for pipeline-sentinel.",
)
def sentinel_regression_suite():
    @task
    def generate_test_fixtures() -> dict:
        # generate once to avoid drift between sibling tasks (timestamps).
        return {"_": "fixtures generated"}

    @task
    def test_row_count_check(_) -> dict:
        clean = make_clean_df()
        small = make_small_df()
        _assert(RowCountCheck(min=1_000), clean, CheckStatus.PASS, "clean")
        _assert(RowCountCheck(min=1_000), small, CheckStatus.FAIL, "small")
        return {"check": "RowCountCheck", "passed": True}

    @task
    def test_null_rate_check(_) -> dict:
        _assert(NullRateCheck("user_id", threshold=0.001), make_clean_df(), CheckStatus.PASS, "clean")
        _assert(
            NullRateCheck("user_id", threshold=0.01),
            make_null_heavy_df(),
            CheckStatus.FAIL,
            "null-heavy",
        )
        return {"check": "NullRateCheck", "passed": True}

    @task
    def test_schema_check(_) -> dict:
        _assert(SchemaCheck(EXPECTED_SCHEMA), make_clean_df(), CheckStatus.PASS, "clean")
        _assert(SchemaCheck(EXPECTED_SCHEMA), make_schema_drift_df(), CheckStatus.FAIL, "drift")
        return {"check": "SchemaCheck", "passed": True}

    @task
    def test_freshness_check(_) -> dict:
        _assert(
            FreshnessCheck("rated_at", max_lag_hours=24),
            make_clean_df(),
            CheckStatus.PASS,
            "clean",
        )
        _assert(
            FreshnessCheck("rated_at", max_lag_hours=24),
            make_stale_df(lag_hours=30),
            CheckStatus.FAIL,
            "stale",
        )
        return {"check": "FreshnessCheck", "passed": True}

    @task
    def test_distribution_check(_) -> dict:
        _assert(
            DistributionCheck("rating", baseline_mean=2.75, baseline_stddev=1.2, z_score_threshold=3.0),
            make_clean_df(),
            CheckStatus.PASS,
            "clean",
        )
        _assert(
            DistributionCheck("rating", baseline_mean=3.5, baseline_stddev=0.5, z_score_threshold=3.0),
            make_distribution_shift_df(),
            CheckStatus.FAIL,
            "shifted",
        )
        return {"check": "DistributionCheck", "passed": True}

    @task
    def test_uniqueness_check(_) -> dict:
        df = make_clean_df()
        df["row_id"] = range(len(df))
        _assert(UniquenessCheck("row_id"), df, CheckStatus.PASS, "unique")
        df_dup = df.copy()
        df_dup["row_id"] = 1
        _assert(UniquenessCheck("row_id"), df_dup, CheckStatus.FAIL, "duplicates")
        return {"check": "UniquenessCheck", "passed": True}

    @task
    def test_range_check(_) -> dict:
        _assert(
            RangeCheck("rating", min_val=0.5, max_val=5.0),
            make_clean_df(),
            CheckStatus.PASS,
            "clean",
        )
        _assert(
            RangeCheck("rating", min_val=0.5, max_val=5.0),
            make_out_of_range_df(),
            CheckStatus.FAIL,
            "bad",
        )
        return {"check": "RangeCheck", "passed": True}

    @task
    def test_anomaly_check(_) -> dict:
        history = baseline_history()
        _assert(
            AnomalyCheck(baseline=history, stddev_threshold=3.0),
            make_clean_df(n=10_000),
            CheckStatus.PASS,
            "clean",
        )
        _assert(
            AnomalyCheck(baseline=history, stddev_threshold=3.0),
            make_small_df(),
            CheckStatus.FAIL,
            "drop",
        )
        return {"check": "AnomalyCheck", "passed": True}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def evaluate_regression_results(*results) -> None:
        failed = [r for r in results if not (isinstance(r, dict) and r.get("passed"))]
        if failed:
            raise AirflowException(f"Sentinel regression failures: {failed}")
        print(f"All {len(results)}/{len(results)} sentinel checks passed.")

    fixtures = generate_test_fixtures()
    results = [
        test_row_count_check(fixtures),
        test_null_rate_check(fixtures),
        test_schema_check(fixtures),
        test_freshness_check(fixtures),
        test_distribution_check(fixtures),
        test_uniqueness_check(fixtures),
        test_range_check(fixtures),
        test_anomaly_check(fixtures),
    ]
    evaluate_regression_results(*results)


dag = sentinel_regression_suite()
