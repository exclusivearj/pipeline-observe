# pipeline-sentinel

> Decorator-based data observability for pandas and PySpark pipelines.

`pipeline-sentinel` adds production-grade data quality checks around your existing transform functions with a single decorator. It supports both pandas and PySpark DataFrames via duck-typing, ships pluggable alert sinks (log, Slack, BigQuery, Prometheus), and includes an Airflow regression suite that tests the library on real fixtures every night.

## Why

Most data pipelines fail silently — bad rows make it downstream and analysts discover the problem days later. `pipeline-sentinel` puts a check in front of every transform, with clear pass/fail/warn signals and structured reports.

## Install

```bash
pip install -e .              # core (pandas only)
pip install -e ".[spark]"     # + PySpark support
pip install -e ".[gcp]"       # + BigQuery sink
pip install -e ".[dev]"       # + test/lint tooling
```

## Usage

```python
import pandas as pd
from sentinel import observe
from sentinel.checks import (
    RowCountCheck, NullRateCheck, SchemaCheck, FreshnessCheck,
    DistributionCheck, UniquenessCheck, RangeCheck,
)
from sentinel.sinks import LogSink, SlackSink

EXPECTED_SCHEMA = {
    "user_id": "object",
    "rating": "float64",
    "event_ts": "datetime64[ns]",
}

@observe(
    pipeline_name="ratings_etl",
    table_name="fact_viewership",
    checks=[
        RowCountCheck(min=100_000),
        NullRateCheck("user_id", threshold=0.001),
        SchemaCheck(expected=EXPECTED_SCHEMA),
        FreshnessCheck("event_ts", max_lag_hours=4),
        RangeCheck("rating", min_val=0.5, max_val=5.0),
        DistributionCheck("rating", baseline_mean=3.5, z_score_threshold=3.0),
    ],
    sinks=[LogSink(), SlackSink(webhook_url=os.environ["SLACK_WEBHOOK"])],
    on_failure="warn",  # or "raise" to block downstream
)
def transform_ratings(df: pd.DataFrame) -> pd.DataFrame:
    # your existing logic, untouched
    return df
```

## Check types

| Check | Purpose |
|---|---|
| `RowCountCheck` | bounds on total row count |
| `NullRateCheck` | null fraction per column ≤ threshold |
| `SchemaCheck` | column names and dtypes match expected |
| `FreshnessCheck` | `max(timestamp_col)` within `max_lag_hours` of now |
| `DistributionCheck` | z-score of column mean vs. baseline |
| `UniquenessCheck` | duplicate rate per column ≤ threshold |
| `RangeCheck` | all values within `[min_val, max_val]` |
| `AnomalyCheck` | metric (row_count/mean/null_rate) vs. rolling baseline |

Full parameter reference: [docs/checks_reference.md](docs/checks_reference.md).

## Sinks

| Sink | Output |
|---|---|
| `LogSink` | Python `logging` (default, always enabled if you pass `sinks=None`) |
| `SlackSink` | Slack webhook, Block Kit message |
| `BigQuerySink` | streaming insert to a BigQuery table |
| `PrometheusSink` | push metrics to a Prometheus Pushgateway |

Custom sinks: subclass `BaseSink` and implement `write(report)`.

## On-failure behavior

- `on_failure="warn"` (default) — log the failure, continue. Use for soft alerting.
- `on_failure="raise"` — raise `DataQualityError` (with the full `ObservabilityReport` attached). Use to gate downstream tasks.

## Layout

```
pipeline-sentinel/
├── sentinel/
│   ├── core.py             ← @observe decorator
│   ├── report.py           ← ObservabilityReport, CheckResult, CheckStatus
│   ├── exceptions.py
│   ├── checks/             ← 8 check classes
│   └── sinks/              ← 4 sinks
├── tests/                  ← 67 tests, 90%+ coverage
├── examples/               ← pandas, Spark, Airflow examples
├── docs/checks_reference.md
└── airflow/                ← Airflow orchestration layer (see airflow/README_AIRFLOW.md)
```

## Development

```bash
make install   # create venv, install editable + dev extras
make test      # pytest + coverage
make lint      # black --check + mypy
make format    # black format
make build     # produce a wheel in dist/
make example   # run examples/pandas_example.py
```

## Airflow integration

A complete Astronomer Airflow setup lives under [airflow/](airflow/). It exposes:

- `ratings_etl_with_sentinel` — daily 3am ETL using `@observe`
- `sentinel_regression_suite` — nightly self-test on every check type
- `sentinel_weekly_digest` — Monday 9am Slack digest of accumulated reports

See [airflow/README_AIRFLOW.md](airflow/README_AIRFLOW.md) for the orchestration architecture.

## License

MIT.
