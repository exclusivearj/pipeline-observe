# Check Reference

Quick reference for all 8 check types. Each evaluates a DataFrame and returns a `CheckResult`.

---

## RowCountCheck

Bounds total row count.

| Param | Type | Default | Description |
|---|---|---|---|
| `min` | `int` | `0` | minimum acceptable row count |
| `max` | `int \| float` | `inf` | maximum acceptable row count |

Use when you want to fail fast if an upstream source is empty or wildly larger than expected.

```python
RowCountCheck(min=1_000, max=10_000_000)
```

`metric_value` is the actual integer row count.

---

## NullRateCheck

Fraction of nulls in a column.

| Param | Type | Default | Description |
|---|---|---|---|
| `column` | `str` | required | column to evaluate |
| `threshold` | `float` | `0.01` | max acceptable null rate ∈ [0, 1] |

Use for required columns that should never (or rarely) be null.

```python
NullRateCheck("user_id", threshold=0.001)
```

`metric_value` is the null rate rounded to 4 decimals. SKIPs if the column is absent.

---

## SchemaCheck

Validate column names and dtypes against a contract.

| Param | Type | Default | Description |
|---|---|---|---|
| `expected` | `dict[str, str]` | required | `{column: dtype_string}` |

Use as the first line of defense against silent schema drift. dtype strings use pandas conventions (e.g. `"object"`, `"int64"`, `"float64"`, `"datetime64[ns]"`).

```python
SchemaCheck(expected={"user_id": "object", "rating": "float64"})
```

`metric_value` is the actual schema dict; message lists every mismatch.

---

## FreshnessCheck

Max of a timestamp column must be within `max_lag_hours` of now.

| Param | Type | Default | Description |
|---|---|---|---|
| `timestamp_column` | `str` | required | timestamp column to inspect |
| `max_lag_hours` | `float` | `24.0` | acceptable lag in hours |
| `timezone` | `str` | `"UTC"` | reference TZ |

Use to detect stuck producers — the table exists but nothing is flowing in.

```python
FreshnessCheck("event_ts", max_lag_hours=2)
```

`metric_value` is the actual lag in hours.

---

## DistributionCheck

Z-score of a column's mean vs. a known baseline.

| Param | Type | Default | Description |
|---|---|---|---|
| `column` | `str` | required | numeric column |
| `baseline_mean` | `float` | required | reference mean |
| `z_score_threshold` | `float` | `3.0` | max acceptable |z| |
| `baseline_stddev` | `float \| None` | None | use provided stddev, else compute from the data |

Use for slow-moving metrics where dramatic shifts indicate upstream regressions.

```python
DistributionCheck("rating", baseline_mean=3.5, baseline_stddev=0.5, z_score_threshold=3.0)
```

`metric_value` is `{"actual_mean": ..., "baseline_mean": ..., "z_score": ...}`.

---

## UniquenessCheck

Duplicate rate per column.

| Param | Type | Default | Description |
|---|---|---|---|
| `column` | `str` | required | column to evaluate |
| `threshold` | `float` | `0.0` | max acceptable duplicate rate |

Use to enforce primary-key constraints in dataframes that lack a DB.

```python
UniquenessCheck("event_id", threshold=0.0)
```

`metric_value` is the duplicate rate (`1 - unique/total`).

---

## RangeCheck

All values in `[min_val, max_val]`.

| Param | Type | Default | Description |
|---|---|---|---|
| `column` | `str` | required | column to evaluate |
| `min_val` | `float` | required | inclusive lower bound |
| `max_val` | `float` | required | inclusive upper bound |

Use to catch silent corruption (e.g. a rating column getting an HTTP status code by mistake).

```python
RangeCheck("rating", min_val=0.5, max_val=5.0)
```

`metric_value` is `{"out_of_range_count": ..., "out_of_range_rate": ...}`.

---

## AnomalyCheck

Z-score of a metric (`row_count`, `mean`, or `null_rate`) against a rolling baseline.

| Param | Type | Default | Description |
|---|---|---|---|
| `column` | `str \| None` | None | required when `metric` is `"mean"` or `"null_rate"` |
| `metric` | `str` | `"row_count"` | one of `row_count`, `mean`, `null_rate` |
| `baseline` | `list[float]` | required | historical values (≥3) |
| `stddev_threshold` | `float` | `3.0` | max acceptable |z| |

Use when a single threshold is too rigid — the baseline reacts to seasonality. Pair with a sink (e.g. `DuckDBMetricsSink`) that supplies the rolling history.

```python
AnomalyCheck(metric="row_count", baseline=last_7_day_counts, stddev_threshold=3.0)
```

`metric_value` is `{"current": ..., "rolling_mean": ..., "rolling_stddev": ..., "z_score": ...}`. SKIPs if baseline has < 3 values.
