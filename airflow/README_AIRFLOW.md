# Airflow Orchestration Layer — pipeline-observe

This directory is a complete Astronomer Airflow project that *consumes* the `pipeline-observe` library. It serves two purposes simultaneously:

1. **Showcase** — `ratings_etl_with_observe` is the canonical example of `@observe` integrated into a scheduled pipeline.
2. **Regression suite** — `observe_regression_suite` runs every check type against fixture DataFrames each night, failing the DAG on unexpected results. The library tests itself in production.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                  Astronomer Airflow (port 8080)                  │
│                                                                  │
│  ┌────────────────────────┐  ┌────────────────────────────────┐  │
│  │ ratings_etl_with_      │  │ observe_regression_suite      │  │
│  │ observe  (3am daily)  │  │ (midnight daily)               │  │
│  │                        │  │                                │  │
│  │ validate_files         │  │ generate_fixtures              │  │
│  │   → ingest             │  │   ├── test_row_count           │  │
│  │   → clean              │  │   ├── test_null_rate           │  │
│  │   → aggregate          │  │   ├── test_schema              │  │
│  │   → log_summary        │  │   ├── test_freshness           │  │
│  │   → notify             │  │   ├── test_distribution        │  │
│  │                        │  │   ├── test_uniqueness          │  │
│  │ @observe sinks:        │  │   ├── test_range               │  │
│  │   LogSink              │  │   └── test_anomaly             │  │
│  │   SlackSink (failure)  │  │       → evaluate_results       │  │
│  │   DuckDBMetricsSink    │  │                                │  │
│  └───────────┬────────────┘  └────────────────────────────────┘  │
│              │                                                   │
│              ▼                                                   │
│      ┌───────────────┐         ┌────────────────────────────┐    │
│      │ DuckDB:       │ ◀────── │ observe_weekly_digest     │    │
│      │ observe_     │         │ (Mon 9am)                  │    │
│      │ reports       │         │                            │    │
│      └───────────────┘         │ load → trends → Slack      │    │
│                                └────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

## Components

| Path | Purpose |
|---|---|
| `Dockerfile` | Astro Runtime 3.1-5 (Airflow 3.1.x) + observe library installed from baked-in source (`pip install /usr/local/airflow/observe_lib/`, non-editable) |
| `requirements.txt` | Airflow providers: slack, duckdb, pandas, pyarrow |
| `docker-compose.yml` | Postgres + scheduler + webserver + triggerer + airflow-init |
| `dags/ratings_etl_pipeline.py` | Showcase ETL DAG (3am) |
| `dags/observe_regression_suite.py` | Library self-test DAG (midnight) |
| `dags/observe_report_digest.py` | Weekly digest DAG (Mon 9am) |
| `plugins/observe_airflow_hook.py` | `ObserveAirflowHook` + `DuckDBMetricsSink` |
| `include/etl_transforms.py` | `@observe`-decorated ETL functions |
| `include/fixture_datasets.py` | Fixture builders for the regression suite |
| `include/connections.md` | Required Airflow connections |
| `tests/test_dags.py` | DagBag integrity + schedule + structure |
| `tests/test_observe_airflow_hook.py` | DuckDB sink unit tests |
| `data/` | Local volume mount: place `ratings_sample.csv` here |

## Local quickstart

```bash
# Option A: generate a synthetic 12k-row MovieLens-shape CSV (needs `make install` first)
make seed-data

# Option B: drop your own MovieLens-style ratings.csv at airflow/data/ratings_sample.csv
# Columns expected: userId, movieId, rating, timestamp (epoch seconds)

cd airflow
docker compose up -d
# Airflow UI: http://localhost:8080 (admin/admin)

# Trigger DAGs from the UI, or:
docker compose exec scheduler airflow dags trigger ratings_etl_with_observe
docker compose exec scheduler airflow dags trigger observe_regression_suite

# Tear down
docker compose down -v
```

## Design notes

- **No code duplication.** DAGs import `observe.checks` directly. There is no "copy of the check logic" — the library *is* the implementation.
- **Sinks resolved at task time.** The `@observe` decorator's sinks are baked in at function definition, but DAG tasks call `_attach_sinks(fn)` to swap them for the production set from `ObserveAirflowHook` before invoking.
- **DuckDB as audit log.** Every check result accumulates as a row in `observe_reports.duckdb`. The weekly digest queries 7 days of that table; the report-summary task queries 1 day. New DAGs can query the same table.
- **`only_on_failure` Slack noise control.** `SlackSink` is configured to fire only on FAIL/ERROR — the digest DAG handles routine pass-rate reporting.
- **Regression DAG fails the test on unexpected check status.** That's the point — when the library regresses, the DAG goes red overnight before any downstream consumer notices the broken check.

## Multi-container Airflow 3 configuration

This stack splits the api-server, scheduler, triggerer, and dag-processor into separate containers. Airflow 3 changed how task subprocesses and inter-component requests talk to the rest of the system, and three env vars in `x-airflow-env` are load-bearing for that — without them, tasks fail before logging a single line or log-fetch URLs come back unsigned.

| Env var | Purpose |
|---|---|
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | URL the Task SDK uses to call the Task Execution API from inside task subprocesses. Must point at the `webserver` service hostname, not `localhost`, because tasks run in the `scheduler` container. Default is `http://localhost:8080/execution/`, which is wrong for any non-single-container deploy. |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Shared HMAC secret used to sign and verify the JWT the Task SDK presents to the Execution API. If unset, each container auto-generates its own random value at startup, so tokens scheduler signs cannot be verified by webserver. Must be a fixed value shared by every airflow container. |
| `AIRFLOW__API__SECRET_KEY` | Flask/Werkzeug secret used to sign log-fetch URLs and other internal handoffs between airflow components (in Airflow 2 this lived under `[webserver] secret_key` — the warning text still says "webserver section" for historical reasons). Same rule: must be a fixed shared value, not auto-generated per container. |

### Diagnostic signatures

If you see these symptoms, the cause is one of the three env vars above:

| Symptom | Likely cause |
|---|---|
| All DAGs fail at their first task; task log files exist but are 0 bytes; scheduler logs show `httpx.ConnectError: [Errno 111] Connection refused` with a traceback through `airflow/sdk/api/client.py` `task_instances.start`. | `EXECUTION_API_SERVER_URL` missing or pointing at `localhost`. |
| Tasks fail with empty logs; scheduler logs show `airflow.sdk.api.client.ServerResponseError: Invalid auth token: Signature verification failed`. | `JWT_SECRET` not set (each container generated a different secret). |
| Logs show `Please make sure that all your Airflow components ... have the same 'secret_key' configured`; UI cannot fetch task logs or cross-component requests fail. | `API__SECRET_KEY` not set (each container generated a different secret). |

After changing any of these vars, run `docker compose up -d` to recreate the containers, then clear the previously-failed task instances so they re-queue.

## Installing the `observe` library

The `Dockerfile` COPYs the `observe/` source (plus `pyproject.toml` and `README.md`) into `/usr/local/airflow/observe_lib/` and installs it with a **non-editable** `pip install /usr/local/airflow/observe_lib/`. The source is baked into the image at build time — `observe_lib/` is not a volume mount — so editable mode (`pip install -e`) buys nothing and its PEP 660 finder did not resolve `observe` at runtime under Astro Runtime, producing `ModuleNotFoundError: No module named 'observe'` during DAG parsing. The non-editable install matches the wheel-based install used by the sibling projects (2/3/4). After editing anything under `observe/`, rebuild the image (`docker compose build` / `astro dev restart`) to pick up the change.
