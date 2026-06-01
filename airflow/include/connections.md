# Airflow Connections

Configure these connections in Airflow UI → Admin → Connections (or via
environment variables `AIRFLOW_CONN_*` for local docker compose).

| Conn ID | Type | Used For | Required Fields |
|---|---|---|---|
| `slack_webhook` | HTTP | Slack alerts from SlackSink | `password` = webhook URL (e.g. `https://hooks.slack.com/services/...`) |
| `observe_metrics_db` | Generic | DuckDB path for storing ObservabilityReports | `host` = absolute filesystem path to `.duckdb` file (default `/usr/local/airflow/data/observe_reports.duckdb`) |

The docker-compose.yml file in this directory wires sane defaults via
`AIRFLOW_CONN_*` env vars; override them by editing that file or
exporting the env vars before `docker compose up`.

Example local-dev override:

```bash
export AIRFLOW_CONN_SLACK_WEBHOOK="http://:HOOK_PATH@hooks.slack.com/services/Txxx/Bxxx/yyy"
docker compose up -d
```
