"""DAG: observe_weekly_digest — Monday 9am quality digest.

Reads accumulated ObservabilityReports from DuckDB, computes 7-day
trends, posts a Block Kit message to Slack.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="observe_weekly_digest",
    start_date=datetime(2024, 1, 1),
    schedule="0 9 * * 1",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["project3", "observe", "reporting"],
    description="Weekly data quality digest from DuckDB observe_reports.",
)
def observe_weekly_digest():
    @task
    def load_weekly_reports() -> dict:
        import duckdb

        db_path = os.environ.get(
            "OBSERVE_DUCKDB_PATH", "/usr/local/airflow/data/observe_reports.duckdb"
        )
        if not os.path.exists(db_path):
            return {"empty": True, "reason": "no observe_reports.duckdb yet"}
        conn = duckdb.connect(db_path)
        try:
            totals = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_checks,
                    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS passes,
                    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS fails
                FROM observe_reports
                WHERE evaluated_at >= now() - INTERVAL 7 DAY
                """
            ).fetchone()
            top_failures = conn.execute(
                """
                SELECT check_name, column_name, COUNT(*) AS n
                FROM observe_reports
                WHERE status = 'fail' AND evaluated_at >= now() - INTERVAL 7 DAY
                GROUP BY check_name, column_name
                ORDER BY n DESC
                LIMIT 5
                """
            ).fetchall()
        finally:
            conn.close()
        total, passes, fails = totals or (0, 0, 0)
        return {
            "total_checks": total or 0,
            "passes": passes or 0,
            "fails": fails or 0,
            "pass_rate": round((passes or 0) / total, 3) if total else 1.0,
            "top_failures": [
                {"check": c, "column": col or "-", "count": n} for c, col, n in top_failures
            ],
        }

    @task
    def compute_quality_trends(summary: dict) -> dict:
        # Flag degrading pipelines: pass rate < 0.8 in last 3 days
        import duckdb

        db_path = os.environ.get(
            "OBSERVE_DUCKDB_PATH", "/usr/local/airflow/data/observe_reports.duckdb"
        )
        degrading = []
        if os.path.exists(db_path):
            conn = duckdb.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT pipeline_name,
                           SUM(CASE WHEN status='pass' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS pass_rate
                    FROM observe_reports
                    WHERE evaluated_at >= now() - INTERVAL 3 DAY
                    GROUP BY pipeline_name
                    HAVING pass_rate < 0.8
                    """
                ).fetchall()
                degrading = [{"pipeline": p, "pass_rate": round(r, 3)} for p, r in rows]
            finally:
                conn.close()
        return {**summary, "degrading": degrading}

    @task
    def post_digest_to_slack(payload: dict) -> str:
        if payload.get("empty"):
            print("No observe reports to summarize; skipping digest.")
            return "skipped"
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        lines = [
            f"*📊 Weekly Data Quality Digest (Week of {date_str})*",
            f"Total checks: {payload['total_checks']:,}",
            f"Pass rate: {payload['pass_rate'] * 100:.1f}%",
            f"Failures: {payload['fails']}",
        ]
        if payload.get("degrading"):
            lines.append("\n*⚠️ Degrading pipelines:*")
            for d in payload["degrading"]:
                lines.append(f"  • {d['pipeline']} → {d['pass_rate'] * 100:.0f}%")
        if payload.get("top_failures"):
            lines.append("\n*Top failures:*")
            for f in payload["top_failures"]:
                lines.append(f"  • {f['check']} / {f['column']} ({f['count']})")
        message = "\n".join(lines)
        print(message)
        # Posting to Slack via SlackSink would happen here; we keep the print
        # so the DAG works without a real webhook configured.
        return message

    summary = load_weekly_reports()
    trends = compute_quality_trends(summary)
    post_digest_to_slack(trends)


dag = observe_weekly_digest()
