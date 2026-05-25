"""PrometheusSink: pushes metrics to a Pushgateway using stdlib urllib."""

from __future__ import annotations

import logging
import re
import urllib.error
import urllib.request

from sentinel.report import CheckStatus, ObservabilityReport
from sentinel.sinks.base import BaseSink

_LABEL_SAFE = re.compile(r'[\\"\n]')


def _escape_label_value(value: str) -> str:
    return _LABEL_SAFE.sub("_", str(value))


def _sanitize_segment(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "_", value or "default") or "default"


class PrometheusSink(BaseSink):
    def __init__(
        self,
        pushgateway_url: str,
        job_name: str = "pipeline_sentinel",
        timeout: float = 5.0,
    ) -> None:
        self.pushgateway_url = pushgateway_url.rstrip("/")
        self.job_name = job_name
        self.timeout = timeout
        self._logger = logging.getLogger(__name__)

    def _format_metrics(self, report: ObservabilityReport) -> str:
        lines: list[str] = []
        pipeline = _escape_label_value(report.pipeline_name)
        table = _escape_label_value(report.table_name)

        lines.append("# TYPE sentinel_check_status gauge")
        for result in report.check_results:
            status_val = 1 if result.status == CheckStatus.PASS else 0
            column = _escape_label_value(result.column or "")
            check = _escape_label_value(result.check_name)
            lines.append(
                f'sentinel_check_status{{pipeline="{pipeline}",table="{table}",'
                f'check="{check}",column="{column}"}} {status_val}'
            )

        lines.append("# TYPE sentinel_row_count gauge")
        lines.append(
            f'sentinel_row_count{{pipeline="{pipeline}",table="{table}"}} {report.row_count}'
        )

        lines.append("# TYPE sentinel_pipeline_duration_ms gauge")
        lines.append(
            f'sentinel_pipeline_duration_ms{{pipeline="{pipeline}",table="{table}"}} '
            f"{report.duration_ms}"
        )
        return "\n".join(lines) + "\n"

    def write(self, report: ObservabilityReport) -> None:
        url = f"{self.pushgateway_url}/metrics/job/{_sanitize_segment(self.job_name)}"
        body = self._format_metrics(report).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "text/plain; version=0.0.4"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                resp.read()
        except urllib.error.HTTPError as e:
            self._logger.error("PrometheusSink HTTP %s: %s", e.code, e.reason)
        except urllib.error.URLError as e:
            self._logger.error("PrometheusSink URL error: %s", e.reason)
