"""LogSink: emits report to Python logging."""

from __future__ import annotations

import logging

from sentinel.report import CheckStatus, ObservabilityReport
from sentinel.sinks.base import BaseSink


class LogSink(BaseSink):
    def __init__(self, level: str = "INFO", logger_name: str = "pipeline-observe") -> None:
        self.level = logging.getLevelName(level.upper())
        self.logger = logging.getLogger(logger_name)

    def write(self, report: ObservabilityReport) -> None:
        status = report.overall_status
        header = (
            f"[sentinel] {report.pipeline_name}/{report.table_name} | "
            f"{status.value.upper()} | "
            f"{len(report.check_results)} checks | "
            f"{report.duration_ms:.1f}ms"
        )
        self.logger.log(self.level, header)

        for result in report.check_results:
            if result.status in (CheckStatus.FAIL, CheckStatus.ERROR):
                self.logger.log(
                    logging.ERROR if result.status == CheckStatus.FAIL else logging.WARNING,
                    "  ✗ %s%s | metric=%s threshold=%s | %s",
                    result.check_name,
                    f"({result.column})" if result.column else "",
                    result.metric_value,
                    result.threshold,
                    result.message,
                )
            elif result.status == CheckStatus.WARN:
                self.logger.warning(
                    "  ! %s | %s",
                    result.check_name,
                    result.message,
                )
