"""Tests for LogSink."""

import logging

from observe.checks import NullRateCheck, RowCountCheck
from observe.report import CheckResult, CheckStatus, ObservabilityReport
from observe.sinks import LogSink


def test_log_sink_logs_header(sample_df_clean, caplog):
    sink = LogSink()
    report = ObservabilityReport(pipeline_name="p", table_name="t")
    report.check_results.append(
        CheckResult(
            check_name="RowCountCheck",
            check_params={},
            status=CheckStatus.PASS,
            metric_value=1000,
            threshold=100,
            message="ok",
        )
    )
    with caplog.at_level(logging.INFO, logger="pipeline-observe"):
        sink.write(report)
    assert any("p/t" in rec.message for rec in caplog.records)


def test_log_sink_logs_failures(caplog):
    sink = LogSink()
    report = ObservabilityReport(pipeline_name="p", table_name="t")
    report.check_results.append(
        CheckResult(
            check_name="NullRateCheck",
            check_params={},
            status=CheckStatus.FAIL,
            metric_value=0.5,
            threshold=0.01,
            message="too many nulls",
            column="user_id",
        )
    )
    with caplog.at_level(logging.ERROR, logger="pipeline-observe"):
        sink.write(report)
    failure_logs = [rec for rec in caplog.records if "NullRateCheck" in rec.message]
    assert failure_logs


def test_log_sink_safe_write_swallows_exceptions(monkeypatch):
    sink = LogSink()
    report = ObservabilityReport()

    def boom(self, report):
        raise RuntimeError("nope")

    monkeypatch.setattr(LogSink, "write", boom)
    sink._safe_write(report)  # must not raise
