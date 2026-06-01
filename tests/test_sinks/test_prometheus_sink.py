"""Tests for PrometheusSink."""

from unittest.mock import patch

from observe.report import CheckResult, CheckStatus, ObservabilityReport
from observe.sinks import PrometheusSink


def _make_report() -> ObservabilityReport:
    report = ObservabilityReport(
        pipeline_name="my_pipe",
        table_name="my_table",
        row_count=1234,
        duration_ms=89.0,
    )
    report.check_results.append(
        CheckResult(
            check_name="RowCountCheck",
            check_params={},
            status=CheckStatus.PASS,
            metric_value=1234,
            threshold=100,
            message="ok",
        )
    )
    report.check_results.append(
        CheckResult(
            check_name="NullRateCheck",
            check_params={},
            status=CheckStatus.FAIL,
            metric_value=0.5,
            threshold=0.01,
            message="too many",
            column="user_id",
        )
    )
    return report


def test_format_metrics_contains_all_three_metric_types():
    sink = PrometheusSink(pushgateway_url="http://pg:9091")
    body = sink._format_metrics(_make_report())
    assert "observe_check_status" in body
    assert "observe_row_count" in body
    assert "observe_pipeline_duration_ms" in body
    assert 'pipeline="my_pipe"' in body
    assert 'table="my_table"' in body


def test_check_status_value_is_zero_for_fail():
    sink = PrometheusSink(pushgateway_url="http://pg:9091")
    body = sink._format_metrics(_make_report())
    lines = [ln for ln in body.splitlines() if ln.startswith("observe_check_status")]
    fail_line = next(ln for ln in lines if "NullRateCheck" in ln)
    assert fail_line.strip().endswith(" 0")


def test_post_called_with_correct_url():
    sink = PrometheusSink(pushgateway_url="http://pg:9091", job_name="my_job")
    with patch("observe.sinks.prometheus_sink.urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.__enter__.return_value.read.return_value = b""
        sink.write(_make_report())
        req = mock_urlopen.call_args.args[0]
        assert req.full_url == "http://pg:9091/metrics/job/my_job"


def test_does_not_raise_on_url_error():
    import urllib.error

    sink = PrometheusSink(pushgateway_url="http://pg:9091")
    with patch(
        "observe.sinks.prometheus_sink.urllib.request.urlopen",
        side_effect=urllib.error.URLError("boom"),
    ):
        sink.write(_make_report())
