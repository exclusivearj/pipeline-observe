"""Tests for SlackSink."""

import json
from unittest.mock import MagicMock, patch

import pytest

from observe.report import CheckResult, CheckStatus, ObservabilityReport
from observe.sinks import SlackSink


def _make_report(status: CheckStatus) -> ObservabilityReport:
    report = ObservabilityReport(pipeline_name="pipe", table_name="tbl", row_count=42)
    report.check_results.append(
        CheckResult(
            check_name="RowCountCheck",
            check_params={"min": 10},
            status=status,
            metric_value=42,
            threshold=10,
            message="ok",
        )
    )
    return report


def test_requires_webhook_url():
    with pytest.raises(ValueError):
        SlackSink(webhook_url="")


def test_post_called_on_failure():
    sink = SlackSink(webhook_url="https://hooks.example.com/abc")
    report = _make_report(CheckStatus.FAIL)
    with patch("observe.sinks.slack_sink.urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.__enter__.return_value.read.return_value = b"ok"
        sink.write(report)
        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args.args[0]
        body = json.loads(req.data)
        assert "blocks" in body
        assert "pipe" in body["text"]
        assert "tbl" in body["text"]


def test_skip_when_only_on_failure_and_status_pass():
    sink = SlackSink(webhook_url="https://hooks.example.com/abc", only_on_failure=True)
    report = _make_report(CheckStatus.PASS)
    with patch("observe.sinks.slack_sink.urllib.request.urlopen") as mock_urlopen:
        sink.write(report)
        mock_urlopen.assert_not_called()


def test_does_not_raise_on_http_error():
    import urllib.error

    sink = SlackSink(webhook_url="https://hooks.example.com/abc")
    report = _make_report(CheckStatus.FAIL)
    with patch(
        "observe.sinks.slack_sink.urllib.request.urlopen",
        side_effect=urllib.error.HTTPError("u", 500, "boom", {}, None),
    ):
        sink.write(report)  # must not raise


def test_safe_write_does_not_raise_on_url_error():
    import urllib.error

    sink = SlackSink(webhook_url="https://hooks.example.com/abc")
    report = _make_report(CheckStatus.FAIL)
    with patch(
        "observe.sinks.slack_sink.urllib.request.urlopen",
        side_effect=urllib.error.URLError("connection refused"),
    ):
        sink.write(report)


def test_channel_override_included():
    sink = SlackSink(
        webhook_url="https://hooks.example.com/abc",
        channel="#data-quality",
    )
    report = _make_report(CheckStatus.FAIL)
    with patch("observe.sinks.slack_sink.urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.__enter__.return_value.read.return_value = b"ok"
        sink.write(report)
        body = json.loads(mock_urlopen.call_args.args[0].data)
        assert body.get("channel") == "#data-quality"
