"""All sink implementations."""

from sentinel.sinks.base import BaseSink
from sentinel.sinks.bigquery_sink import BigQuerySink
from sentinel.sinks.log_sink import LogSink
from sentinel.sinks.prometheus_sink import PrometheusSink
from sentinel.sinks.slack_sink import SlackSink

__all__ = [
    "BaseSink",
    "BigQuerySink",
    "LogSink",
    "PrometheusSink",
    "SlackSink",
]
