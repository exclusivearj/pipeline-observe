"""All sink implementations."""

from observe.sinks.base import BaseSink
from observe.sinks.bigquery_sink import BigQuerySink
from observe.sinks.log_sink import LogSink
from observe.sinks.prometheus_sink import PrometheusSink
from observe.sinks.slack_sink import SlackSink

__all__ = [
    "BaseSink",
    "BigQuerySink",
    "LogSink",
    "PrometheusSink",
    "SlackSink",
]
