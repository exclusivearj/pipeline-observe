"""pipeline-sentinel: decorator-based data observability for pandas and PySpark."""

from sentinel.core import observe
from sentinel.exceptions import CheckConfigurationError, DataQualityError, SinkError
from sentinel.report import CheckResult, CheckStatus, ObservabilityReport
from sentinel.checks import (
    AnomalyCheck,
    BaseCheck,
    DistributionCheck,
    FreshnessCheck,
    NullRateCheck,
    RangeCheck,
    RowCountCheck,
    SchemaCheck,
    UniquenessCheck,
)
from sentinel.sinks import (
    BaseSink,
    BigQuerySink,
    LogSink,
    PrometheusSink,
    SlackSink,
)

__version__ = "0.1.0"

__all__ = [
    "observe",
    # Reports
    "CheckResult",
    "CheckStatus",
    "ObservabilityReport",
    # Exceptions
    "CheckConfigurationError",
    "DataQualityError",
    "SinkError",
    # Checks
    "BaseCheck",
    "AnomalyCheck",
    "DistributionCheck",
    "FreshnessCheck",
    "NullRateCheck",
    "RangeCheck",
    "RowCountCheck",
    "SchemaCheck",
    "UniquenessCheck",
    # Sinks
    "BaseSink",
    "BigQuerySink",
    "LogSink",
    "PrometheusSink",
    "SlackSink",
]
