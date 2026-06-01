"""pipeline-observe: decorator-based data observability for pandas and PySpark."""

from observe.core import observe
from observe.exceptions import CheckConfigurationError, DataQualityError, SinkError
from observe.report import CheckResult, CheckStatus, ObservabilityReport
from observe.checks import (
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
from observe.sinks import (
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
