"""Custom exceptions for pipeline-sentinel."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from sentinel.report import ObservabilityReport


class DataQualityError(Exception):
    """Raised when on_failure='raise' and a check fails.

    The full ObservabilityReport is attached so the caller can inspect
    every CheckResult, not just the message.
    """

    def __init__(self, message: str, report: Optional["ObservabilityReport"] = None) -> None:
        super().__init__(message)
        self.report = report


class CheckConfigurationError(ValueError):
    """Raised for invalid check parameters at construction time."""


class SinkError(RuntimeError):
    """Raised when a sink fails to write.

    This is caught internally by BaseSink._safe_write so it does not
    propagate to the user; we never want a sink failure to break a
    pipeline.
    """
