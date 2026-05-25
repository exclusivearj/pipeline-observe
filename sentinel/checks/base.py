"""BaseCheck abstract class and dataframe duck-typing helpers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional, Union

import pandas as pd

try:  # pragma: no cover - optional dep
    from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore

    AnyDataFrame = Union[pd.DataFrame, SparkDataFrame]  # type: ignore[valid-type]
    _HAS_SPARK = True
except Exception:  # pragma: no cover
    SparkDataFrame = None  # type: ignore[assignment,misc]
    AnyDataFrame = pd.DataFrame  # type: ignore[misc,assignment]
    _HAS_SPARK = False

from sentinel.report import CheckResult, CheckStatus


class BaseCheck(ABC):
    """Abstract base class for all data quality checks.

    Subclasses implement `evaluate(df)` and `_params_dict()`. The runner
    should call `_safe_evaluate` so any unexpected exception inside a
    check becomes a CheckResult with status=ERROR rather than crashing
    the whole pipeline.
    """

    def __init__(self, name: Optional[str] = None) -> None:
        self.name = name or self.__class__.__name__

    @abstractmethod
    def evaluate(self, df: Any) -> CheckResult:
        """Evaluate the check against the given DataFrame."""

    def _params_dict(self) -> dict:
        """Return constructor params as dict for reporting. Override in subclasses."""
        return {}

    def _is_spark(self, df: Any) -> bool:
        if not _HAS_SPARK or SparkDataFrame is None:
            return False
        return isinstance(df, SparkDataFrame)

    def _get_row_count(self, df: Any) -> int:
        if self._is_spark(df):
            return int(df.count())
        return int(len(df))

    def _columns(self, df: Any) -> list[str]:
        if self._is_spark(df):
            return list(df.columns)
        return list(df.columns)

    def _safe_evaluate(self, df: Any) -> CheckResult:
        try:
            return self.evaluate(df)
        except Exception as e:
            return CheckResult(
                check_name=self.name,
                check_params=self._params_dict(),
                status=CheckStatus.ERROR,
                metric_value=None,
                threshold=None,
                message=f"Check raised exception: {type(e).__name__}: {e}",
            )

    def _skip(self, message: str, column: Optional[str] = None) -> CheckResult:
        return CheckResult(
            check_name=self.name,
            check_params=self._params_dict(),
            status=CheckStatus.SKIP,
            metric_value=None,
            threshold=None,
            message=message,
            column=column,
        )
