"""UniquenessCheck: duplicate rate per column <= threshold."""

from __future__ import annotations

from typing import Any

from sentinel.checks.base import BaseCheck
from sentinel.exceptions import CheckConfigurationError
from sentinel.report import CheckResult, CheckStatus


class UniquenessCheck(BaseCheck):
    def __init__(self, column: str, threshold: float = 0.0, name: str | None = None) -> None:
        super().__init__(name=name)
        if not 0.0 <= threshold <= 1.0:
            raise CheckConfigurationError(
                f"UniquenessCheck: threshold must be in [0, 1], got {threshold}"
            )
        self.column = column
        self.threshold = threshold

    def _params_dict(self) -> dict:
        return {"column": self.column, "threshold": self.threshold}

    def evaluate(self, df: Any) -> CheckResult:
        if self.column not in self._columns(df):
            return self._skip(
                message=f"column '{self.column}' not in DataFrame",
                column=self.column,
            )
        total = self._get_row_count(df)
        if total == 0:
            return self._skip(
                message="empty DataFrame; cannot compute duplicate rate",
                column=self.column,
            )
        if self._is_spark(df):
            n_unique = int(df.select(self.column).distinct().count())
        else:
            n_unique = int(df[self.column].nunique(dropna=False))
        dup_rate = round(1.0 - (n_unique / total), 4)
        passed = dup_rate <= self.threshold
        status = CheckStatus.PASS if passed else CheckStatus.FAIL
        message = (
            f"dup_rate={dup_rate} <= threshold={self.threshold} ({n_unique} unique of {total})"
            if passed
            else f"dup_rate={dup_rate} > threshold={self.threshold} ({n_unique} unique of {total})"
        )
        return CheckResult(
            check_name=self.name,
            check_params=self._params_dict(),
            status=status,
            metric_value=dup_rate,
            threshold=self.threshold,
            message=message,
            column=self.column,
        )
