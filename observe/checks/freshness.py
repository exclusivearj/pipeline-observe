"""FreshnessCheck: max(timestamp_col) within max_lag_hours of now."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from observe.checks.base import BaseCheck
from observe.exceptions import CheckConfigurationError
from observe.report import CheckResult, CheckStatus


class FreshnessCheck(BaseCheck):
    def __init__(
        self,
        timestamp_column: str,
        max_lag_hours: float = 24.0,
        timezone: str = "UTC",
        name: str | None = None,
    ) -> None:
        super().__init__(name=name)
        if max_lag_hours <= 0:
            raise CheckConfigurationError(
                f"FreshnessCheck: max_lag_hours must be > 0, got {max_lag_hours}"
            )
        self.timestamp_column = timestamp_column
        self.max_lag_hours = max_lag_hours
        self.timezone = timezone

    def _params_dict(self) -> dict:
        return {
            "timestamp_column": self.timestamp_column,
            "max_lag_hours": self.max_lag_hours,
            "timezone": self.timezone,
        }

    def evaluate(self, df: Any) -> CheckResult:
        if self.timestamp_column not in self._columns(df):
            return self._skip(
                message=f"column '{self.timestamp_column}' not in DataFrame",
                column=self.timestamp_column,
            )
        if self._is_spark(df):
            from pyspark.sql import functions as F  # type: ignore

            row = df.agg(F.max(self.timestamp_column).alias("m")).collect()
            max_ts = row[0]["m"] if row else None
        else:
            series = df[self.timestamp_column]
            max_ts = series.max() if len(series) else None

        if max_ts is None or (isinstance(max_ts, float) and pd.isna(max_ts)):
            return self._skip(
                message=f"no timestamps in column '{self.timestamp_column}'",
                column=self.timestamp_column,
            )

        if isinstance(max_ts, pd.Timestamp):
            max_dt = max_ts.to_pydatetime()
        elif isinstance(max_ts, datetime):
            max_dt = max_ts
        else:
            return self._skip(
                message=f"column '{self.timestamp_column}' is not a timestamp type ({type(max_ts).__name__})",
                column=self.timestamp_column,
            )

        if max_dt.tzinfo is None:
            max_dt = max_dt.replace(tzinfo=timezone.utc)

        now_utc = datetime.now(timezone.utc)
        lag_hours = (now_utc - max_dt).total_seconds() / 3600.0
        passed = lag_hours <= self.max_lag_hours
        status = CheckStatus.PASS if passed else CheckStatus.FAIL
        message = (
            f"max_ts={max_dt.isoformat()}, lag={lag_hours:.2f}h <= max_lag={self.max_lag_hours}h"
            if passed
            else f"max_ts={max_dt.isoformat()}, lag={lag_hours:.2f}h > max_lag={self.max_lag_hours}h"
        )
        return CheckResult(
            check_name=self.name,
            check_params=self._params_dict(),
            status=status,
            metric_value=round(lag_hours, 4),
            threshold=self.max_lag_hours,
            message=message,
            column=self.timestamp_column,
        )
