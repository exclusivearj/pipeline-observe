"""AnomalyCheck: current metric within N stddev of rolling baseline."""

from __future__ import annotations

import statistics
from typing import Any, Optional

import pandas as pd

from observe.checks.base import BaseCheck
from observe.exceptions import CheckConfigurationError
from observe.report import CheckResult, CheckStatus

ALLOWED_METRICS = {"row_count", "mean", "null_rate"}


class AnomalyCheck(BaseCheck):
    def __init__(
        self,
        column: Optional[str] = None,
        metric: str = "row_count",
        baseline: Optional[list[float]] = None,
        stddev_threshold: float = 3.0,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name)
        if metric not in ALLOWED_METRICS:
            raise CheckConfigurationError(
                f"AnomalyCheck: metric must be one of {sorted(ALLOWED_METRICS)}, got '{metric}'"
            )
        if metric in {"mean", "null_rate"} and not column:
            raise CheckConfigurationError(
                f"AnomalyCheck: column is required when metric='{metric}'"
            )
        if stddev_threshold <= 0:
            raise CheckConfigurationError(
                f"AnomalyCheck: stddev_threshold must be > 0, got {stddev_threshold}"
            )
        self.column = column
        self.metric = metric
        self.baseline = list(baseline) if baseline else []
        self.stddev_threshold = stddev_threshold

    def _params_dict(self) -> dict:
        return {
            "column": self.column,
            "metric": self.metric,
            "baseline_n": len(self.baseline),
            "stddev_threshold": self.stddev_threshold,
        }

    def _current_value(self, df: Any) -> Optional[float]:
        if self.metric == "row_count":
            return float(self._get_row_count(df))
        if self.column not in self._columns(df):
            return None
        if self._is_spark(df):
            from pyspark.sql import functions as F  # type: ignore

            if self.metric == "mean":
                row = df.agg(F.mean(self.column).alias("m")).collect()
                return float(row[0]["m"]) if row and row[0]["m"] is not None else None
            if self.metric == "null_rate":
                total = self._get_row_count(df)
                if total == 0:
                    return None
                nulls = int(df.filter(F.col(self.column).isNull()).count())
                return nulls / total
        else:
            series = df[self.column]
            if self.metric == "mean":
                clean = series.dropna()
                return float(clean.mean()) if len(clean) else None
            if self.metric == "null_rate":
                total = len(series)
                return float(pd.isna(series).sum()) / total if total else None
        return None

    def evaluate(self, df: Any) -> CheckResult:
        if len(self.baseline) < 3:
            return self._skip(
                message=f"baseline has {len(self.baseline)} values; need >= 3 to compute z-score",
                column=self.column,
            )
        current = self._current_value(df)
        if current is None:
            return self._skip(
                message=f"could not compute current value of metric '{self.metric}'"
                + (f" for column '{self.column}'" if self.column else ""),
                column=self.column,
            )

        rolling_mean = statistics.fmean(self.baseline)
        rolling_std = statistics.pstdev(self.baseline) if len(self.baseline) > 1 else 0.0
        denom = rolling_std if rolling_std > 0 else 1.0
        z_score = abs(current - rolling_mean) / denom
        passed = z_score <= self.stddev_threshold
        status = CheckStatus.PASS if passed else CheckStatus.FAIL
        message = (
            f"current={current:.4f}, rolling_mean={rolling_mean:.4f}, z={z_score:.3f} <= {self.stddev_threshold}"
            if passed
            else f"anomaly: current={current:.4f} vs rolling_mean={rolling_mean:.4f}, z={z_score:.3f} > {self.stddev_threshold}"
        )
        return CheckResult(
            check_name=self.name,
            check_params=self._params_dict(),
            status=status,
            metric_value={
                "current": round(current, 6),
                "rolling_mean": round(rolling_mean, 6),
                "rolling_stddev": round(rolling_std, 6),
                "z_score": round(z_score, 4),
            },
            threshold=self.stddev_threshold,
            message=message,
            column=self.column,
        )
