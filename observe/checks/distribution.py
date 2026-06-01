"""DistributionCheck: z-score of column mean vs. baseline."""

from __future__ import annotations

from typing import Any, Optional

from observe.checks.base import BaseCheck
from observe.exceptions import CheckConfigurationError
from observe.report import CheckResult, CheckStatus


class DistributionCheck(BaseCheck):
    def __init__(
        self,
        column: str,
        baseline_mean: float,
        z_score_threshold: float = 3.0,
        baseline_stddev: Optional[float] = None,
        name: str | None = None,
    ) -> None:
        super().__init__(name=name)
        if z_score_threshold <= 0:
            raise CheckConfigurationError(
                f"DistributionCheck: z_score_threshold must be > 0, got {z_score_threshold}"
            )
        if baseline_stddev is not None and baseline_stddev < 0:
            raise CheckConfigurationError(
                f"DistributionCheck: baseline_stddev must be >= 0, got {baseline_stddev}"
            )
        self.column = column
        self.baseline_mean = baseline_mean
        self.z_score_threshold = z_score_threshold
        self.baseline_stddev = baseline_stddev

    def _params_dict(self) -> dict:
        return {
            "column": self.column,
            "baseline_mean": self.baseline_mean,
            "z_score_threshold": self.z_score_threshold,
            "baseline_stddev": self.baseline_stddev,
        }

    def evaluate(self, df: Any) -> CheckResult:
        if self.column not in self._columns(df):
            return self._skip(
                message=f"column '{self.column}' not in DataFrame",
                column=self.column,
            )
        if self._is_spark(df):
            from pyspark.sql import functions as F  # type: ignore

            row = df.agg(
                F.mean(self.column).alias("m"),
                F.stddev(self.column).alias("s"),
            ).collect()
            actual_mean = float(row[0]["m"]) if row and row[0]["m"] is not None else None
            actual_std = float(row[0]["s"]) if row and row[0]["s"] is not None else None
        else:
            series = df[self.column].dropna()
            if len(series) == 0:
                return self._skip(
                    message=f"column '{self.column}' has no non-null values",
                    column=self.column,
                )
            actual_mean = float(series.mean())
            actual_std = float(series.std()) if len(series) > 1 else 0.0

        if actual_mean is None:
            return self._skip(
                message=f"could not compute mean for column '{self.column}'",
                column=self.column,
            )

        stddev = self.baseline_stddev if self.baseline_stddev is not None else (actual_std or 1.0)
        if stddev == 0:
            stddev = 1.0
        z_score = abs(actual_mean - self.baseline_mean) / stddev
        passed = z_score <= self.z_score_threshold
        status = CheckStatus.PASS if passed else CheckStatus.FAIL
        message = (
            f"mean={actual_mean:.4f}, baseline={self.baseline_mean:.4f}, z={z_score:.3f} <= {self.z_score_threshold}"
            if passed
            else f"mean={actual_mean:.4f} drifted from baseline={self.baseline_mean:.4f}, z={z_score:.3f} > {self.z_score_threshold}"
        )
        return CheckResult(
            check_name=self.name,
            check_params=self._params_dict(),
            status=status,
            metric_value={
                "actual_mean": round(actual_mean, 6),
                "baseline_mean": self.baseline_mean,
                "z_score": round(z_score, 4),
            },
            threshold=self.z_score_threshold,
            message=message,
            column=self.column,
        )
