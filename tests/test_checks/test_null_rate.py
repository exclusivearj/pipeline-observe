"""Tests for NullRateCheck."""

import pytest

from observe.checks import NullRateCheck
from observe.exceptions import CheckConfigurationError
from observe.report import CheckStatus


def test_pass_zero_nulls(sample_df_clean):
    result = NullRateCheck("user_id", threshold=0.01).evaluate(sample_df_clean)
    assert result.status == CheckStatus.PASS
    assert result.metric_value == 0.0


def test_fail_above_threshold(sample_df_with_nulls):
    result = NullRateCheck("user_id", threshold=0.01).evaluate(sample_df_with_nulls)
    assert result.status == CheckStatus.FAIL
    assert result.metric_value > 0.01


def test_pass_under_loose_threshold(sample_df_with_nulls):
    result = NullRateCheck("user_id", threshold=0.10).evaluate(sample_df_with_nulls)
    assert result.status == CheckStatus.PASS


def test_skip_when_column_missing(sample_df_clean):
    result = NullRateCheck("not_a_column").evaluate(sample_df_clean)
    assert result.status == CheckStatus.SKIP
    assert "not_a_column" in result.message


def test_configuration_invalid_threshold():
    with pytest.raises(CheckConfigurationError):
        NullRateCheck("col", threshold=1.5)
    with pytest.raises(CheckConfigurationError):
        NullRateCheck("col", threshold=-0.1)


def test_metric_value_is_rounded(sample_df_with_nulls):
    result = NullRateCheck("user_id", threshold=0.5).evaluate(sample_df_with_nulls)
    assert isinstance(result.metric_value, float)
    # 4 decimal places by spec
    assert round(result.metric_value, 4) == result.metric_value
