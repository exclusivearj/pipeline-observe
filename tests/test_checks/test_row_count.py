"""Tests for RowCountCheck."""

import pytest

from observe.checks import RowCountCheck
from observe.exceptions import CheckConfigurationError
from observe.report import CheckStatus


def test_pass_within_bounds(sample_df_clean):
    result = RowCountCheck(min=100, max=10_000).evaluate(sample_df_clean)
    assert result.status == CheckStatus.PASS
    assert result.metric_value == 1000


def test_fail_below_min(sample_df_small):
    result = RowCountCheck(min=100).evaluate(sample_df_small)
    assert result.status == CheckStatus.FAIL
    assert result.metric_value == 10
    assert "10" in result.message


def test_fail_above_max(sample_df_clean):
    result = RowCountCheck(max=500).evaluate(sample_df_clean)
    assert result.status == CheckStatus.FAIL
    assert result.metric_value == 1000


def test_default_max_is_infinity(sample_df_clean):
    result = RowCountCheck(min=0).evaluate(sample_df_clean)
    assert result.status == CheckStatus.PASS


def test_configuration_negative_min():
    with pytest.raises(CheckConfigurationError):
        RowCountCheck(min=-1)


def test_configuration_max_less_than_min():
    with pytest.raises(CheckConfigurationError):
        RowCountCheck(min=100, max=50)


def test_safe_evaluate_catches_exception():
    check = RowCountCheck(min=0)
    # Pass a non-DataFrame to force an exception in evaluate
    result = check._safe_evaluate(object())
    assert result.status == CheckStatus.ERROR
    assert "exception" in result.message.lower()
