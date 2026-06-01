"""Tests for FreshnessCheck."""

import pytest

from observe.checks import FreshnessCheck
from observe.exceptions import CheckConfigurationError
from observe.report import CheckStatus


def test_pass_recent_timestamp(sample_df_clean):
    result = FreshnessCheck("event_ts", max_lag_hours=24).evaluate(sample_df_clean)
    assert result.status == CheckStatus.PASS


def test_fail_stale_timestamp(sample_df_stale):
    result = FreshnessCheck("event_ts", max_lag_hours=24).evaluate(sample_df_stale)
    assert result.status == CheckStatus.FAIL
    assert result.metric_value > 24


def test_skip_when_column_missing(sample_df_clean):
    result = FreshnessCheck("nope").evaluate(sample_df_clean)
    assert result.status == CheckStatus.SKIP


def test_configuration_non_positive_lag():
    with pytest.raises(CheckConfigurationError):
        FreshnessCheck("event_ts", max_lag_hours=0)
