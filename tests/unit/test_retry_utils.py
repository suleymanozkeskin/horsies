"""Unit tests for horsies.core.utils.retry shared utilities."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from horsies.core.models.tasks import OperationalErrorCode
from horsies.core.utils.retry import (
    calculate_retry_delay,
    check_retry_eligibility,
    parse_retry_policy,
)


@pytest.mark.unit
class TestCalculateRetryDelay:
    """Tests for calculate_retry_delay (extracted from Worker._calculate_retry_delay)."""

    def test_fixed_strategy_uses_intervals(self) -> None:
        policy = {'intervals': [10, 20, 30], 'backoff_strategy': 'fixed', 'jitter': False}
        assert calculate_retry_delay(1, policy) == 10.0
        assert calculate_retry_delay(2, policy) == 20.0
        assert calculate_retry_delay(3, policy) == 30.0

    def test_fixed_strategy_clamps_beyond_length(self) -> None:
        policy = {'intervals': [10, 20], 'backoff_strategy': 'fixed', 'jitter': False}
        assert calculate_retry_delay(5, policy) == 20.0

    def test_exponential_strategy(self) -> None:
        policy = {'intervals': [10], 'backoff_strategy': 'exponential', 'jitter': False}
        assert calculate_retry_delay(1, policy) == 10.0
        assert calculate_retry_delay(2, policy) == 20.0
        assert calculate_retry_delay(3, policy) == 40.0

    def test_jitter_varies_result(self) -> None:
        policy = {'intervals': [100], 'backoff_strategy': 'fixed', 'jitter': True}
        results = {calculate_retry_delay(1, policy) for _ in range(50)}
        assert len(results) > 1, 'Jitter should produce varied results'

    def test_minimum_delay_is_one(self) -> None:
        policy = {'intervals': [0], 'backoff_strategy': 'fixed', 'jitter': False}
        assert calculate_retry_delay(1, policy) == 1.0

    def test_defaults_when_keys_missing(self) -> None:
        result = calculate_retry_delay(1, {})
        assert result >= 1.0


@pytest.mark.unit
class TestParseRetryPolicy:
    """Tests for parse_retry_policy."""

    def test_returns_none_for_none_input(self) -> None:
        assert parse_retry_policy(None) is None

    def test_returns_none_for_empty_string(self) -> None:
        assert parse_retry_policy('') is None

    def test_returns_none_for_corrupt_json(self) -> None:
        assert parse_retry_policy('{bad json') is None

    def test_returns_none_for_non_dict(self) -> None:
        assert parse_retry_policy(json.dumps([1, 2, 3])) is None

    def test_returns_none_when_no_retry_policy_key(self) -> None:
        assert parse_retry_policy(json.dumps({'task_name': 'x'})) is None

    def test_returns_none_when_retry_policy_is_not_dict(self) -> None:
        assert parse_retry_policy(json.dumps({'retry_policy': 'not a dict'})) is None

    def test_returns_policy_dict(self) -> None:
        opts = {'retry_policy': {'max_retries': 3, 'auto_retry_for': ['WORKER_CRASHED']}}
        result = parse_retry_policy(json.dumps(opts))
        assert result is not None
        assert result['max_retries'] == 3


@pytest.mark.unit
class TestCheckRetryEligibility:
    """Tests for check_retry_eligibility."""

    _DB_NOW = datetime(2025, 6, 1, 12, 0, tzinfo=timezone.utc)

    def _make_opts(
        self,
        *,
        auto_retry_for: list[str] | None = None,
        max_retries: int = 3,
    ) -> str:
        return json.dumps({
            'task_name': 'test',
            'retry_policy': {
                'max_retries': max_retries,
                'intervals': [60],
                'backoff_strategy': 'exponential',
                'jitter': False,
                'auto_retry_for': auto_retry_for or ['WORKER_CRASHED'],
            },
        })

    def test_eligible_when_code_matches_and_retries_remaining(self) -> None:
        assert check_retry_eligibility(
            retry_count=0, max_retries=3,
            task_options_json=self._make_opts(),
            error_code=OperationalErrorCode.WORKER_CRASHED,
            good_until=None, db_now=self._DB_NOW,
        ) is True

    def test_not_eligible_when_retries_exhausted(self) -> None:
        assert check_retry_eligibility(
            retry_count=3, max_retries=3,
            task_options_json=self._make_opts(),
            error_code=OperationalErrorCode.WORKER_CRASHED,
            good_until=None, db_now=self._DB_NOW,
        ) is False

    def test_not_eligible_when_max_retries_zero(self) -> None:
        assert check_retry_eligibility(
            retry_count=0, max_retries=0,
            task_options_json=self._make_opts(),
            error_code=OperationalErrorCode.WORKER_CRASHED,
            good_until=None, db_now=self._DB_NOW,
        ) is False

    def test_not_eligible_when_no_task_options(self) -> None:
        assert check_retry_eligibility(
            retry_count=0, max_retries=3,
            task_options_json=None,
            error_code=OperationalErrorCode.WORKER_CRASHED,
            good_until=None, db_now=self._DB_NOW,
        ) is False

    def test_not_eligible_when_code_not_in_auto_retry_for(self) -> None:
        assert check_retry_eligibility(
            retry_count=0, max_retries=3,
            task_options_json=self._make_opts(auto_retry_for=['RATE_LIMITED']),
            error_code=OperationalErrorCode.WORKER_CRASHED,
            good_until=None, db_now=self._DB_NOW,
        ) is False

    def test_not_eligible_when_good_until_passed(self) -> None:
        past = self._DB_NOW - timedelta(minutes=5)
        assert check_retry_eligibility(
            retry_count=0, max_retries=3,
            task_options_json=self._make_opts(),
            error_code=OperationalErrorCode.WORKER_CRASHED,
            good_until=past, db_now=self._DB_NOW,
        ) is False

    def test_eligible_when_good_until_in_future(self) -> None:
        future = self._DB_NOW + timedelta(hours=1)
        assert check_retry_eligibility(
            retry_count=0, max_retries=3,
            task_options_json=self._make_opts(),
            error_code=OperationalErrorCode.WORKER_CRASHED,
            good_until=future, db_now=self._DB_NOW,
        ) is True

    def test_string_error_code_matches(self) -> None:
        assert check_retry_eligibility(
            retry_count=0, max_retries=3,
            task_options_json=self._make_opts(auto_retry_for=['MY_ERROR']),
            error_code='MY_ERROR',
            good_until=None, db_now=self._DB_NOW,
        ) is True

    def test_none_error_code_not_eligible(self) -> None:
        assert check_retry_eligibility(
            retry_count=0, max_retries=3,
            task_options_json=self._make_opts(),
            error_code=None,
            good_until=None, db_now=self._DB_NOW,
        ) is False
