"""Unit tests for horsies.core.utils.retry shared utilities."""

from __future__ import annotations

import json

import pytest

from horsies.core.codec.task_options import InvalidRetryPolicy, NoRetryPolicy
from horsies.core.models.tasks import OperationalErrorCode, RetryPolicy
from horsies.core.utils.retry import (
    OptionsNotAnObject,
    admitted_retry_policy,
    calculate_retry_delay,
    read_stored_retry_policy,
)


def _fixed(intervals: list[int], *, jitter: bool = False) -> RetryPolicy:
    return RetryPolicy.fixed(intervals, auto_retry_for=['E'], jitter=jitter)


def _exponential(
    base: int,
    *,
    jitter: bool = False,
    max_delay_seconds: int | None = None,
) -> RetryPolicy:
    return RetryPolicy.exponential(
        base,
        max_retries=20,
        auto_retry_for=['E'],
        jitter=jitter,
        max_delay_seconds=max_delay_seconds,
    )


@pytest.mark.unit
class TestCalculateRetryDelay:
    """The delay of a retry from a validated policy; no default is used."""

    def test_fixed_strategy_uses_intervals(self) -> None:
        policy = _fixed([10, 20, 30])
        assert calculate_retry_delay(1, policy) == 10.0
        assert calculate_retry_delay(2, policy) == 20.0
        assert calculate_retry_delay(3, policy) == 30.0

    def test_fixed_strategy_clamps_beyond_length(self) -> None:
        assert calculate_retry_delay(5, _fixed([10, 20])) == 20.0

    def test_exponential_strategy(self) -> None:
        policy = _exponential(10)
        assert calculate_retry_delay(1, policy) == 10.0
        assert calculate_retry_delay(2, policy) == 20.0
        assert calculate_retry_delay(3, policy) == 40.0

    def test_exponential_without_max_delay_remains_uncapped(self) -> None:
        assert calculate_retry_delay(20, _exponential(60)) == 31_457_280.0

    def test_exponential_max_delay_caps_large_attempts(self) -> None:
        assert calculate_retry_delay(20, _exponential(60, max_delay_seconds=600)) == 600.0

    def test_max_delay_caps_after_jitter(self) -> None:
        policy = _exponential(60, jitter=True, max_delay_seconds=600)
        for _ in range(50):
            assert calculate_retry_delay(5, policy) <= 600.0

    def test_jitter_varies_result(self) -> None:
        policy = _fixed([100], jitter=True)
        results = {calculate_retry_delay(1, policy) for _ in range(50)}
        assert len(results) > 1, 'Jitter should produce varied results'

    def test_jitter_above_floor_does_not_collapse_lower_half(self) -> None:
        # Regression: jitter is applied upward from the 1.0 floor, not as a
        # symmetric window that a trailing max(1.0, ...) clamps. base=1.0 used
        # to push the [0.75, 1.0) half onto exactly 1.0, destroying spread.
        policy = _fixed([1], jitter=True)
        samples = [calculate_retry_delay(1, policy) for _ in range(200)]
        assert all(d >= 1.0 for d in samples)
        assert all(d <= 1.25 + 1e-9 for d in samples)
        assert any(d > 1.0 for d in samples)
        assert len({round(d, 4) for d in samples}) > 5


_VALID_POLICY: dict[str, object] = {
    'max_retries': 2,
    'intervals': [5, 10],
    'backoff_strategy': 'fixed',
    'jitter': False,
    'auto_retry_for': ['WORKER_CRASHED'],
}


def _options(policy: object) -> str:
    return json.dumps({'retry_policy': policy})


@pytest.mark.unit
class TestReadStoredRetryPolicy:
    """The stored retry policy is read with the rule of enqueue."""

    @pytest.mark.parametrize(
        'task_options',
        [None, json.dumps({'timeout_ms': 5}), _options(None)],
        ids=['no-options', 'no-key', 'null'],
    )
    def test_no_policy(self, task_options: str | None) -> None:
        assert read_stored_retry_policy(task_options) == NoRetryPolicy()

    def test_an_empty_text_does_not_parse(self) -> None:
        """No writer stores an empty text: it is corrupt data, not "no
        policy". The core's ``read_stored_options`` gives the same reason."""
        assert read_stored_retry_policy('') == OptionsNotAnObject(
            reason='stored task_options do not parse: JSON parse failed: the text is empty',
        )

    def test_a_valid_policy_reads_as_retry_policy(self) -> None:
        assert read_stored_retry_policy(_options(_VALID_POLICY)) == RetryPolicy(
            max_retries=2,
            intervals=[5, 10],
            backoff_strategy='fixed',
            jitter=False,
            auto_retry_for=['WORKER_CRASHED'],
        )

    @pytest.mark.parametrize(
        ('task_options', 'cause'),
        [
            ('{bad json', 'stored task_options do not parse'),
            (json.dumps([1, 2, 3]), 'stored task_options are a JSON list, not an object'),
        ],
        ids=['not-json', 'not-an-object'],
    )
    def test_options_that_are_not_an_object(
        self, task_options: str, cause: str,
    ) -> None:
        match read_stored_retry_policy(task_options):
            case OptionsNotAnObject(reason=reason):
                assert reason.startswith(cause)
            case other:
                raise AssertionError(f'expected OptionsNotAnObject, got {other!r}')

    @pytest.mark.parametrize(
        'policy',
        [
            'not a dict',
            {**_VALID_POLICY, 'intervals': []},
            {**_VALID_POLICY, 'intervals': ['fast', 'slow']},
            {**_VALID_POLICY, 'intervals': [True, False]},
            {**_VALID_POLICY, 'intervals': 'abc'},
            {**_VALID_POLICY, 'backoff_strategy': 'quadratic'},
            {**_VALID_POLICY, 'max_retries': 3},
            {**_VALID_POLICY, 'jitter': 'yes'},
            {**_VALID_POLICY, 'auto_retry_for': []},
            {**_VALID_POLICY, 'extra': 1},
        ],
        ids=[
            'not-an-object', 'empty-intervals', 'string-intervals',
            'bool-intervals', 'intervals-not-a-list', 'unknown-backoff',
            'fixed-length-mismatch', 'jitter-not-bool', 'empty-auto-retry-for',
            'unknown-key',
        ],
    )
    def test_a_policy_that_does_not_validate_is_refused(self, policy: object) -> None:
        """These shapes used to fall back to default intervals or backoff."""
        assert isinstance(read_stored_retry_policy(_options(policy)), InvalidRetryPolicy)


@pytest.mark.unit
class TestAdmittedRetryPolicy:
    """The policy when it retries the code now; the deadline is judged by SQL."""

    _OPTIONS = _options({**_VALID_POLICY, 'auto_retry_for': ['WORKER_CRASHED', 'MY_ERROR']})

    def _admitted(
        self,
        *,
        retry_count: int = 0,
        max_retries: int = 2,
        task_options: str | None = _OPTIONS,
        error_code: str | OperationalErrorCode | None = OperationalErrorCode.WORKER_CRASHED,
    ) -> RetryPolicy | None:
        return admitted_retry_policy(
            retry_count=retry_count,
            max_retries=max_retries,
            task_options_json=task_options,
            error_code=error_code,
        )

    def test_a_named_code_with_retries_left_is_admitted(self) -> None:
        admitted = self._admitted()
        assert isinstance(admitted, RetryPolicy)
        assert admitted.intervals == [5, 10]

    def test_a_named_user_code_is_admitted(self) -> None:
        assert isinstance(self._admitted(error_code='MY_ERROR'), RetryPolicy)

    def test_spent_retries_are_not_admitted(self) -> None:
        assert self._admitted(retry_count=2) is None

    def test_zero_max_retries_is_not_admitted(self) -> None:
        assert self._admitted(max_retries=0) is None

    def test_no_options_are_not_admitted(self) -> None:
        assert self._admitted(task_options=None) is None

    def test_a_code_that_is_not_named_is_not_admitted(self) -> None:
        assert self._admitted(error_code='OTHER') is None

    @pytest.mark.parametrize('code', [None, ''], ids=['none', 'empty'])
    def test_no_code_is_not_admitted(self, code: str | None) -> None:
        assert self._admitted(error_code=code) is None

    def test_a_policy_that_does_not_validate_is_not_admitted(self) -> None:
        """No retry with default intervals: the failure is terminal."""
        options = _options({**_VALID_POLICY, 'intervals': []})
        assert self._admitted(task_options=options) is None
