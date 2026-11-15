"""The retry policy of a failed attempt: ``retry_request_for``.

The policy reads the facts of the unlocked read (``RunningAttempt``) and the
error code, and asks for a retry with its delay, or for none. The deadline is
judged by ``horsies_finish_failed_attempt`` under the lock (integration
tests). Also the full chain: resolve_exception_error_code() -> the code ->
retry_request_for().
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from horsies.core.exception_mapper import ExceptionMapper, resolve_exception_error_code
from horsies.core.models.tasks import OperationalErrorCode
from horsies.core.utils.retry import calculate_retry_delay
from horsies.core.worker.failed_attempt import (
    NoRetry,
    RetryAfter,
    RetryRequest,
    RunningAttempt,
    retry_request_for,
)
from horsies.core.worker.shared_stop import RetryPlacement


def _task_options_json(
    auto_retry_for: list[str],
    *,
    intervals: list[int] | None = None,
    backoff_strategy: str | None = None,
    jitter: bool | None = None,
) -> str:
    """Build a valid task_options JSON with retry_policy.auto_retry_for."""
    retry_policy: dict[str, object] = {
        'auto_retry_for': auto_retry_for,
    }
    if intervals is not None:
        retry_policy['intervals'] = intervals
    if backoff_strategy is not None:
        retry_policy['backoff_strategy'] = backoff_strategy
    if jitter is not None:
        retry_policy['jitter'] = jitter
    return json.dumps(
        {
            'retry_policy': retry_policy,
        }
    )


def _decide(
    *,
    retry_count: int,
    max_retries: int,
    task_options: str | None,
    code: str | None,
    placement: RetryPlacement = RetryPlacement.SAME_CLASS,
) -> RetryRequest:
    return retry_request_for(
        RunningAttempt(
            retry_count=retry_count,
            max_retries=max_retries,
            task_options=task_options,
        ),
        code,
        placement,
    )


def _retries(request: RetryRequest) -> bool:
    match request:
        case RetryAfter():
            return True
        case NoRetry():
            return False


@pytest.mark.unit
class TestRetryRequestFor:
    """The policy decision on the facts of the unlocked read."""

    def test_matching_error_code_asks_for_a_retry(self) -> None:
        request = _decide(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(['RATE_LIMITED']),
            code='RATE_LIMITED',
        )
        assert _retries(request)

    def test_non_matching_error_code_asks_for_none(self) -> None:
        request = _decide(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(['RATE_LIMITED']),
            code='TIMEOUT',
        )
        assert request == NoRetry()

    def test_retries_exhausted_asks_for_none(self) -> None:
        request = _decide(
            retry_count=3,
            max_retries=3,
            task_options=_task_options_json(['RATE_LIMITED']),
            code='RATE_LIMITED',
        )
        assert request == NoRetry()

    def test_max_retries_zero_asks_for_none(self) -> None:
        request = _decide(
            retry_count=0,
            max_retries=0,
            task_options=_task_options_json(['RATE_LIMITED']),
            code='RATE_LIMITED',
        )
        assert request == NoRetry()

    def test_library_error_code_value_matches(self) -> None:
        """The callers pass the stored code text of an OperationalErrorCode."""
        request = _decide(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(['UNHANDLED_EXCEPTION']),
            code=OperationalErrorCode.UNHANDLED_EXCEPTION.value,
        )
        assert _retries(request)

    @pytest.mark.parametrize(
        'task_options',
        [
            None,
            'NOT_JSON{',
            '"just_a_string"',
            json.dumps({'retry_policy': 'string'}),
            _task_options_json([]),
            json.dumps({'retry_policy': {'backoff_strategy': 'fixed'}}),
        ],
        ids=[
            'no_options',
            'malformed_json',
            'options_not_a_dict',
            'policy_not_a_dict',
            'empty_auto_retry_for',
            'auto_retry_for_missing',
        ],
    )
    def test_options_without_a_usable_policy_ask_for_none(
        self,
        task_options: str | None,
    ) -> None:
        request = _decide(
            retry_count=0,
            max_retries=3,
            task_options=task_options,
            code='RATE_LIMITED',
        )
        assert request == NoRetry()

    @pytest.mark.parametrize('code', ['', None], ids=['empty', 'none'])
    def test_an_empty_error_code_asks_for_none(self, code: str | None) -> None:
        request = _decide(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(['RATE_LIMITED']),
            code=code,
        )
        assert request == NoRetry()

    def test_multiple_codes_match_any(self) -> None:
        request = _decide(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(['A_CODE', 'B_CODE', 'C_CODE']),
            code='B_CODE',
        )
        assert _retries(request)

    def test_the_request_carries_the_count_it_read_and_the_next_delay(self) -> None:
        """The delay belongs to the next attempt: retry_count + 1."""
        request = _decide(
            retry_count=1,
            max_retries=3,
            task_options=_task_options_json(
                ['RATE_LIMITED'],
                intervals=[10, 20, 30],
                backoff_strategy='fixed',
                jitter=False,
            ),
            code='RATE_LIMITED',
            placement=RetryPlacement.ISOLATED,
        )
        assert request == RetryAfter(
            retry_count=1,
            delay_s=20.0,
            placement=RetryPlacement.ISOLATED,
        )

    def test_a_policy_without_intervals_uses_the_default_intervals(self) -> None:
        request = _decide(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(['RATE_LIMITED'], jitter=False),
            code='RATE_LIMITED',
        )
        assert request == RetryAfter(
            retry_count=0,
            delay_s=60.0,
            placement=RetryPlacement.SAME_CLASS,
        )


def _resolved_code(
    exc: Exception,
    *,
    task_mapper: ExceptionMapper | None,
    global_mapper: ExceptionMapper | None,
    task_default: str | None,
    global_default: str,
) -> str:
    code = resolve_exception_error_code(
        exc,
        task_mapper=task_mapper,
        global_mapper=global_mapper,
        task_default=task_default,
        global_default=global_default,
    )
    assert isinstance(code, str)
    return code


@pytest.mark.unit
class TestRetryDecisionChain:
    """Full pipeline: resolve_exception_error_code() -> retry_request_for()."""

    @pytest.mark.parametrize(
        (
            'task_mapper',
            'global_mapper',
            'task_default',
            'listed',
            'expected_code',
            'retries',
        ),
        [
            ({ValueError: 'VAL_ERROR'}, None, None, 'VAL_ERROR', 'VAL_ERROR', True),
            (None, {ValueError: 'VAL_ERROR'}, None, 'VAL_ERROR', 'VAL_ERROR', True),
            (
                {ValueError: 'TASK_CODE'},
                {ValueError: 'GLOBAL_CODE'},
                None,
                'GLOBAL_CODE',
                'TASK_CODE',
                False,
            ),
            (
                {ValueError: 'TASK_CODE'},
                {ValueError: 'GLOBAL_CODE'},
                None,
                'TASK_CODE',
                'TASK_CODE',
                True,
            ),
        ],
        ids=[
            'task_mapper_code_triggers_retry',
            'global_mapper_code_triggers_retry',
            'task_mapper_wins_over_global_for_retry',
            'task_mapper_wins_over_global_positive',
        ],
    )
    def test_mapped_code_decides(
        self,
        task_mapper: ExceptionMapper | None,
        global_mapper: ExceptionMapper | None,
        task_default: str | None,
        listed: str,
        expected_code: str,
        retries: bool,
    ) -> None:
        code = _resolved_code(
            ValueError('bad value'),
            task_mapper=task_mapper,
            global_mapper=global_mapper,
            task_default=task_default,
            global_default='UNHANDLED',
        )
        request = _decide(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json([listed]),
            code=code,
        )
        assert code == expected_code
        assert _retries(request) is retries

    @pytest.mark.parametrize(
        ('task_default', 'listed', 'expected_code', 'retries'),
        [
            (None, 'UNHANDLED', 'UNHANDLED', True),
            ('TASK_DEFAULT', 'GLOBAL_DEFAULT', 'TASK_DEFAULT', False),
        ],
        ids=[
            'unmapped_exception_uses_global_default',
            'task_default_wins_over_global_default',
        ],
    )
    def test_default_code_decides(
        self,
        task_default: str | None,
        listed: str,
        expected_code: str,
        retries: bool,
    ) -> None:
        code = _resolved_code(
            RuntimeError('unexpected'),
            task_mapper=None,
            global_mapper=None,
            task_default=task_default,
            global_default=('UNHANDLED' if task_default is None else 'GLOBAL_DEFAULT'),
        )
        request = _decide(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json([listed]),
            code=code,
        )
        assert code == expected_code
        assert _retries(request) is retries


@pytest.mark.unit
class TestCalculateRetryDelay:
    """calculate_retry_delay, the delay that a RetryAfter carries."""

    def test_exponential_delays_strictly_increase_with_jitter(self) -> None:
        """Exponential backoff delays must strictly increase across attempts, even with jitter."""
        policy_data = {
            'intervals': [1],
            'backoff_strategy': 'exponential',
            'jitter': True,
        }
        # Run many iterations to catch jitter-induced collisions
        for _ in range(200):
            delay_1 = calculate_retry_delay(1, policy_data)
            delay_2 = calculate_retry_delay(2, policy_data)
            delay_3 = calculate_retry_delay(3, policy_data)
            assert delay_2 > delay_1, f'delay_2={delay_2} must be > delay_1={delay_1}'
            assert delay_3 > delay_2, f'delay_3={delay_3} must be > delay_2={delay_2}'

    def test_exponential_delay_returns_float(self) -> None:
        """Delay must be float to preserve sub-second precision from jitter."""
        policy_data = {
            'intervals': [1],
            'backoff_strategy': 'exponential',
            'jitter': True,
        }
        delay = calculate_retry_delay(1, policy_data)
        assert isinstance(delay, float)

    def test_fixed_delay_returns_float(self) -> None:
        """Fixed strategy also returns float for type consistency."""
        policy_data = {
            'intervals': [5, 10],
            'backoff_strategy': 'fixed',
            'jitter': False,
        }
        delay = calculate_retry_delay(1, policy_data)
        assert isinstance(delay, float)

    def test_minimum_delay_is_one_second(self) -> None:
        """Delay must never drop below 1.0 second."""
        policy_data = {
            'intervals': [1],
            'backoff_strategy': 'exponential',
            'jitter': True,
        }
        for _ in range(200):
            delay = calculate_retry_delay(1, policy_data)
            assert delay >= 1.0, f'delay={delay} is below 1.0s minimum'

    # --- U-1a: fixed strategy picks intervals[index] ---

    def test_fixed_first_attempt_uses_first_interval(self) -> None:
        """Fixed strategy, attempt=1 → intervals[0]."""
        policy_data = {
            'intervals': [10, 30, 90],
            'backoff_strategy': 'fixed',
            'jitter': False,
        }
        delay = calculate_retry_delay(1, policy_data)
        assert delay == 10.0

    def test_fixed_second_attempt_uses_second_interval(self) -> None:
        """Fixed strategy, attempt=2 → intervals[1]."""
        policy_data = {
            'intervals': [10, 30, 90],
            'backoff_strategy': 'fixed',
            'jitter': False,
        }
        delay = calculate_retry_delay(2, policy_data)
        assert delay == 30.0

    # --- U-1b: fixed strategy clamps out-of-bounds attempts ---

    def test_fixed_attempt_exceeds_intervals_clamps_to_last(self) -> None:
        """Fixed strategy, attempt > len(intervals) → clamped to last interval."""
        policy_data = {
            'intervals': [10, 30],
            'backoff_strategy': 'fixed',
            'jitter': False,
        }
        delay = calculate_retry_delay(5, policy_data)
        assert delay == 30.0

    # --- U-1c: exponential exact values ---

    def test_exponential_exact_values_without_jitter(self) -> None:
        """Exponential strategy: base * 2^(attempt-1) with jitter off."""
        policy_data = {
            'intervals': [10],
            'backoff_strategy': 'exponential',
            'jitter': False,
        }
        assert calculate_retry_delay(1, policy_data) == 10.0
        assert calculate_retry_delay(2, policy_data) == 20.0
        assert calculate_retry_delay(3, policy_data) == 40.0
        assert calculate_retry_delay(4, policy_data) == 80.0

    # --- U-1d: unknown strategy → fallback ---

    def test_unknown_strategy_falls_back_to_first_interval(self) -> None:
        """Unknown backoff_strategy uses intervals[0] as fallback."""
        policy_data = {
            'intervals': [42],
            'backoff_strategy': 'nonexistent_strategy',
            'jitter': False,
        }
        delay = calculate_retry_delay(1, policy_data)
        assert delay == 42.0

    # --- U-1e: jitter band ±25% ---

    def test_jitter_stays_within_25_percent_band(self) -> None:
        """With jitter=True, delay stays within ±25% of base_delay."""
        policy_data = {
            'intervals': [100],
            'backoff_strategy': 'fixed',
            'jitter': True,
        }
        for _ in range(300):
            delay = calculate_retry_delay(1, policy_data)
            assert (
                75.0 <= delay <= 125.0
            ), f'delay={delay} outside ±25% band of base=100'

    # --- U-1f: jitter=False → deterministic ---

    def test_jitter_disabled_gives_exact_delay(self) -> None:
        """With jitter=False, delay equals base_delay exactly."""
        policy_data = {
            'intervals': [60, 300, 900],
            'backoff_strategy': 'fixed',
            'jitter': False,
        }
        # Repeated calls must all return the same value
        for attempt, expected in [(1, 60.0), (2, 300.0), (3, 900.0)]:
            delay = calculate_retry_delay(attempt, policy_data)
            assert (
                delay == expected
            ), f'attempt={attempt}: expected {expected}, got {delay}'

    # --- U-1g: empty intervals → fallback 60 ---

    def test_exponential_empty_intervals_falls_back_to_60(self) -> None:
        """Exponential with empty intervals list uses 60 as base."""
        policy_data: dict[str, Any] = {
            'intervals': [],
            'backoff_strategy': 'exponential',
            'jitter': False,
        }
        assert calculate_retry_delay(1, policy_data) == 60.0
        assert calculate_retry_delay(2, policy_data) == 120.0

    def test_unknown_strategy_empty_intervals_falls_back_to_60(self) -> None:
        """Unknown strategy with empty intervals list uses 60 as fallback."""
        policy_data: dict[str, Any] = {
            'intervals': [],
            'backoff_strategy': 'bogus',
            'jitter': False,
        }
        delay = calculate_retry_delay(1, policy_data)
        assert delay == 60.0

    # --- U-1h: floor at 1.0 with small base and negative jitter ---

    def test_floor_at_one_second_with_tiny_base_and_jitter(self) -> None:
        """Even with a tiny base interval and worst-case jitter, floor is 1.0."""
        policy_data = {
            'intervals': [1],
            'backoff_strategy': 'fixed',
            'jitter': True,
        }
        for _ in range(300):
            delay = calculate_retry_delay(1, policy_data)
            assert delay >= 1.0, f'delay={delay} below 1.0s floor'
