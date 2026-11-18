"""The worker parent reads a claimed row's stored timeout into one closed union."""

from __future__ import annotations

import json

import pytest

from horsies.core.codec.task_options import serialize_task_options
from horsies.core.models.tasks import (
    MAX_TASK_TIMEOUT_MS,
    MIN_TASK_TIMEOUT_MS,
    TaskOptions,
)
from horsies.core.types.result import is_ok
from horsies.core.worker.stored_timeout import (
    NoTimeout,
    OptionsUnreadable,
    TaskTimeout,
    TimeoutValueInvalid,
    read_claimed_options,
    read_stored_timeout,
)

pytestmark = pytest.mark.unit


def _options(timeout_ms: object) -> str:
    return json.dumps({'retry_policy': None, 'good_until': None, 'timeout_ms': timeout_ms})


class TestNoTimeout:
    @pytest.mark.parametrize('stored', [None, json.dumps({'retry_policy': None})])
    def test_no_options_or_no_key(self, stored: str | None) -> None:
        assert read_stored_timeout(stored) == NoTimeout()

    def test_null_value(self) -> None:
        assert read_stored_timeout(_options(None)) == NoTimeout()


class TestTaskTimeout:
    @pytest.mark.parametrize('ms', [MIN_TASK_TIMEOUT_MS, 5_000, MAX_TASK_TIMEOUT_MS])
    def test_an_int_inside_the_bounds(self, ms: int) -> None:
        assert read_stored_timeout(_options(ms)) == TaskTimeout(ms=ms)

    def test_what_the_definition_writes_is_read_back(self) -> None:
        written = serialize_task_options(
            TaskOptions(task_name='t', timeout_ms=MAX_TASK_TIMEOUT_MS),
        )

        assert is_ok(written)
        assert read_stored_timeout(written.ok_value) == TaskTimeout(ms=MAX_TASK_TIMEOUT_MS)


class TestOptionsUnreadable:
    @pytest.mark.parametrize(
        ('stored', 'reason_start'),
        [
            ('{not json', 'stored task_options do not parse'),
            ('[1, 2]', 'stored task_options are a JSON list, not an object'),
            ('"text"', 'stored task_options are a JSON str, not an object'),
            (b'{}', 'stored task_options are a bytes, not text'),
            ('1' * 5_000, 'stored task_options do not parse'),
        ],
    )
    def test_the_subject_is_task_options(self, stored: object, reason_start: str) -> None:
        result = read_stored_timeout(stored)

        assert isinstance(result, OptionsUnreadable)
        assert result.reason.startswith(reason_start)

    def test_an_empty_text_does_not_parse(self) -> None:
        """No writer stores an empty text: it is corrupt data, not "no
        options". The core's ``read_stored_options`` gives the same reason."""
        refused = OptionsUnreadable(
            reason='stored task_options do not parse: JSON parse failed: the text is empty',
        )

        assert read_stored_timeout('') == refused
        assert read_claimed_options('') == refused


class TestTimeoutValueInvalid:
    @pytest.mark.parametrize(
        ('raw', 'reason'),
        [
            (True, 'stored timeout_ms is a bool, not an int'),
            (1500.0, 'stored timeout_ms is a float, not an int'),
            ('5000', 'stored timeout_ms is a str, not an int'),
            ({'ms': 5}, 'stored timeout_ms is a dict, not an int'),
            (MIN_TASK_TIMEOUT_MS - 1, f'stored timeout_ms is below {MIN_TASK_TIMEOUT_MS} ms'),
            (-5, f'stored timeout_ms is below {MIN_TASK_TIMEOUT_MS} ms'),
            (
                MAX_TASK_TIMEOUT_MS + 1,
                f'stored timeout_ms is above the limit of {MAX_TASK_TIMEOUT_MS} ms '
                '(about 49.7 days)',
            ),
        ],
    )
    def test_the_subject_is_timeout_ms(self, raw: object, reason: str) -> None:
        assert read_stored_timeout(_options(raw)) == TimeoutValueInvalid(reason=reason)

    def test_a_value_past_the_float_range_names_no_digits(self) -> None:
        stored = '{"timeout_ms": 1' + '0' * 400 + '}'

        result = read_stored_timeout(stored)

        assert isinstance(result, TimeoutValueInvalid)
        assert '0000' not in result.reason


_POLICY: dict[str, object] = {
    'max_retries': 2,
    'intervals': [5, 10],
    'backoff_strategy': 'fixed',
    'jitter': False,
    'auto_retry_for': ['E'],
}


def _claimed(policy: object, timeout_ms: object = 1_000) -> str:
    return json.dumps({'retry_policy': policy, 'timeout_ms': timeout_ms})


class TestClaimedOptions:
    """The claim refuses a stored retry policy that does not validate, so the
    task fails before it runs; then it reads the timeout."""

    @pytest.mark.parametrize('policy', [None, _POLICY], ids=['no-policy', 'valid-policy'])
    def test_a_usable_policy_gives_the_timeout(self, policy: object) -> None:
        assert read_claimed_options(_claimed(policy)) == TaskTimeout(ms=1_000)

    def test_no_options_have_no_timeout(self) -> None:
        assert read_claimed_options(None) == NoTimeout()

    @pytest.mark.parametrize(
        'policy',
        [
            'not a dict',
            {**_POLICY, 'intervals': []},
            {**_POLICY, 'backoff_strategy': 'quadratic'},
            {**_POLICY, 'max_retries': 3},
        ],
        ids=['not-an-object', 'empty-intervals', 'unknown-backoff', 'length-mismatch'],
    )
    def test_a_policy_that_does_not_validate_is_refused(self, policy: object) -> None:
        """Before, the retry of such a task used default intervals or backoff."""
        match read_claimed_options(_claimed(policy)):
            case OptionsUnreadable(reason=reason):
                assert reason.startswith('stored retry_policy does not validate: ')
            case other:
                raise AssertionError(f'expected OptionsUnreadable, got {other!r}')

    def test_the_policy_is_refused_before_the_timeout(self) -> None:
        refused = read_claimed_options(_claimed({**_POLICY, 'intervals': []}, timeout_ms=-1))
        assert isinstance(refused, OptionsUnreadable)

    def test_options_that_do_not_parse_keep_the_timeout_reason(self) -> None:
        assert read_claimed_options('{bad') == read_stored_timeout('{bad')
