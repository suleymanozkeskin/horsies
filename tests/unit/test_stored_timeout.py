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
    read_stored_timeout,
)

pytestmark = pytest.mark.unit


def _options(timeout_ms: object) -> str:
    return json.dumps({'retry_policy': None, 'good_until': None, 'timeout_ms': timeout_ms})


class TestNoTimeout:
    @pytest.mark.parametrize('stored', [None, '', json.dumps({'retry_policy': None})])
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
