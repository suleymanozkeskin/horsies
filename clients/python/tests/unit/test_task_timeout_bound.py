"""`timeout_ms` on a task is bounded where the task is defined."""

from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from horsies.core.app import Horsies
from horsies.core.errors import ErrorCode, TaskDefinitionError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    MAX_TASK_TIMEOUT_MS,
    MIN_TASK_TIMEOUT_MS,
    TaskError,
    TaskOptions,
)
from horsies.core.types.result import Ok, Result

pytestmark = pytest.mark.unit


def _app() -> Horsies:
    return Horsies(
        AppConfig(
            database=PostgresConfig(
                database_url=SecretStr('postgresql://user:pass@127.0.0.1:1/none'),
            ),
        ),
    )


class TestTaskOptions:
    @pytest.mark.parametrize('ms', [MIN_TASK_TIMEOUT_MS, MAX_TASK_TIMEOUT_MS])
    def test_the_bounds_are_accepted(self, ms: int) -> None:
        assert TaskOptions(task_name='t', timeout_ms=ms).timeout_ms == ms

    @pytest.mark.parametrize(
        'ms',
        [
            pytest.param(MIN_TASK_TIMEOUT_MS - 1, id='min-1'),
            pytest.param(MAX_TASK_TIMEOUT_MS + 1, id='limit+1'),
            pytest.param(10**400, id='401-digits'),
        ],
    )
    def test_outside_the_bounds_is_refused(self, ms: int) -> None:
        with pytest.raises(ValidationError):
            TaskOptions(task_name='t', timeout_ms=ms)


class TestTaskDecorator:
    def test_the_limit_registers(self) -> None:
        app = _app()

        @app.task('at_the_limit', timeout_ms=MAX_TASK_TIMEOUT_MS)
        def at_the_limit(*, n: int) -> Result[int, TaskError]:
            return Ok(n)

        assert 'at_the_limit' in app.tasks
        assert at_the_limit is app.tasks['at_the_limit']

    @pytest.mark.parametrize(
        'ms',
        [
            pytest.param(MAX_TASK_TIMEOUT_MS + 1, id='limit+1'),
            pytest.param(10**5000, id='5001-digits'),
        ],
    )
    def test_above_the_limit_is_a_definition_error(self, ms: int) -> None:
        app = _app()

        with pytest.raises(TaskDefinitionError) as raised:

            @app.task('above_the_limit', timeout_ms=ms)
            def above_the_limit(*, n: int) -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
                return Ok(n)

        assert raised.value.code is ErrorCode.TASK_INVALID_OPTIONS
        assert str(MAX_TASK_TIMEOUT_MS) in str(raised.value)
