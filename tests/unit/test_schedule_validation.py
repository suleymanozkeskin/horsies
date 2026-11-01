"""Tests for schedule validation shared by scheduler boot and app.check().

Covers ``prepare_schedules`` and ``collect_schedule_errors`` orchestration
(failure modes, per-schedule first-error, multi-schedule collection, disabled
skip) and the ``app.check()`` integration including the worker-role gate.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel, field_validator

from horsies.core.app import Horsies
from horsies.core.codec.json_io import dumps_json
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    RegistryError,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.schedule import (
    IntervalSchedule,
    ScheduleConfig,
    TaskSchedule,
)
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.scheduler.validation import (
    collect_schedule_errors,
    prepare_schedules,
)
from horsies.core.types.result import Err, Ok


class _EchoingModel(BaseModel):
    """A validator that writes the refused value in its error text."""

    name: str

    @field_validator('name')
    @classmethod
    def refuse(cls, value: str) -> str:
        raise ValueError(f'bad name {value}')


class _Linked(BaseModel):
    """A self-referencing model: registration accepts it in a task signature."""

    next: _Linked | None = None


def _make_mock_app(tasks: dict[str, MagicMock] | None = None) -> MagicMock:
    """Build a minimal mock app for collect_schedule_errors."""
    app = MagicMock()
    app.tasks = tasks or {}
    app.config.payload = PayloadPolicy()
    app.validate_queue_name = MagicMock(return_value='default')
    return app


def _make_task_mock(fn: object) -> MagicMock:
    """Mock task object exposing _original_fn and a default queue."""
    task = MagicMock()
    task._original_fn = fn
    task.task_queue_name = None
    return task


def _schedule(
    *,
    name: str = 's',
    task_name: str = 'task_a',
    timezone: str = 'UTC',
    enabled: bool = True,
    kwargs: dict[str, object] | None = None,
    queue_name: str | None = None,
) -> TaskSchedule:
    return TaskSchedule(
        name=name,
        task_name=task_name,
        pattern=IntervalSchedule(seconds=5),
        timezone=timezone,
        enabled=enabled,
        kwargs=kwargs or {},
        queue_name=queue_name,
    )


# =============================================================================
# prepare_schedules — the one pass
# =============================================================================


@pytest.mark.unit
class TestPrepareSchedules:
    """The pass that boot keeps and check reports from."""

    def test_ok_holds_the_text_of_each_enabled_schedule(self) -> None:
        """A schedule without kwargs has None; a disabled one has no entry."""
        def fn(x: int = 0) -> str:
            return str(x)

        config = ScheduleConfig(
            schedules=[
                _schedule(name='with_kwargs', kwargs={'x': 1}),
                _schedule(name='without_kwargs'),
                _schedule(name='disabled', kwargs={'x': 2}, enabled=False),
            ],
        )
        app = _make_mock_app(tasks={'task_a': _make_task_mock(fn)})

        result = prepare_schedules(app, config)

        assert result == Ok(
            {
                'with_kwargs': dumps_json({'x': 1}).ok_value,
                'without_kwargs': None,
            },
        )

    def test_the_text_cannot_be_replaced_after_boot(self) -> None:
        def fn() -> str:
            return 'ok'

        config = ScheduleConfig(schedules=[_schedule()])
        app = _make_mock_app(tasks={'task_a': _make_task_mock(fn)})

        encoded_kwargs = prepare_schedules(app, config).ok_value

        with pytest.raises(TypeError):
            encoded_kwargs['s'] = 'other'  # type: ignore[index]

    @pytest.mark.parametrize(
        'value',
        [
            {'key': f'text{chr(0xd800)}'},
            {f'key{chr(0xd800)}': 'text'},
        ],
        ids=['string', 'key'],
    )
    def test_a_lone_surrogate_in_kwargs_is_refused_at_boot(
        self, value: dict[str, str],
    ) -> None:
        def fn(x: dict[str, str]) -> str:
            return str(x)

        config = ScheduleConfig(schedules=[_schedule(kwargs={'x': value})])
        app = _make_mock_app(tasks={'task_a': _make_task_mock(fn)})

        result = prepare_schedules(app, config)

        match result:
            case Err(errors):
                assert len(errors) == 1
                assert isinstance(errors[0], ConfigurationError)
                assert errors[0].code == ErrorCode.CONFIG_INVALID_SCHEDULE
                assert 'not encodable' in errors[0].message
                assert 'lone surrogate' in ' '.join(errors[0].notes)
            case Ok(encoded_kwargs):
                raise AssertionError(f'expected a refusal, got {encoded_kwargs!r}')

    def test_a_validator_that_echoes_a_lone_surrogate_is_refused_at_boot(
        self,
    ) -> None:
        def fn(x: _EchoingModel) -> str:
            return x.name

        config = ScheduleConfig(
            schedules=[_schedule(kwargs={'x': {'name': f'text{chr(0xd800)}'}})],
        )
        app = _make_mock_app(tasks={'task_a': _make_task_mock(fn)})

        result = prepare_schedules(app, config)

        match result:
            case Err(errors):
                assert len(errors) == 1
                assert isinstance(errors[0], ConfigurationError)
                assert errors[0].code == ErrorCode.CONFIG_INVALID_SCHEDULE
                assert 'not encodable' in errors[0].message
                assert (
                    'x: a validation error holds a lone surrogate'
                    in ' '.join(errors[0].notes)
                )
            case Ok(encoded_kwargs):
                raise AssertionError(f'expected a refusal, got {encoded_kwargs!r}')

    def test_a_value_that_holds_itself_is_refused_at_boot(self) -> None:
        def fn(x: _Linked) -> str:
            return str(x)

        node = _Linked()
        node.next = node
        config = ScheduleConfig(schedules=[_schedule(kwargs={'x': node})])
        app = _make_mock_app(tasks={'task_a': _make_task_mock(fn)})

        result = prepare_schedules(app, config)

        match result:
            case Err(errors):
                assert len(errors) == 1
                assert isinstance(errors[0], ConfigurationError)
                assert errors[0].code == ErrorCode.CONFIG_INVALID_SCHEDULE
                assert 'not encodable' in errors[0].message
                assert (
                    'x: serialization failed: Circular reference detected'
                    in ' '.join(errors[0].notes)
                )
            case Ok(encoded_kwargs):
                raise AssertionError(f'expected a refusal, got {encoded_kwargs!r}')

    def test_err_holds_one_error_per_bad_schedule_in_config_order(self) -> None:
        def fn() -> str:
            return 'ok'

        config = ScheduleConfig(
            schedules=[
                _schedule(name='bad_timezone', timezone='Not/AZone'),
                _schedule(name='good'),
                _schedule(name='bad_task', task_name='missing'),
            ],
        )
        app = _make_mock_app(tasks={'task_a': _make_task_mock(fn)})

        result = prepare_schedules(app, config)

        match result:
            case Err(errors):
                assert [type(error) for error in errors] == [
                    ConfigurationError,
                    RegistryError,
                ]
                assert 'invalid timezone' in errors[0].message
                assert 'not registered' in errors[1].message
            case Ok(encoded_kwargs):
                raise AssertionError(f'expected errors, got {encoded_kwargs!r}')

    def test_a_disabled_schedule_over_the_limit_passes(self) -> None:
        def fn(*, blob: str) -> None:
            return None

        config = ScheduleConfig(
            schedules=[_schedule(kwargs={'blob': 'x' * 2048}, enabled=False)],
        )
        app = _make_mock_app(tasks={'task_a': _make_task_mock(fn)})
        app.config.payload = PayloadPolicy(warn_bytes=None, reject_bytes=256)

        assert collect_schedule_errors(app, config) == []


# =============================================================================
# collect_schedule_errors — failure modes
# =============================================================================


@pytest.mark.unit
class TestCollectScheduleErrors:
    """Per-schedule checks via the shared collector."""

    def _collect(
        self,
        schedules: list[TaskSchedule],
        tasks: dict[str, MagicMock] | None = None,
        app: MagicMock | None = None,
    ) -> list[HorsiesError]:
        config = ScheduleConfig(schedules=schedules)
        app = app or _make_mock_app(tasks=tasks)
        return collect_schedule_errors(app, config)

    def test_all_valid_returns_empty(self) -> None:
        """A valid kwargs-only schedule produces no errors."""
        def fn(x: int) -> str:
            return str(x)

        errors = self._collect(
            [_schedule(kwargs={'x': 1})],
            tasks={'task_a': _make_task_mock(fn)},
        )
        assert errors == []

    def test_invalid_timezone(self) -> None:
        """Unknown IANA timezone is rejected."""
        def fn() -> str:
            return 'ok'

        errors = self._collect(
            [_schedule(timezone='Not/AZone')],
            tasks={'task_a': _make_task_mock(fn)},
        )
        assert len(errors) == 1
        assert isinstance(errors[0], ConfigurationError)
        assert errors[0].code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'invalid timezone' in errors[0].message

    def test_unregistered_task(self) -> None:
        """Schedule referencing an unknown task is rejected."""
        errors = self._collect([_schedule(task_name='missing')], tasks={})
        assert len(errors) == 1
        assert isinstance(errors[0], RegistryError)
        assert errors[0].code == ErrorCode.TASK_NOT_REGISTERED

    def test_invalid_queue(self) -> None:
        """A queue that fails validate_queue_name is rejected."""
        def fn() -> str:
            return 'ok'

        app = _make_mock_app(tasks={'task_a': _make_task_mock(fn)})
        app.validate_queue_name = MagicMock(side_effect=ValueError('bad queue'))

        errors = self._collect([_schedule()], app=app)
        assert len(errors) == 1
        assert isinstance(errors[0], ConfigurationError)
        assert errors[0].code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'invalid queue configuration' in errors[0].message

    def test_signature_mismatch(self) -> None:
        """Kwargs that do not bind to the task signature are rejected."""
        def fn(x: int) -> str:
            return str(x)

        errors = self._collect(
            [_schedule(kwargs={'wrong': 1})],
            tasks={'task_a': _make_task_mock(fn)},
        )
        assert len(errors) == 1
        assert 'kwargs do not match' in errors[0].message

    def test_kwarg_type_mismatch_not_encodable(self) -> None:
        """A kwarg that binds by name but violates the param type is rejected.

        `encode_value` validates before serializing, so a dict for a `str`
        param fails the wire-encode dry run instead of slipping through.
        """
        def fn(region: str) -> str:
            return region

        errors = self._collect(
            [_schedule(kwargs={'region': {'not': 'a string'}})],
            tasks={'task_a': _make_task_mock(fn)},
        )
        assert len(errors) == 1
        assert isinstance(errors[0], ConfigurationError)
        assert errors[0].code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'not encodable' in errors[0].message

    def test_first_error_wins_per_schedule(self) -> None:
        """Within one schedule, timezone is checked before task registration."""
        # Bad timezone AND unregistered task: timezone error reported (checked first).
        errors = self._collect(
            [_schedule(timezone='Not/AZone', task_name='missing')],
            tasks={},
        )
        assert len(errors) == 1
        assert 'invalid timezone' in errors[0].message

    def test_disabled_schedule_skipped(self) -> None:
        """A disabled schedule is not validated even if invalid."""
        errors = self._collect(
            [_schedule(enabled=False, timezone='Not/AZone')],
            tasks={},
        )
        assert errors == []

    def test_collects_one_error_per_bad_schedule(self) -> None:
        """Multiple bad schedules each contribute one error."""
        errors = self._collect(
            [
                _schedule(name='a', timezone='Not/AZone'),
                _schedule(name='b', task_name='missing'),
            ],
            tasks={},
        )
        assert len(errors) == 2
        messages = ' '.join(e.message for e in errors)
        assert 'invalid timezone' in messages
        assert 'not registered' in messages


# =============================================================================
# app.check() integration — role gate
# =============================================================================


def _make_real_app(schedule: ScheduleConfig | None) -> Horsies:
    return Horsies(
        config=AppConfig(
            broker=PostgresConfig(
                database_url='postgresql+psycopg://user:pass@localhost/db',
            ),
            schedule=schedule,
        )
    )


@pytest.mark.unit
class TestCheckValidatesSchedules:
    """app.check() surfaces schedule errors, gated by role."""

    def _valid_schedule_app(self) -> Horsies:
        config = ScheduleConfig(
            schedules=[
                _schedule(task_name='my_task', kwargs={'x': 1}),
            ],
        )
        app = _make_real_app(config)

        @app.task('my_task')
        def my_task(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        return app

    def _bad_schedule_app(self) -> Horsies:
        config = ScheduleConfig(
            schedules=[
                _schedule(task_name='my_task', timezone='Not/AZone'),
            ],
        )
        app = _make_real_app(config)

        @app.task('my_task')
        def my_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        return app

    def test_valid_schedule_passes_check(self) -> None:
        """A valid schedule yields no check errors."""
        app = self._valid_schedule_app()
        assert app.check() == []

    def test_bad_schedule_surfaced_by_check(self) -> None:
        """check() (producer role) reports the invalid schedule."""
        app = self._bad_schedule_app()
        errors = app.check()
        assert len(errors) == 1
        assert errors[0].code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'invalid timezone' in errors[0].message

    def test_schedule_over_the_limit_surfaced_by_check(self) -> None:
        """check() reports kwargs over payload.reject_bytes before a deploy."""
        schedule = _schedule(task_name='my_task', kwargs={'blob': 'x' * 2048})
        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://user:pass@localhost/db',
                ),
                schedule=ScheduleConfig(schedules=[schedule]),
                payload=PayloadPolicy(warn_bytes=None, reject_bytes=256),
            ),
        )

        @app.task('my_task')
        def my_task(*, blob: str) -> TaskResult[int, TaskError]:
            return TaskResult(ok=len(blob))

        errors = app.check()

        assert len(errors) == 1
        assert errors[0].code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'payload.reject_bytes' in errors[0].message

    def test_worker_role_skips_schedule_validation(self) -> None:
        """Workers never act on schedules; check() skips them."""
        app = self._bad_schedule_app()
        app.set_role('worker')
        assert app.check() == []

    def test_no_schedule_config_returns_empty(self) -> None:
        """An app without schedule config produces no schedule errors."""
        app = _make_real_app(None)
        assert app.check() == []
