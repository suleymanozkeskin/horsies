"""Tests for schedule validation shared by scheduler boot and app.check().

Covers ``collect_schedule_errors`` orchestration (failure modes, per-schedule
first-error, multi-schedule collection, disabled skip) and the ``app.check()``
integration including the worker-role gate.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from horsies.core.app import Horsies
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    RegistryError,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.schedule import (
    IntervalSchedule,
    ScheduleConfig,
    TaskSchedule,
)
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.scheduler.validation import collect_schedule_errors


def _make_mock_app(tasks: dict[str, MagicMock] | None = None) -> MagicMock:
    """Build a minimal mock app for collect_schedule_errors."""
    app = MagicMock()
    app.tasks = tasks or {}
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

    def test_worker_role_skips_schedule_validation(self) -> None:
        """Workers never act on schedules; check() skips them."""
        app = self._bad_schedule_app()
        app.set_role('worker')
        assert app.check() == []

    def test_no_schedule_config_returns_empty(self) -> None:
        """An app without schedule config produces no schedule errors."""
        app = _make_real_app(None)
        assert app.check() == []
