# pyright: reportPrivateUsage=false
"""Tests of the client parts of the scheduler (mock-based, no DB).

The schedule checks and the kwargs encode of ``scheduler/validation.py``, and
the import of the task modules and the schedule preparation of the facade
``Scheduler`` (``horsies.core.scheduler.service``). The facade's core run is
tested in ``test_scheduler_facade.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from horsies.core.codec.json_io import Json, SerializationError, dumps_json
from horsies.core.errors import ErrorCode
from horsies.core.models.schedule import (
    IntervalSchedule,
    ScheduleConfig,
    TaskSchedule,
)
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.outcome import Succeeded, TaskOutcome
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import WorkflowContext
from horsies.core.scheduler.service import Scheduler
from horsies.core.scheduler.validation import (
    _encode_schedule_kwargs,
    _signature_error,
)
from horsies.core.types.result import Err, Ok, is_err, is_ok


def _json_text(value: Json) -> str:
    """The JSON text of a value that the strict encoder takes."""
    match dumps_json(value):
        case Ok(text):
            return text
        case Err(error):
            raise AssertionError(f'value does not encode: {error}')


def _make_app(
    schedule_config: ScheduleConfig | None = None,
    tasks: dict[str, MagicMock] | None = None,
) -> MagicMock:
    """Build a minimal mock Horsies app for Scheduler tests.

    ``config.payload`` is a REAL PayloadPolicy: the payload guardrail
    compares sizes against it, and int-vs-MagicMock comparison raises.
    """
    app = MagicMock()
    app.config.schedule = schedule_config
    app.config.payload = PayloadPolicy()
    app.tasks = tasks or {}
    # Default validate_queue_name returns 'default'
    app.validate_queue_name = MagicMock(return_value='default')
    # Default queue mode so effective_priority returns 100
    app.config.queue_mode.name = 'DEFAULT'
    return app


def _make_task_mock(fn: object) -> MagicMock:
    """Create a mock task object with _original_fn pointing to a real function.

    The task has no options: ``task_options_json`` is None.
    """
    task = MagicMock()
    task._original_fn = fn
    task.task_queue_name = None
    task.task_options_json = None
    return task


# =============================================================================
# validation._signature_error
# =============================================================================


@pytest.mark.unit
class TestSignatureError:
    """Tests for validation._signature_error (isolated signature bind).

    Args tolerance is exercised here in isolation; the integrated
    collect_schedule_errors path rejects positional args upstream (see
    test_schedule_validation.py).
    """

    def _make_app_with_task(self, task: MagicMock) -> MagicMock:
        """Build a mock app exposing the given task as 'task_a'."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='task_a',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        return _make_app(schedule_config=config, tasks={'task_a': task})

    def test_no_required_params_ok(self) -> None:
        """Function with no required params passes validation."""
        def no_params() -> str:
            return 'ok'

        app = self._make_app_with_task(_make_task_mock(no_params))
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
        )

        assert _signature_error(app, schedule) is None

    def test_missing_required_returns_configuration_error(self) -> None:
        """Missing required param returns ConfigurationError HRS-205."""
        def needs_value(value: int) -> str:
            return str(value)

        app = self._make_app_with_task(_make_task_mock(needs_value))
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
        )

        error = _signature_error(app, schedule)
        assert error is not None
        assert error.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'kwargs do not match' in error.message

    def test_unexpected_kwarg_returns_configuration_error(self) -> None:
        """Unexpected kwargs are rejected."""

        def needs_value(value: int) -> str:
            return str(value)

        app = self._make_app_with_task(_make_task_mock(needs_value))
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'unknown': 42},
        )

        error = _signature_error(app, schedule)
        assert error is not None
        assert error.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'kwargs do not match' in error.message

    def test_satisfied_by_kwargs(self) -> None:
        """Required param satisfied by keyword args passes."""
        def needs_value(value: int) -> str:
            return str(value)

        app = self._make_app_with_task(_make_task_mock(needs_value))
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'value': 42},
        )

        assert _signature_error(app, schedule) is None

    def test_multiple_params_satisfied_by_kwargs(self) -> None:
        """Multiple required params satisfied entirely by kwargs."""
        def needs_both(a: int, b: str) -> str:
            return f'{a}{b}'

        app = self._make_app_with_task(_make_task_mock(needs_both))
        schedule = TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'a': 1, 'b': 'hello'},
        )

        assert _signature_error(app, schedule) is None


# =============================================================================
# validation._encode_schedule_kwargs
# =============================================================================


@pytest.mark.unit
class TestEncodeScheduleKwargs:
    """Schedules whose kwargs pass sig.bind but cannot go on the wire must be
    rejected at startup, not on every tick."""

    def _make_app_with_task(self, task: MagicMock) -> MagicMock:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='task_a',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        return _make_app(schedule_config=config, tasks={'task_a': task})

    def _schedule(self, kwargs: dict[str, object]) -> TaskSchedule:
        return TaskSchedule(
            name='s',
            task_name='task_a',
            pattern=IntervalSchedule(seconds=5),
            kwargs=kwargs,
        )

    def _blob_app(self, *, reject_bytes: int | None) -> MagicMock:
        def blob_fn(*, blob: str) -> None:
            return None

        app = self._make_app_with_task(_make_task_mock(blob_fn))
        app.config.payload = PayloadPolicy(warn_bytes=None, reject_bytes=reject_bytes)
        return app

    def test_no_kwargs_is_ok_none(self) -> None:
        def f() -> str:
            return 'ok'

        app = self._make_app_with_task(_make_task_mock(f))
        assert _encode_schedule_kwargs(app, self._schedule({})) == Ok(None)

    def test_plain_kwargs_give_the_wire_text(self) -> None:
        def f(value: int) -> str:
            return str(value)

        app = self._make_app_with_task(_make_task_mock(f))
        result = _encode_schedule_kwargs(app, self._schedule({'value': 5}))
        assert result == dumps_json({'value': 5})

    def test_engine_injected_kwarg_rejected(self) -> None:
        def f(x: int, workflow_ctx: WorkflowContext | None = None) -> str:
            return str(x)

        app = self._make_app_with_task(_make_task_mock(f))
        result = _encode_schedule_kwargs(app, self._schedule({'x': 1, 'workflow_ctx': None}))
        assert is_err(result)
        assert result.err_value.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'not encodable' in result.err_value.message

    def test_outcome_typed_kwarg_rejected(self) -> None:
        def f(dep: TaskOutcome[int, TaskError]) -> str:
            return 'ok'

        app = self._make_app_with_task(_make_task_mock(f))
        result = _encode_schedule_kwargs(app, self._schedule({'dep': Succeeded(1)}))
        assert is_err(result)
        assert result.err_value.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'not encodable' in result.err_value.message

    def test_serialize_failure_rejected_with_the_cause(self) -> None:
        def f(value: int) -> str:
            return str(value)

        app = self._make_app_with_task(_make_task_mock(f))
        with patch(
            'horsies.core.scheduler.validation.dumps_json',
            return_value=Err(SerializationError('bad kwargs')),
        ):
            result = _encode_schedule_kwargs(app, self._schedule({'value': 5}))

        assert is_err(result)
        assert result.err_value.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'not JSON-serializable' in result.err_value.message
        assert result.err_value.notes == ['serialization error: bad kwargs']

    def test_kwargs_over_the_limit_rejected_with_size_and_limit(self) -> None:
        wire_text = _json_text({'blob': 'x' * 2048})
        app = self._blob_app(reject_bytes=256)

        result = _encode_schedule_kwargs(app, self._schedule({'blob': 'x' * 2048}))

        assert is_err(result)
        assert result.err_value.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert result.err_value.message == "schedule 's' kwargs exceed payload.reject_bytes"
        assert result.err_value.notes == [
            f'kwargs payload size: {len(wire_text)} bytes',
            'payload.reject_bytes: 256',
        ]

    def test_kwargs_equal_to_the_limit_pass(self) -> None:
        wire_text = _json_text({'blob': 'x' * 2048})
        app = self._blob_app(reject_bytes=len(wire_text))

        result = _encode_schedule_kwargs(app, self._schedule({'blob': 'x' * 2048}))

        assert result == Ok(wire_text)

    def test_one_character_over_the_limit_is_rejected(self) -> None:
        wire_text = _json_text({'blob': 'x' * 2048})
        app = self._blob_app(reject_bytes=len(wire_text) - 1)

        result = _encode_schedule_kwargs(app, self._schedule({'blob': 'x' * 2048}))

        assert is_err(result)

    def test_no_limit_passes_any_size(self) -> None:
        app = self._blob_app(reject_bytes=None)

        result = _encode_schedule_kwargs(app, self._schedule({'blob': 'x' * 2048}))

        assert is_ok(result)


# =============================================================================
# _preload_task_modules
# =============================================================================


@pytest.mark.unit
class TestPreloadTaskModules:
    """Tests for Scheduler._preload_task_modules."""

    def _make_scheduler(self) -> Scheduler:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        return Scheduler(app)

    def test_no_modules_discovered_logs_warning(self) -> None:
        """Empty module list → warning logged, returns early."""
        scheduler = self._make_scheduler()
        scheduler.app.get_discovered_task_modules = MagicMock(return_value=[])

        with patch('horsies.core.scheduler.service.logger') as mock_logger:
            scheduler._preload_task_modules()

        mock_logger.warning.assert_called_once()
        assert 'No task modules discovered' in mock_logger.warning.call_args[0][0]

    def test_discover_raises_propagates(self) -> None:
        """get_discovered_task_modules raises → error propagates to caller."""
        scheduler = self._make_scheduler()
        scheduler.app.get_discovered_task_modules = MagicMock(
            side_effect=RuntimeError('discovery broken'),
        )

        with pytest.raises(RuntimeError, match='discovery broken'):
            scheduler._preload_task_modules()

    def test_file_path_import_calls_import_by_path(self) -> None:
        """Module ending with .py → import_file_path called."""
        scheduler = self._make_scheduler()
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['tasks/my_tasks.py'],
        )

        with patch('horsies.core.scheduler.service.import_file_path') as mock_import:
            scheduler._preload_task_modules()

        mock_import.assert_called_once()
        # The path is os.path.abspath'd
        assert mock_import.call_args[0][0].endswith('tasks/my_tasks.py')

    def test_module_name_import_calls_importlib(self) -> None:
        """Dotted module name → importlib.import_module called."""
        scheduler = self._make_scheduler()
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['myapp.tasks'],
        )

        with patch('horsies.core.scheduler.service.importlib.import_module') as mock_import:
            scheduler._preload_task_modules()

        mock_import.assert_called_once_with('myapp.tasks')

    def test_import_failure_logs_warning_continues(self) -> None:
        """One module fails → warning logged, next module still imported."""
        scheduler = self._make_scheduler()
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['bad_module', 'good_module'],
        )

        import_calls: list[str] = []

        def _track_import(name: str) -> None:
            import_calls.append(name)
            if name == 'bad_module':
                raise ImportError('cannot import bad_module')

        with patch(
            'horsies.core.scheduler.service.importlib.import_module',
            side_effect=_track_import,
        ):
            scheduler._preload_task_modules()

        assert import_calls == ['bad_module', 'good_module']


# =============================================================================
# validation._signature_error — no _original_fn
# =============================================================================


@pytest.mark.unit
class TestSignatureErrorNoOriginalFn:
    """Test for _signature_error when task has no _original_fn."""

    def test_no_original_fn_returns_none(self) -> None:
        """Task without _original_fn → no error (cannot validate)."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='task_a',
                    pattern=IntervalSchedule(seconds=5),
                    kwargs={'anything': 42},
                ),
            ],
        )
        task_mock = MagicMock()
        task_mock._original_fn = None
        app = _make_app(schedule_config=config, tasks={'task_a': task_mock})

        # No error despite kwargs that wouldn't match any real function
        assert _signature_error(app, config.schedules[0]) is None


# =============================================================================
# Remaining coverage gap tests
# =============================================================================


@pytest.mark.unit
class TestPreloadTaskModulesAdditionalPaths:
    """Cover suppress_sends exceptions and file path import failure."""

    def test_suppress_sends_true_failure_propagates(self) -> None:
        """suppress_sends(True) raises → error propagates to caller."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['myapp.tasks'],
        )
        scheduler.app.suppress_sends = MagicMock(
            side_effect=AttributeError('no suppress_sends'),
        )

        with pytest.raises(AttributeError, match='no suppress_sends'):
            scheduler._preload_task_modules()

    def test_suppress_sends_false_failure_propagates(self) -> None:
        """suppress_sends(False) raises in finally → error propagates."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['myapp.tasks'],
        )

        def _suppress_side_effect(value: bool) -> None:
            if not value:
                raise AttributeError('no suppress_sends')

        scheduler.app.suppress_sends = MagicMock(
            side_effect=_suppress_side_effect,
        )
        # The finally-restore passes the saved prior flag (spec change,
        # 0.1.7); pin it to False so the failing restore path is exercised.
        scheduler.app._suppress_sends = False

        with pytest.raises(AttributeError, match='no suppress_sends'):
            with patch(
                'horsies.core.scheduler.service.importlib.import_module',
            ):
                scheduler._preload_task_modules()

    def test_file_path_import_failure_logs_warning(self) -> None:
        """File ending with .py that fails import → warning logged, continues."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['tasks/bad.py', 'myapp.good'],
        )

        import_calls: list[str] = []

        def _record_import(name: str) -> None:
            import_calls.append(name)

        with (
            patch(
                'horsies.core.scheduler.service.import_file_path',
                side_effect=ImportError('cannot import'),
            ),
            patch(
                'horsies.core.scheduler.service.importlib.import_module',
                side_effect=_record_import,
            ),
        ):
            scheduler._preload_task_modules()

        # Despite .py import failing, module import was still attempted
        assert import_calls == ['myapp.good']

    def test_sys_path_insert_when_cwd_missing(self) -> None:
        """When cwd is not in sys.path, it gets inserted."""
        import sys

        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['myapp.tasks'],
        )

        fake_cwd = '/unlikely/test/dir/xyz'
        original_sys_path = sys.path.copy()

        try:
            # Ensure fake cwd is not in sys.path
            sys.path = [p for p in sys.path if p != fake_cwd]

            with (
                patch('os.getcwd', return_value=fake_cwd),
                patch(
                    'horsies.core.scheduler.service.importlib.import_module',
                ),
            ):
                scheduler._preload_task_modules()

            assert fake_cwd in sys.path
        finally:
            sys.path = original_sys_path


@pytest.mark.unit
class TestValidateSchedulesCallsSignatureValidation:
    """The start path of the scheduler facade prepares each schedule through
    ``scheduler/validation.py`` before the core run."""

    def test_valid_task_and_queue_calls_signature_validation(self) -> None:
        """A valid (kwargs-only) schedule reaches signature validation."""
        def my_fn(x: int) -> str:
            return str(x)

        task_mock = _make_task_mock(my_fn)

        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    kwargs={'x': 42},
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={'my_task': task_mock})
        scheduler = Scheduler(app)

        # Should not raise — kwargs match signature
        prepared = scheduler._prepare_schedules()

        assert list(prepared) == ['s']
