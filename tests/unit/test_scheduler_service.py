# pyright: reportPrivateUsage=false
"""Tests of the Python scheduler loop (mock-based, no DB).

The loop is the reference of the scheduler dual-runs
(``tests/dualrun/reference/scheduler_loop_caller.py``); the production
``Scheduler`` is a facade on ``horsies._core.CoreScheduler``
(``test_scheduler_facade.py``).
"""

from __future__ import annotations

import logging
from datetime import datetime, time, timezone
from typing import Annotated
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import psycopg.errors
from pydantic import AfterValidator, SecretStr
from sqlalchemy.exc import DBAPIError, OperationalError

from horsies.core.codec.json_io import Json, SerializationError, dumps_json
from horsies.core.codec.kwargs import encode_kwargs_read_back
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.brokers.postgres import PostgresBroker, SendCommand
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerInvalidInput,
    BrokerListenerDisabled,
    BrokerOperationFailed,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.errors import ConfigurationError, ErrorCode, RegistryError
from horsies.core.models.schedule import (
    DailySchedule,
    IntervalSchedule,
    MonthlySchedule,
    ScheduleConfig,
    TaskSchedule,
    Weekday,
    WeeklySchedule,
)
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import Execution, TaskError, TaskResult
from horsies.core.models.workflow import WorkflowContext
from horsies.core.scheduler.calculator import calculate_next_run
from tests.dualrun.reference.scheduler_loop_caller import Scheduler
from horsies.core.scheduler.state import (
    DueSchedule,
    DueScheduleRead,
    FiredSlot,
    ScheduleAdvanced,
    ScheduleFire,
    ScheduleFireOutcomeUnknown,
    ScheduleFireRefused,
    ScheduleStateMissing,
    ScheduleStateMoved,
    ScheduleStateWrite,
    SlotOutcome,
    StartStateRead,
    StoredConfigHash,
)
from horsies.core.scheduler.validation import (
    PreparedSchedule,
    _encode_schedule_kwargs,
    _signature_error,
)
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.utils.fingerprint import schedule_slot_task_id
from tests.broker_resources_support import (
    attach_loop_limit,
    attach_resources,
    fake_resources,
)


class _Deadline:
    """The clock of `_deadline_task`: tests move it past the value."""

    passed = False


def _not_expired(value: str) -> str:
    if _Deadline.passed:
        raise ValueError('deadline has passed')
    return value


# A 32-byte config hash that no schedule of these tests produces.
STALE_CONFIG_HASH = bytes(32)

# A database time far from the host clock: a first run or a slot computed
# from the host clock cannot equal one computed from this time.
DATABASE_NOW = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _start_read(*rows: tuple[str, bytes]) -> StartStateRead:
    """The start read at DATABASE_NOW with these stored digests."""
    return StartStateRead(
        database_now=DATABASE_NOW,
        rows=tuple(StoredConfigHash(name, digest) for name, digest in rows),
    )


def _due_read(*names: str) -> DueScheduleRead:
    """The due read at DATABASE_NOW with these rows due."""
    return DueScheduleRead(
        database_now=DATABASE_NOW,
        due=tuple(DueSchedule(name, DATABASE_NOW) for name in names),
    )


def _written_names(store: AsyncMock) -> list[str]:
    """The schedule names of the one start write or heal call."""
    assert store.await_args is not None, 'the write was not called'
    writes: list[ScheduleStateWrite] = store.await_args.args[0]
    return [write.schedule_name for write in writes]


def _fail_on_call(failing_call: int) -> object:
    """A calculate_next_run that raises on its ``failing_call``-th call."""
    calls = 0

    def _next_run(pattern: object, from_time: datetime, tz: str = 'UTC') -> datetime:
        nonlocal calls
        calls += 1
        if calls == failing_call:
            raise RuntimeError('simulated calculator failure')
        return calculate_next_run(pattern, from_time, tz)  # type: ignore[arg-type]

    return _next_run


def _deadline_task(*, deadline: Annotated[str, AfterValidator(_not_expired)]) -> None:
    """A parameter whose validator can refuse later what it accepted at boot."""
    return None


def _json_text(value: Json) -> str:
    """The JSON text of a value that the strict encoder takes."""
    match dumps_json(value):
        case Ok(text):
            return text
        case Err(error):
            raise AssertionError(f'value does not encode: {error}')


def _utc(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
) -> datetime:
    """Helper to construct a UTC-aware datetime."""
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


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


# A send command that no test reads the values of.
COMMAND = SendCommand(
    task_name='my_task',
    queue_name='default',
    priority=100,
    stored_input='{}',
    task_options=None,
    execution=Execution.ISOLATED,
    max_retries=0,
    retention_class_key='standard_30d',
    retain_rerun_input=False,
    fingerprint_version=1,
    fingerprint=bytes(32),
)

# A broker that builds real send commands; it opens no connection.
REAL_BROKER_URL = 'postgresql://user:pass@localhost/db'


def _fire_harness(
    schedule: TaskSchedule,
    *answers: object,
) -> tuple[Scheduler, MagicMock]:
    """A started scheduler with one schedule; each fire answers the next of ``answers``.

    The prepared text and the send command are set as ``start()`` sets them.
    The state manager double records the fires and the heals.
    """
    app = _make_app(schedule_config=ScheduleConfig(schedules=[schedule]))
    scheduler = Scheduler(app)
    scheduler.broker = MagicMock()
    scheduler._prepared_schedules = {
        schedule.name: PreparedSchedule(input_json=None, task_options_json=None),
    }
    scheduler._commands = {schedule.name: COMMAND}
    state_manager = MagicMock()
    state_manager.fire_schedule = AsyncMock(side_effect=list(answers))
    state_manager.heal_missing_states = AsyncMock(return_value=(schedule.name,))
    scheduler.state_manager = state_manager
    return scheduler, state_manager


def _fires(state_manager: MagicMock) -> list[ScheduleFire]:
    """Every fire the state manager double was sent, in order."""
    return [call.args[0] for call in state_manager.fire_schedule.await_args_list]


def _advanced(*slots: datetime) -> Ok[ScheduleAdvanced]:
    """An advanced fire whose slots are all new rows."""
    return Ok(
        ScheduleAdvanced(
            slots=tuple(
                FiredSlot(slot, str(schedule_slot_task_id('s', slot)), SlotOutcome.INSERTED)
                for slot in slots
            ),
        ),
    )


def _real_broker() -> PostgresBroker:
    return PostgresBroker(PostgresConfig(database_url=SecretStr(REAL_BROKER_URL)))


# =============================================================================
# Scheduler.__init__
# =============================================================================


@pytest.mark.unit
class TestSchedulerInit:
    """Tests for Scheduler initialization."""

    def test_missing_config_raises_configuration_error(self) -> None:
        """App without schedule config raises ConfigurationError HRS-205."""
        app = _make_app(schedule_config=None)

        with pytest.raises(ConfigurationError) as exc_info:
            Scheduler(app)

        exc = exc_info.value
        assert exc.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'schedule configuration' in exc.message
        assert exc.help_text is not None

    def test_valid_config_initializes(self) -> None:
        """Valid schedule config allows Scheduler construction."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='test',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=10),
                ),
            ],
        )
        app = _make_app(schedule_config=config)

        scheduler = Scheduler(app)

        assert scheduler.schedule_config is config
        assert scheduler._initialized is False


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

    def test_taskresult_typed_kwarg_rejected(self) -> None:
        def f(dep: TaskResult[int, TaskError]) -> str:
            return 'ok'

        app = self._make_app_with_task(_make_task_mock(f))
        result = _encode_schedule_kwargs(app, self._schedule({'dep': TaskResult(ok=1)}))
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
# _compute_config_hash
# =============================================================================


@pytest.mark.unit
class TestComputeConfigHash:
    """Tests for Scheduler._compute_config_hash."""

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

    def test_deterministic(self) -> None:
        """Same schedule produces same hash."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='test',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=10),
        )

        hash1 = scheduler._compute_config_hash(schedule)
        hash2 = scheduler._compute_config_hash(schedule)

        assert hash1 == hash2

    def test_changes_on_pattern_change(self) -> None:
        """Different pattern produces different hash."""
        scheduler = self._make_scheduler()
        schedule_a = TaskSchedule(
            name='test',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=10),
        )
        schedule_b = TaskSchedule(
            name='test',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=20),
        )

        hash_a = scheduler._compute_config_hash(schedule_a)
        hash_b = scheduler._compute_config_hash(schedule_b)

        assert hash_a != hash_b

    def test_changes_on_timezone_change(self) -> None:
        """Different timezone produces different hash."""
        scheduler = self._make_scheduler()
        schedule_utc = TaskSchedule(
            name='test',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=10),
            timezone='UTC',
        )
        schedule_ny = TaskSchedule(
            name='test',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=10),
            timezone='America/New_York',
        )

        hash_utc = scheduler._compute_config_hash(schedule_utc)
        hash_ny = scheduler._compute_config_hash(schedule_ny)

        assert hash_utc != hash_ny

    def test_time_based_patterns_serialize(self) -> None:
        """Regression: time-based patterns (Daily/Weekly/Monthly) carry a
        datetime.time. _compute_config_hash must serialize them, not raise —
        strict dumps_json rejects a live datetime.time, so model_dump(mode='json')
        is required. Interval-only tests previously hid this."""
        scheduler = self._make_scheduler()
        patterns = [
            DailySchedule(time=time(3, 0)),
            WeeklySchedule(days=[Weekday.MONDAY, Weekday.FRIDAY], time=time(9, 0)),
            MonthlySchedule(day=15, time=time(17, 30)),
        ]
        for pattern in patterns:
            schedule = TaskSchedule(name='t', task_name='my_task', pattern=pattern)
            digest = scheduler._compute_config_hash(schedule)
            assert type(digest) is bytes and len(digest) == 32  # sha256 digest

    def test_time_based_pattern_hash_is_deterministic(self) -> None:
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='t', task_name='my_task', pattern=DailySchedule(time=time(3, 0)),
        )
        assert scheduler._compute_config_hash(schedule) == scheduler._compute_config_hash(schedule)


# =============================================================================
# _calculate_missed_runs
# =============================================================================


@pytest.mark.unit
class TestCalculateMissedRuns:
    """Tests for Scheduler._calculate_missed_runs."""

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

    def test_includes_first_due_run(self) -> None:
        """Catch-up includes the first overdue scheduled slot."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=2),
        )
        last = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 1)

        result = scheduler._calculate_missed_runs(schedule, last, now, max_runs=10)

        assert result == [last]

    def test_gap_returns_correct_due_list(self) -> None:
        """Gap produces all due runs including first due slot."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
        )
        last = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 17)

        result = scheduler._calculate_missed_runs(schedule, last, now, max_runs=10)

        assert len(result) == 4
        assert result[0] == _utc(2025, 6, 1, 12, 0, 0)
        assert result[1] == _utc(2025, 6, 1, 12, 0, 5)
        assert result[2] == _utc(2025, 6, 1, 12, 0, 10)
        assert result[3] == _utc(2025, 6, 1, 12, 0, 15)

    def test_chronologically_ordered(self) -> None:
        """Due runs are in ascending chronological order."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=3),
        )
        last = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 12)

        result = scheduler._calculate_missed_runs(schedule, last, now, max_runs=10)

        for i in range(1, len(result)):
            assert result[i] > result[i - 1]

    def test_respects_max_runs_cap(self) -> None:
        """Catch-up caps due runs for a single tick."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=1),
        )
        last = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 5)

        result = scheduler._calculate_missed_runs(schedule, last, now, max_runs=3)

        assert result == [
            _utc(2025, 6, 1, 12, 0, 0),
            _utc(2025, 6, 1, 12, 0, 1),
            _utc(2025, 6, 1, 12, 0, 2),
        ]


# =============================================================================
# _advance_to_latest_due_slot (catch_up_missed=False skip semantics)
# =============================================================================


@pytest.mark.unit
class TestAdvanceToLatestDueSlot:
    """Tests for Scheduler._advance_to_latest_due_slot."""

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

    def test_single_due_slot_is_not_skipped(self) -> None:
        """Normal operation: slot due by under one period fires unchanged."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
        )
        first_due = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 1)

        slot, next_run, skipped = scheduler._advance_to_latest_due_slot(
            schedule, first_due, now,
        )

        assert slot == first_due
        assert skipped == 0
        assert next_run == _utc(2025, 6, 1, 12, 0, 5)
        assert next_run > now

    def test_backlog_skips_to_latest_due_slot(self) -> None:
        """Downtime backlog: only the latest due slot remains."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
        )
        first_due = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 52)  # slots 0..50 due (11 slots)

        slot, next_run, skipped = scheduler._advance_to_latest_due_slot(
            schedule, first_due, now,
        )

        assert slot == _utc(2025, 6, 1, 12, 0, 50)
        assert skipped == 10
        assert next_run == _utc(2025, 6, 1, 12, 0, 55)
        assert next_run > now

    def test_scan_cap_returns_partial_progress(self) -> None:
        """Deep backlog beyond the cap returns a still-due next_run."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=1),
        )
        first_due = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 30)

        with patch.object(Scheduler, '_MAX_SKIP_SCAN_PER_TICK', 3):
            slot, next_run, skipped = scheduler._advance_to_latest_due_slot(
                schedule, first_due, now,
            )

        assert skipped == 3
        assert slot == _utc(2025, 6, 1, 12, 0, 3)
        assert next_run == _utc(2025, 6, 1, 12, 0, 4)
        assert next_run <= now, 'cap path must signal an unconverged backlog'


@pytest.mark.unit
@pytest.mark.asyncio
class TestCheckScheduleSkipsMissedRuns:
    """catch_up_missed=False fires at most one run after downtime."""

    async def test_downtime_backlog_fires_latest_slot_once(self) -> None:
        """A backlog of ten 5-second slots sends one fire with the latest slot."""
        schedule = TaskSchedule(name='s', task_name='my_task', pattern=IntervalSchedule(seconds=5))
        latest = _utc(2025, 6, 1, 12, 0, 50)
        scheduler, state_manager = _fire_harness(schedule, _advanced(latest))
        now = _utc(2025, 6, 1, 12, 0, 52)

        await scheduler._check_schedule(
            schedule, DueSchedule('s', _utc(2025, 6, 1, 12, 0, 0)), now,
        )

        [fire] = _fires(state_manager)
        assert fire.slot_ats == (latest,)
        assert fire.expected_next_run_at == _utc(2025, 6, 1, 12, 0, 0)
        assert fire.next_run_at == _utc(2025, 6, 1, 12, 0, 55)
        assert fire.next_run_at > now

    async def test_deep_backlog_persists_progress_without_firing(self) -> None:
        """The scan bound moves the row with a fire of no slots."""
        schedule = TaskSchedule(name='s', task_name='my_task', pattern=IntervalSchedule(seconds=1))
        scheduler, state_manager = _fire_harness(schedule, _advanced())
        now = _utc(2025, 6, 1, 12, 0, 30)

        with patch.object(Scheduler, '_MAX_SKIP_SCAN_PER_TICK', 3):
            await scheduler._check_schedule(
                schedule, DueSchedule('s', _utc(2025, 6, 1, 12, 0, 0)), now,
            )

        [fire] = _fires(state_manager)
        assert fire.slot_ats == ()
        assert fire.next_run_at == _utc(2025, 6, 1, 12, 0, 4)


# =============================================================================
# _validate_schedules
# =============================================================================


@pytest.mark.unit
class TestValidateSchedules:
    """Tests for Scheduler._validate_schedules."""

    def test_invalid_timezone_raises_configuration_error(self) -> None:
        """Regression: a bad timezone fails at startup validation (exit 1)
        instead of failing calculate_next_run on every tick — the
        missing-row self-heal would otherwise retry it forever."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    timezone='Not/AZone',
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        with pytest.raises(ConfigurationError) as exc_info:
            scheduler._validate_schedules()

        exc = exc_info.value
        assert exc.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'invalid timezone' in exc.message

    def test_unregistered_task_raises_registry_error(self) -> None:
        """Schedule referencing unregistered task raises RegistryError HRS-300."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='nonexistent_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={})
        scheduler = Scheduler(app)

        with pytest.raises(RegistryError) as exc_info:
            scheduler._validate_schedules()

        exc = exc_info.value
        assert exc.code == ErrorCode.TASK_NOT_REGISTERED
        assert 'not registered' in exc.message

    def test_kwargs_over_the_limit_raise_configuration_error(self) -> None:
        """A schedule over payload.reject_bytes fails boot, once."""
        def blob_fn(*, blob: str) -> None:
            return None

        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    kwargs={'blob': 'x' * 2048},
                ),
            ],
        )
        app = _make_app(
            schedule_config=config,
            tasks={'my_task': _make_task_mock(blob_fn)},
        )
        app.config.payload = PayloadPolicy(warn_bytes=None, reject_bytes=256)
        scheduler = Scheduler(app)

        with pytest.raises(ConfigurationError) as exc_info:
            scheduler._validate_schedules()

        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'payload.reject_bytes' in exc_info.value.message

    def test_a_valid_schedule_keeps_its_prepared_text(self) -> None:
        def value_fn(*, value: int) -> None:
            return None

        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    kwargs={'value': 5},
                ),
            ],
        )
        app = _make_app(
            schedule_config=config,
            tasks={'my_task': _make_task_mock(value_fn)},
        )
        scheduler = Scheduler(app)

        scheduler._validate_schedules()

        assert scheduler._prepared_schedules == {
            's': PreparedSchedule(
                input_json=_json_text({'value': 5}),
                task_options_json=None,
            ),
        }

    def test_disabled_schedule_skipped(self) -> None:
        """Disabled schedules are not validated."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='disabled',
                    task_name='nonexistent_task',
                    pattern=IntervalSchedule(seconds=5),
                    enabled=False,
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={})
        scheduler = Scheduler(app)

        # Should not raise even though task doesn't exist
        scheduler._validate_schedules()

    def test_invalid_queue_raises_configuration_error(self) -> None:
        """Invalid queue configuration raises ConfigurationError HRS-205."""
        def no_params() -> str:
            return 'ok'

        task_mock = _make_task_mock(no_params)

        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    queue_name='invalid_queue',
                ),
            ],
        )
        app = _make_app(
            schedule_config=config,
            tasks={'my_task': task_mock},
        )
        # Make validate_queue_name raise to simulate invalid queue
        app.validate_queue_name = MagicMock(
            side_effect=ConfigurationError(
                message='invalid queue',
                code=ErrorCode.CONFIG_INVALID_QUEUE_MODE,
            ),
        )
        scheduler = Scheduler(app)

        with pytest.raises(ConfigurationError) as exc_info:
            scheduler._validate_schedules()

        exc = exc_info.value
        assert exc.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'invalid queue configuration' in exc.message


# =============================================================================
# _initialize_schedules
# =============================================================================


@pytest.mark.unit
class TestInitializeSchedules:
    """Tests for Scheduler._initialize_schedules."""

    @pytest.mark.asyncio
    async def test_foreign_schedule_state_rows_are_kept(self) -> None:
        """Startup must not delete state rows outside this config.

        Spec change (0.1.7): auto-pruning broke rolling deploys (the
        old-config scheduler lost its rows and silently stopped firing)
        and shared-database topologies. Orphan rows are kept and only
        logged.
        """
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='kept',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.read_start_states = AsyncMock(
            return_value=_start_read(('other_schedulers_row', STALE_CONFIG_HASH)),
        )
        state_manager.store_start_states = AsyncMock(return_value=('kept',))
        scheduler.state_manager = state_manager

        await scheduler._initialize_schedules()

        state_manager.read_start_states.assert_awaited_once()
        assert _written_names(state_manager.store_start_states) == ['kept']

    @pytest.mark.asyncio
    async def test_first_runs_come_from_the_database_time(self) -> None:
        """A new schedule's first run is computed from the start read's
        database time, not from the host clock."""
        pattern = IntervalSchedule(seconds=10)
        config = ScheduleConfig(
            schedules=[TaskSchedule(name='s', task_name='my_task', pattern=pattern)],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.read_start_states = AsyncMock(return_value=_start_read())
        state_manager.store_start_states = AsyncMock(return_value=('s',))
        scheduler.state_manager = state_manager

        await scheduler._initialize_schedules()

        writes: list[ScheduleStateWrite] = state_manager.store_start_states.await_args.args[0]
        assert writes == [
            ScheduleStateWrite(
                schedule_name='s',
                next_run_at=calculate_next_run(pattern, DATABASE_NOW, 'UTC'),
                config_hash=scheduler._compute_config_hash(config.schedules[0]),
            ),
        ]

    @pytest.mark.asyncio
    async def test_one_bad_schedule_does_not_block_others(self) -> None:
        """If calculate_next_run fails for one schedule, the rest still initialize."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='good_first',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=10),
                ),
                TaskSchedule(
                    name='bad',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=10),
                ),
                TaskSchedule(
                    name='good_last',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=10),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.read_start_states = AsyncMock(return_value=_start_read())
        state_manager.store_start_states = AsyncMock(return_value=('good_first', 'good_last'))
        scheduler.state_manager = state_manager

        # Should not raise — bad schedule is isolated
        with patch(
            'tests.dualrun.reference.scheduler_loop_caller.calculate_next_run',
            side_effect=_fail_on_call(2),
        ):
            await scheduler._initialize_schedules()

        assert _written_names(state_manager.store_start_states) == ['good_first', 'good_last']

    @pytest.mark.asyncio
    async def test_store_failure_is_logged_and_start_goes_on(self) -> None:
        """A database error of the start write does not raise: the tick's
        existence heal creates the missing rows."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(name='s', task_name='my_task', pattern=IntervalSchedule(seconds=5)),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.read_start_states = AsyncMock(return_value=_start_read())
        state_manager.store_start_states = AsyncMock(side_effect=RuntimeError('db error'))
        scheduler.state_manager = state_manager

        await scheduler._initialize_schedules()

        state_manager.store_start_states.assert_awaited_once()


# =============================================================================
# _check_schedule: one statement per due schedule
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestCheckScheduleOneStatement:
    """A due schedule sends one fire; no lock session, no per-slot enqueue."""

    async def test_a_due_slot_sends_one_fire_from_the_due_read(self) -> None:
        schedule = TaskSchedule(name='s', task_name='my_task', pattern=IntervalSchedule(seconds=5))
        slot = _utc(2025, 6, 1, 11, 59, 59)
        scheduler, state_manager = _fire_harness(schedule, _advanced(slot))

        await scheduler._check_schedule(schedule, DueSchedule('s', slot), _utc(2025, 6, 1, 12, 0, 0))

        assert _fires(state_manager) == [
            ScheduleFire(
                schedule_name='s',
                expected_next_run_at=slot,
                next_run_at=_utc(2025, 6, 1, 12, 0, 4),
                slot_ats=(slot,),
                command=COMMAND,
            ),
        ]
        state_manager.heal_missing_states.assert_not_awaited()
        assert isinstance(scheduler.broker, MagicMock)
        scheduler.broker.enqueue_async.assert_not_called()

    async def test_a_missing_state_row_is_healed_from_the_database_time(self) -> None:
        schedule = TaskSchedule(name='s', task_name='my_task', pattern=IntervalSchedule(seconds=5))
        scheduler, state_manager = _fire_harness(schedule, Ok(ScheduleStateMissing()))

        await scheduler._check_schedule(
            schedule, DueSchedule('s', _utc(2020, 1, 1, 11, 59, 59)), DATABASE_NOW,
        )

        assert _written_names(state_manager.heal_missing_states) == ['s']
        [write] = state_manager.heal_missing_states.await_args.args[0]
        assert write == ScheduleStateWrite(
            schedule_name='s',
            next_run_at=calculate_next_run(schedule.pattern, DATABASE_NOW, 'UTC'),
            config_hash=scheduler._compute_config_hash(schedule),
        )
        state_manager.fire_schedule.assert_awaited_once()


# =============================================================================
# Scheduler.start
# =============================================================================


def _make_broker_mock(
    *,
    schema_ok: bool = True,
    close_ok: bool = True,
) -> MagicMock:
    """Build a mock PostgresBroker for Scheduler tests."""
    broker = MagicMock()
    if schema_ok:
        broker.ensure_schema_initialized = AsyncMock(return_value=Ok(None))
    else:
        broker.ensure_schema_initialized = AsyncMock(
            return_value=Err(BrokerOperationFailed(
                code=BrokerErrorCode.SCHEMA_INIT_FAILED,
                message='schema init failed',
                retryable=False,
                exception=RuntimeError('db down'),
            )),
        )
    if close_ok:
        broker.close_async = AsyncMock(return_value=Ok(None))
    else:
        broker.close_async = AsyncMock(
            return_value=Err(BrokerOperationFailed(
                code=BrokerErrorCode.CLOSE_FAILED,
                message='close failed',
                retryable=False,
                exception=ConnectionError('close failed'),
            )),
        )
    # ``broker.session_factory`` is a test-side handle only: the same mock is
    # placed in the attached resource set, which is what the code under test
    # reads through ``select_resources()``. Tests configure it through this name.
    broker.session_factory = MagicMock()
    attach_resources(broker, fake_resources(session_factory=broker.session_factory))
    return broker


@pytest.mark.unit
@pytest.mark.asyncio
class TestSchedulerStart:
    """Tests for Scheduler.start()."""

    async def test_already_initialized_returns_immediately(self) -> None:
        """If _initialized is True, start() is a no-op."""
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
        scheduler._initialized = True

        # Should not call get_broker or anything else
        await scheduler.start()

        app.get_broker.assert_not_called()

    async def test_schema_init_failure_raises_runtime_error(self) -> None:
        """Schema init failure raises RuntimeError with chained exception."""
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
        broker = _make_broker_mock(schema_ok=False)
        app.get_broker = MagicMock(return_value=broker)
        scheduler = Scheduler(app)

        with pytest.raises(RuntimeError, match='Schema initialization failed') as raised:
            await scheduler.start()

        assert isinstance(raised.value.__cause__, RuntimeError)
        assert str(raised.value.__cause__) == 'db down'

    async def test_schema_init_failure_without_a_cause_raises_unchained(self) -> None:
        """A schema init error with no exception raises RuntimeError with no cause."""
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
        broker = _make_broker_mock()
        broker.ensure_schema_initialized = AsyncMock(
            return_value=Err(BrokerListenerDisabled(message='no listener')),
        )
        app.get_broker = MagicMock(return_value=broker)
        scheduler = Scheduler(app)

        with pytest.raises(RuntimeError, match='Schema initialization failed: no listener') as raised:
            await scheduler.start()

        assert raised.value.__cause__ is None

    async def test_event_loop_limit_raises_before_schema_init(self) -> None:
        """A broker whose resources serve another loop stops start(); nothing runs."""
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
        broker = _make_broker_mock()
        loop_limit = attach_loop_limit(broker)
        app.get_broker = MagicMock(return_value=broker)
        scheduler = Scheduler(app)

        with pytest.raises(RuntimeError) as raised:
            await scheduler.start()

        assert str(raised.value) == (
            f'Scheduler start failed: {loop_limit.describe()}'
        )
        broker.ensure_schema_initialized.assert_not_awaited()
        assert scheduler.state_manager is None
        assert scheduler._initialized is False

    async def test_state_manager_uses_selected_session_factory(self) -> None:
        """start() selects once and gives the state manager that set's session factory."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={'my_task': _make_task_mock(lambda: 'ok')})
        broker = _make_broker_mock()
        selected = attach_resources(broker, fake_resources(session_factory=MagicMock()))
        app.get_broker = MagicMock(return_value=broker)
        scheduler = Scheduler(app)
        scheduler._preload_task_modules = MagicMock()  # type: ignore[method-assign]
        scheduler._validate_schedules = MagicMock()  # type: ignore[method-assign]
        scheduler._initialize_schedules = AsyncMock()  # type: ignore[method-assign]

        await scheduler.start()

        broker.select_resources.assert_awaited_once()
        assert scheduler.state_manager is not None
        assert scheduler.state_manager.session_factory is selected.session_factory

    async def test_happy_path_initializes_all_components(self) -> None:
        """Successful start: broker, preload, state_manager, validate, init schedules."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={'my_task': _make_task_mock(lambda: 'ok')})
        broker = _make_broker_mock()
        app.get_broker = MagicMock(return_value=broker)
        scheduler = Scheduler(app)

        # Patch methods that need DB or real modules
        scheduler._preload_task_modules = MagicMock()  # type: ignore[method-assign]
        scheduler._validate_schedules = MagicMock()  # type: ignore[method-assign]
        scheduler._initialize_schedules = AsyncMock()  # type: ignore[method-assign]

        await scheduler.start()

        assert scheduler._initialized is True
        assert scheduler.broker is broker
        assert scheduler.state_manager is not None
        scheduler._preload_task_modules.assert_called_once()
        scheduler._validate_schedules.assert_called_once()
        scheduler._initialize_schedules.assert_awaited_once()

    async def test_start_calls_preload_before_validate(self) -> None:
        """Preload runs before validate to ensure task registry is populated."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config, tasks={'my_task': _make_task_mock(lambda: 'ok')})
        broker = _make_broker_mock()
        app.get_broker = MagicMock(return_value=broker)
        scheduler = Scheduler(app)

        call_order: list[str] = []
        scheduler._preload_task_modules = MagicMock(side_effect=lambda: call_order.append('preload'))  # type: ignore[method-assign]
        scheduler._validate_schedules = MagicMock(side_effect=lambda: call_order.append('validate'))  # type: ignore[method-assign]
        scheduler._initialize_schedules = AsyncMock(side_effect=lambda: call_order.append('init'))  # type: ignore[method-assign]

        await scheduler.start()

        assert call_order == ['preload', 'validate', 'init']

    async def test_start_builds_the_command_of_each_schedule(self) -> None:
        """start() builds each enabled schedule's command once, after validation."""
        schedule = TaskSchedule(name='s', task_name='my_task', pattern=IntervalSchedule(seconds=5))
        app = _make_app(
            schedule_config=ScheduleConfig(schedules=[schedule]),
            tasks={'my_task': _make_task_mock(lambda: 'ok')},
        )
        broker = _make_broker_mock()
        broker.prepare_send_command = MagicMock(return_value=Ok(COMMAND))
        app.get_broker = MagicMock(return_value=broker)
        scheduler = Scheduler(app)
        scheduler._preload_task_modules = MagicMock()  # type: ignore[method-assign]
        scheduler._initialize_schedules = AsyncMock()  # type: ignore[method-assign]

        await scheduler.start()

        assert scheduler._commands == {'s': COMMAND}
        assert broker.prepare_send_command.call_args.kwargs == {
            'task_name': 'my_task',
            'queue_name': 'default',
            'priority': 100,
            'input_json': None,
            'task_options': None,
            'retention_class_key': 'standard_30d',
        }


# =============================================================================
# Scheduler.stop
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestSchedulerStop:
    """Tests for Scheduler.stop()."""

    async def test_stop_sets_event_and_closes_broker(self) -> None:
        """Happy path: stop event set, broker closed."""
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
        scheduler.broker = _make_broker_mock(close_ok=True)

        await scheduler.stop()

        assert scheduler._stop.is_set()
        scheduler.broker.close_async.assert_awaited_once()

    async def test_stop_broker_close_error_logs_without_raising(self) -> None:
        """Broker close failure logs error but does not raise."""
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
        scheduler.broker = _make_broker_mock(close_ok=False)

        # Should not raise
        await scheduler.stop()

        assert scheduler._stop.is_set()
        scheduler.broker.close_async.assert_awaited_once()

    async def test_stop_no_broker_only_sets_event(self) -> None:
        """No broker → just sets stop event, no crash."""
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
        scheduler.broker = None

        await scheduler.stop()

        assert scheduler._stop.is_set()


# =============================================================================
# Scheduler.request_stop
# =============================================================================


@pytest.mark.unit
class TestRequestStop:
    """Tests for Scheduler.request_stop()."""

    def test_request_stop_sets_stop_event(self) -> None:
        """request_stop sets the internal _stop event."""
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

        assert not scheduler._stop.is_set()

        scheduler.request_stop()

        assert scheduler._stop.is_set()


# =============================================================================
# Scheduler.run_forever
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestRunForever:
    """Tests for Scheduler.run_forever()."""

    async def test_stop_signal_breaks_loop(self) -> None:
        """Pre-set stop event → loop exits after start()."""
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

        # Stub start and stop to avoid real broker/DB
        scheduler.start = AsyncMock()  # type: ignore[method-assign]
        scheduler.stop = AsyncMock()  # type: ignore[method-assign]

        # Pre-set stop signal so wait_for returns immediately
        scheduler._stop.set()

        await scheduler.run_forever()

        scheduler.start.assert_awaited_once()
        scheduler.stop.assert_awaited_once()

    async def test_schedule_error_does_not_crash_loop(self) -> None:
        """Error in _check_and_run_schedules is caught; loop continues."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
            check_interval_seconds=1,
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        scheduler.start = AsyncMock()  # type: ignore[method-assign]
        scheduler.stop = AsyncMock()  # type: ignore[method-assign]

        call_count = 0

        async def _check_side_effect() -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError('simulated error')
            # On second call, signal stop
            scheduler._stop.set()

        scheduler._check_and_run_schedules = AsyncMock(side_effect=_check_side_effect)  # type: ignore[method-assign]

        await scheduler.run_forever()

        # Called at least twice: first raises, second stops
        assert call_count >= 2

    async def test_timeout_continues_loop(self) -> None:
        """wait_for timeout continues to next iteration."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
            check_interval_seconds=1,
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        scheduler.start = AsyncMock()  # type: ignore[method-assign]
        scheduler.stop = AsyncMock()  # type: ignore[method-assign]

        iteration_count = 0

        async def _check_side_effect() -> None:
            nonlocal iteration_count
            iteration_count += 1
            if iteration_count >= 2:
                scheduler._stop.set()

        scheduler._check_and_run_schedules = AsyncMock(side_effect=_check_side_effect)  # type: ignore[method-assign]

        await scheduler.run_forever()

        # At least 2 iterations before stop
        assert iteration_count >= 2


# =============================================================================
# _initialize_schedules — additional paths
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestInitializeSchedulesAdditionalPaths:
    """Additional tests for Scheduler._initialize_schedules."""

    async def test_no_state_manager_raises_runtime_error(self) -> None:
        """state_manager=None → RuntimeError."""
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
        scheduler.state_manager = None

        with pytest.raises(RuntimeError, match='State manager not initialized'):
            await scheduler._initialize_schedules()

    async def test_start_read_failure_is_logged_and_writes_nothing(self) -> None:
        """read_start_states raises → logged, no write; the tick's existence
        heal creates the missing rows."""
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

        state_manager = MagicMock()
        state_manager.read_start_states = AsyncMock(side_effect=RuntimeError('db error'))
        state_manager.store_start_states = AsyncMock()
        scheduler.state_manager = state_manager

        # Should not raise
        await scheduler._initialize_schedules()

        state_manager.store_start_states.assert_not_awaited()

    async def test_disabled_schedule_skipped_during_init(self) -> None:
        """Disabled schedules get no state row."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='enabled_one',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
                TaskSchedule(
                    name='disabled_one',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    enabled=False,
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.read_start_states = AsyncMock(return_value=_start_read())
        state_manager.store_start_states = AsyncMock(return_value=('enabled_one',))
        scheduler.state_manager = state_manager

        await scheduler._initialize_schedules()

        assert _written_names(state_manager.store_start_states) == ['enabled_one']

    async def test_config_hash_changed_recalculates_next_run(self) -> None:
        """A stored digest that differs → the row is written with the new
        digest and a next run from the database time."""
        pattern = IntervalSchedule(seconds=10)
        config = ScheduleConfig(
            schedules=[TaskSchedule(name='s', task_name='my_task', pattern=pattern)],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.read_start_states = AsyncMock(
            return_value=_start_read(('s', STALE_CONFIG_HASH)),
        )
        state_manager.store_start_states = AsyncMock(return_value=('s',))
        scheduler.state_manager = state_manager

        await scheduler._initialize_schedules()

        writes: list[ScheduleStateWrite] = state_manager.store_start_states.await_args.args[0]
        assert len(writes) == 1
        assert writes[0].config_hash != STALE_CONFIG_HASH
        assert writes[0].config_hash == scheduler._compute_config_hash(config.schedules[0])
        assert writes[0].next_run_at == calculate_next_run(pattern, DATABASE_NOW, 'UTC')

    async def test_unchanged_digest_is_not_written(self) -> None:
        """A stored digest that matches → no write."""
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=10),
        )
        config = ScheduleConfig(schedules=[schedule])
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.read_start_states = AsyncMock(
            return_value=_start_read(('s', scheduler._compute_config_hash(schedule))),
        )
        state_manager.store_start_states = AsyncMock()
        scheduler.state_manager = state_manager

        await scheduler._initialize_schedules()

        state_manager.store_start_states.assert_not_awaited()


# =============================================================================
# _check_and_run_schedules
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestCheckAndRunSchedules:
    """Tests for Scheduler._check_and_run_schedules."""

    async def test_not_initialized_raises_runtime_error(self) -> None:
        """state_manager or broker is None → RuntimeError."""
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
        scheduler.state_manager = None
        scheduler.broker = None

        with pytest.raises(RuntimeError, match='not properly initialized'):
            await scheduler._check_and_run_schedules()

    async def test_no_enabled_schedules_returns_early(self) -> None:
        """All disabled → returns without querying state."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='s',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                    enabled=False,
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.read_due_schedules = AsyncMock()
        scheduler.state_manager = state_manager
        scheduler.broker = _make_broker_mock()

        await scheduler._check_and_run_schedules()

        # The due read is not sent since no schedule is enabled
        state_manager.read_due_schedules.assert_not_awaited()

    async def test_iterates_due_states_and_checks_each(self) -> None:
        """Two due states → _check_schedule called for each."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='sched_a',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
                TaskSchedule(
                    name='sched_b',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.heal_missing_states = AsyncMock(return_value=())
        state_manager.read_due_schedules = AsyncMock(
            return_value=_due_read('sched_a', 'sched_b'),
        )
        scheduler.state_manager = state_manager
        scheduler.broker = _make_broker_mock()

        checked: list[tuple[str, object]] = []

        async def _track_check(schedule: object, due: object, now: object) -> None:
            checked.append((schedule.name, now))  # type: ignore[union-attr]
            assert due == DueSchedule(schedule.name, DATABASE_NOW)  # type: ignore[union-attr]

        scheduler._check_schedule = AsyncMock(side_effect=_track_check)  # type: ignore[method-assign]

        await scheduler._check_and_run_schedules()

        # Each due schedule is checked at the due read's database time.
        assert checked == [('sched_a', DATABASE_NOW), ('sched_b', DATABASE_NOW)]

    async def test_check_schedule_error_does_not_block_others(self) -> None:
        """First _check_schedule raises → second still called."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='sched_a',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
                TaskSchedule(
                    name='sched_b',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.heal_missing_states = AsyncMock(return_value=())
        state_manager.read_due_schedules = AsyncMock(
            return_value=_due_read('sched_a', 'sched_b'),
        )
        scheduler.state_manager = state_manager
        scheduler.broker = _make_broker_mock()

        checked: list[str] = []

        async def _fail_first(schedule: TaskSchedule, due: object, now: object) -> None:
            name = schedule.name
            checked.append(name)
            if name == 'sched_a':
                raise RuntimeError('simulated failure')

        scheduler._check_schedule = AsyncMock(side_effect=_fail_first)  # type: ignore[method-assign]

        await scheduler._check_and_run_schedules()

        assert checked == ['sched_a', 'sched_b']


# =============================================================================
# _compute_config_hash — serialization failure
# =============================================================================


@pytest.mark.unit
class TestComputeConfigHashSerializationFailure:
    """Tests for _compute_config_hash when dumps_json fails."""

    def test_serialization_failure_raises_runtime_error(self) -> None:
        """dumps_json returning Err raises RuntimeError."""
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

        with patch(
            'horsies.core.codec.json_io.dumps_json',
            return_value=Err(ValueError('bad json')),
        ):
            with pytest.raises(RuntimeError, match='Failed to serialize config'):
                scheduler._compute_config_hash(config.schedules[0])


# =============================================================================
# _check_schedule — the outcomes of one fire
# =============================================================================


_FIVE_SECONDS = IntervalSchedule(seconds=5)


def _schedule(*, catch_up_missed: bool = False, max_catch_up_runs: int = 100) -> TaskSchedule:
    return TaskSchedule(
        name='s',
        task_name='my_task',
        pattern=_FIVE_SECONDS,
        catch_up_missed=catch_up_missed,
        max_catch_up_runs=max_catch_up_runs,
    )


_DUE = DueSchedule('s', _utc(2025, 6, 1, 12, 0, 0))


def _refusal() -> Err[ScheduleFireRefused]:
    return Err(
        ScheduleFireRefused(
            schedule_name='s',
            cause=DBAPIError('SELECT', {}, psycopg.errors.CheckViolation('check')),
        ),
    )


def _unknown() -> Err[ScheduleFireOutcomeUnknown]:
    return Err(
        ScheduleFireOutcomeUnknown(
            schedule_name='s',
            cause=OperationalError('SELECT', {}, Exception('connection lost')),
        ),
    )


@pytest.mark.unit
@pytest.mark.asyncio
class TestCheckScheduleOutcomes:
    """One test per outcome of the fire, and the slots it is sent."""

    async def test_not_initialized_raises_runtime_error(self) -> None:
        """state_manager=None → RuntimeError."""
        schedule = _schedule()
        scheduler = Scheduler(_make_app(schedule_config=ScheduleConfig(schedules=[schedule])))
        scheduler.state_manager = None
        scheduler.broker = None

        with pytest.raises(RuntimeError, match='not properly initialized'):
            await scheduler._check_schedule(schedule, _DUE, _utc(2025, 6, 1, 12, 0, 0))

    async def test_a_tick_before_start_raises_with_the_schedule_name(self) -> None:
        """A tick before start() is a defect, not a refused slot."""
        schedule = _schedule()
        scheduler, state_manager = _fire_harness(schedule)
        scheduler._prepared_schedules = {}
        scheduler._commands = {}

        with pytest.raises(RuntimeError, match="Schedule 's' has no prepared wire text"):
            await scheduler._check_schedule(schedule, _DUE, _utc(2025, 6, 1, 12, 0, 3))

        state_manager.fire_schedule.assert_not_awaited()

    async def test_a_catch_up_sends_every_due_slot_in_one_fire(self) -> None:
        schedule = _schedule(catch_up_missed=True)
        slots = (_utc(2025, 6, 1, 12, 0, 0), _utc(2025, 6, 1, 12, 0, 5), _utc(2025, 6, 1, 12, 0, 10))
        scheduler, state_manager = _fire_harness(schedule, _advanced(*slots))

        await scheduler._check_schedule(schedule, _DUE, _utc(2025, 6, 1, 12, 0, 12))

        [fire] = _fires(state_manager)
        assert fire.slot_ats == slots
        assert fire.next_run_at == _utc(2025, 6, 1, 12, 0, 15)

    async def test_a_catch_up_sends_at_most_max_catch_up_runs_slots(self) -> None:
        schedule = _schedule(catch_up_missed=True, max_catch_up_runs=2)
        slots = (_utc(2025, 6, 1, 12, 0, 0), _utc(2025, 6, 1, 12, 0, 5))
        scheduler, state_manager = _fire_harness(schedule, _advanced(*slots))

        await scheduler._check_schedule(schedule, _DUE, _utc(2025, 6, 1, 12, 0, 12))

        [fire] = _fires(state_manager)
        assert fire.slot_ats == slots
        # The rest stays due: the row moves to the slot after the last one sent.
        assert fire.next_run_at == _utc(2025, 6, 1, 12, 0, 10)

    async def test_each_slot_of_an_advanced_fire_is_logged_by_its_outcome(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        schedule = _schedule(catch_up_missed=True)
        slots = (_utc(2025, 6, 1, 12, 0, 0), _utc(2025, 6, 1, 12, 0, 5), _utc(2025, 6, 1, 12, 0, 10))
        answer = Ok(
            ScheduleAdvanced(
                slots=(
                    FiredSlot(slots[0], 'id-0', SlotOutcome.SAME_COMMAND),
                    FiredSlot(slots[1], 'id-1', SlotOutcome.CONFLICT_UNVERIFIABLE),
                    FiredSlot(slots[2], 'id-2', SlotOutcome.INSERTED),
                ),
            ),
        )
        scheduler, _ = _fire_harness(schedule, answer)

        with caplog.at_level(logging.INFO, logger='horsies.scheduler'):
            await scheduler._check_schedule(schedule, _DUE, _utc(2025, 6, 1, 12, 0, 12))

        by_level = [(record.levelno, record.getMessage()) for record in caplog.records]
        assert any(level == logging.INFO and 'already has its task id-0' in m for level, m in by_level)
        assert any(level == logging.ERROR and 'not recorded as a run' in m for level, m in by_level)
        assert any(level == logging.INFO and 'enqueued task id-2' in m for level, m in by_level)

    async def test_a_moved_row_writes_nothing_more(self) -> None:
        scheduler, state_manager = _fire_harness(_schedule(), Ok(ScheduleStateMoved()))

        await scheduler._check_schedule(_schedule(), _DUE, _utc(2025, 6, 1, 12, 0, 3))

        state_manager.fire_schedule.assert_awaited_once()
        state_manager.heal_missing_states.assert_not_awaited()

    async def test_an_unknown_outcome_is_logged_and_left_to_the_next_tick(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        scheduler, state_manager = _fire_harness(_schedule(), _unknown())

        with caplog.at_level(logging.WARNING, logger='horsies.scheduler'):
            await scheduler._check_schedule(_schedule(), _DUE, _utc(2025, 6, 1, 12, 0, 3))

        state_manager.fire_schedule.assert_awaited_once()
        assert any('transient' in record.getMessage() for record in caplog.records)

    async def test_a_refused_latest_slot_moves_the_row_past_it_with_no_run(self) -> None:
        scheduler, state_manager = _fire_harness(_schedule(), _refusal(), _advanced())

        await scheduler._check_schedule(_schedule(), _DUE, _utc(2025, 6, 1, 12, 0, 3))

        first, skip = _fires(state_manager)
        assert first.slot_ats == (_utc(2025, 6, 1, 12, 0, 0),)
        assert (skip.slot_ats, skip.expected_next_run_at, skip.next_run_at) == (
            (),
            _utc(2025, 6, 1, 12, 0, 0),
            _utc(2025, 6, 1, 12, 0, 5),
        )

    async def test_a_refused_catch_up_moves_the_row_past_its_first_slot(self) -> None:
        """Python moved past the slot it was refused at; one statement is
        refused for all its slots, so the row moves past the first one and
        the next tick tries the rest."""
        schedule = _schedule(catch_up_missed=True)
        scheduler, state_manager = _fire_harness(schedule, _refusal(), _advanced())

        await scheduler._check_schedule(schedule, _DUE, _utc(2025, 6, 1, 12, 0, 12))

        first, skip = _fires(state_manager)
        assert len(first.slot_ats) == 3
        assert (skip.slot_ats, skip.next_run_at) == ((), _utc(2025, 6, 1, 12, 0, 5))

    async def test_a_failed_move_past_a_refusal_is_logged_for_the_next_tick(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        scheduler, state_manager = _fire_harness(_schedule(), _refusal(), _unknown())

        with caplog.at_level(logging.ERROR, logger='horsies.scheduler'):
            await scheduler._check_schedule(_schedule(), _DUE, _utc(2025, 6, 1, 12, 0, 3))

        assert state_manager.fire_schedule.await_count == 2
        assert any('could not move past' in record.getMessage() for record in caplog.records)

    async def test_a_broken_precondition_raises_to_the_tick_seam(self) -> None:
        cause = DBAPIError('SELECT', {}, psycopg.errors.InvalidParameterValue('slots'))
        scheduler, _ = _fire_harness(_schedule(), cause)

        with pytest.raises(DBAPIError) as raised:
            await scheduler._check_schedule(_schedule(), _DUE, _utc(2025, 6, 1, 12, 0, 3))

        assert raised.value is cause


# =============================================================================
# _calculate_missed_runs — additional paths
# =============================================================================


@pytest.mark.unit
class TestCalculateMissedRunsAdditionalPaths:
    """Additional tests for Scheduler._calculate_missed_runs."""

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

    def test_future_first_due_returns_empty(self) -> None:
        """first_due_run > current_time → empty list."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
        )
        first_due = _utc(2025, 6, 1, 13, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 0)

        result = scheduler._calculate_missed_runs(schedule, first_due, now, max_runs=10)

        assert result == []

    def test_non_monotonic_next_run_breaks(self) -> None:
        """calculate_next_run returning same time → logs error, breaks."""
        scheduler = self._make_scheduler()
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
        )
        first_due = _utc(2025, 6, 1, 12, 0, 0)
        now = _utc(2025, 6, 1, 12, 0, 20)

        with patch(
            'tests.dualrun.reference.scheduler_loop_caller.calculate_next_run',
            return_value=first_due,  # non-monotonic: same as cursor
        ):
            result = scheduler._calculate_missed_runs(schedule, first_due, now, max_runs=10)

        # Only the first slot, then break due to non-monotonic
        assert result == [first_due]


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

        with patch('tests.dualrun.reference.scheduler_loop_caller.logger') as mock_logger:
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
        """Module ending with .py → import_by_path called."""
        scheduler = self._make_scheduler()
        scheduler.app.get_discovered_task_modules = MagicMock(
            return_value=['tasks/my_tasks.py'],
        )

        with patch('tests.dualrun.reference.scheduler_loop_caller.import_by_path') as mock_import:
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

        with patch('tests.dualrun.reference.scheduler_loop_caller.importlib.import_module') as mock_import:
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
            'tests.dualrun.reference.scheduler_loop_caller.importlib.import_module',
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
# _build_commands: the send command of each schedule, built at start
# =============================================================================


def _started_with_real_broker(
    schedule: TaskSchedule,
    fn: object,
) -> tuple[Scheduler, MagicMock]:
    """A scheduler whose start-time text and command come from a real broker (no connection)."""
    app = _make_app(
        schedule_config=ScheduleConfig(schedules=[schedule]),
        tasks={'my_task': _make_task_mock(fn)},
    )
    scheduler = Scheduler(app)
    broker = _real_broker()
    scheduler.broker = broker
    scheduler._validate_schedules()
    scheduler._build_commands(broker)
    state_manager = MagicMock()
    state_manager.fire_schedule = AsyncMock(return_value=_advanced())
    scheduler.state_manager = state_manager
    return scheduler, state_manager


@pytest.mark.unit
@pytest.mark.asyncio
class TestScheduleCommand:
    """The command that every fire of a schedule sends."""

    async def test_a_value_refused_after_boot_is_sent_as_boot_encoded_it(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Regression: a validator that reads the clock accepts a value at boot
        and refuses it later. The tick does not parse the value again, so the
        slot does not wedge; the worker refuses the value when it decodes."""
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'deadline': 'soon'},
        )
        scheduler, state_manager = _started_with_real_broker(schedule, _deadline_task)

        monkeypatch.setattr(_Deadline, 'passed', True)
        with pytest.raises(ValueValidationError, match='deadline has passed'):
            encode_kwargs_read_back(_deadline_task, schedule.kwargs)

        await scheduler._check_schedule(schedule, _DUE, _utc(2025, 6, 1, 12, 0, 3))

        [fire] = _fires(state_manager)
        assert fire.command.stored_input == _json_text({'deadline': 'soon'})

    async def test_a_fired_slot_stamps_the_class_of_the_queue_period(self) -> None:
        """A cron fire resolves through the queue mapping, like a send."""
        from datetime import timedelta

        from horsies.core.models.retention import RetentionConfig

        schedule = TaskSchedule(name='s', task_name='my_task', pattern=IntervalSchedule(seconds=5))

        def no_kwargs_fn() -> None:
            return None

        app = _make_app(
            schedule_config=ScheduleConfig(schedules=[schedule]),
            tasks={'my_task': _make_task_mock(no_kwargs_fn)},
        )
        app.config.retention = RetentionConfig(
            queue_retention={'default': timedelta(days=7)},
        )
        scheduler = Scheduler(app)
        broker = _real_broker()
        scheduler._validate_schedules()
        scheduler._build_commands(broker)

        assert scheduler._commands['s'].retention_class_key == 'standard_7d'

    async def test_the_fingerprint_covers_the_text_boot_encoded(self) -> None:
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'value': 5},
        )

        def value_fn(*, value: int) -> None:
            return None

        scheduler, _ = _started_with_real_broker(schedule, value_fn)
        wire_text = _json_text({'value': 5})
        match _real_broker().prepare_send_command(
            task_name='my_task',
            queue_name='default',
            priority=100,
            input_json=wire_text,
            task_options=None,
        ):
            case Ok(expected):
                pass
            case Err(refused):
                pytest.fail(f'the command was refused: {refused}')

        assert scheduler._commands['s'] == expected

    async def test_stored_kwargs_warn_at_the_tick(self) -> None:
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'value': 5},
        )

        def value_fn(*, value: int) -> None:
            return None

        scheduler, _ = _started_with_real_broker(schedule, value_fn)

        with patch('tests.dualrun.reference.scheduler_loop_caller.warn_if_payload_large') as warn:
            await scheduler._check_schedule(schedule, _DUE, _utc(2025, 6, 1, 12, 0, 3))

        warn.assert_called_once_with(
            scheduler.app.config.payload,
            task_name='my_task',
            kind='kwargs',
            payload_text=_json_text({'value': 5}),
        )

    async def test_a_fire_sends_the_command_of_the_schedule(self) -> None:
        schedule = TaskSchedule(
            name='s',
            task_name='my_task',
            pattern=IntervalSchedule(seconds=5),
            kwargs={'a': 1, 'b': 2, 'key': 'val'},
        )

        def scheduled(*, a: int, b: int, key: str) -> None:
            _ = a, b, key

        scheduler, state_manager = _started_with_real_broker(schedule, scheduled)

        await scheduler._check_schedule(schedule, _DUE, _utc(2025, 6, 1, 12, 0, 3))

        [fire] = _fires(state_manager)
        assert fire.slot_ats == (_utc(2025, 6, 1, 12, 0, 0),)
        assert (fire.command.task_name, fire.command.queue_name, fire.command.priority) == (
            'my_task', 'default', 100,
        )
        assert fire.command.stored_input == _json_text({'a': 1, 'b': 2, 'key': 'val'})

    async def test_no_kwargs_store_an_empty_object_and_do_not_warn(self) -> None:
        schedule = TaskSchedule(name='s', task_name='my_task', pattern=IntervalSchedule(seconds=5))
        scheduler, state_manager = _started_with_real_broker(schedule, lambda: 'ok')

        with patch('tests.dualrun.reference.scheduler_loop_caller.warn_if_payload_large') as warn:
            await scheduler._check_schedule(schedule, _DUE, _utc(2025, 6, 1, 12, 0, 3))

        [fire] = _fires(state_manager)
        assert fire.command.stored_input == '{}'
        warn.assert_not_called()

    async def test_a_command_the_broker_refuses_fails_start_with_the_schedule_name(self) -> None:
        schedule = TaskSchedule(name='s', task_name='my_task', pattern=IntervalSchedule(seconds=5))
        app = _make_app(
            schedule_config=ScheduleConfig(schedules=[schedule]),
            tasks={'my_task': _make_task_mock(lambda: 'ok')},
        )
        scheduler = Scheduler(app)
        scheduler._validate_schedules()
        broker = MagicMock()
        broker.prepare_send_command = MagicMock(
            return_value=Err(BrokerInvalidInput(message='input refused', exception=NoCause())),
        )

        with pytest.raises(ConfigurationError) as raised:
            scheduler._build_commands(broker)

        assert raised.value.code == ErrorCode.CONFIG_INVALID_SCHEDULE
        assert "schedule 's'" in raised.value.message
        assert raised.value.notes == ['input refused']


# =============================================================================
# Remaining coverage gap tests
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestCheckAndRunSchedulesOrphanDueState:
    """Cover the 'continue' when due state has no matching enabled schedule (L263)."""

    async def test_due_state_without_matching_schedule_skipped(self) -> None:
        """Due state for unknown schedule_name → skipped via continue."""
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='real',
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)

        state_manager = MagicMock()
        state_manager.heal_missing_states = AsyncMock(return_value=())
        state_manager.read_due_schedules = AsyncMock(
            return_value=_due_read('deleted_schedule', 'real'),
        )
        scheduler.state_manager = state_manager
        scheduler.broker = _make_broker_mock()

        checked: list[str] = []

        async def _track(schedule: object, due: object, now: object) -> None:
            checked.append(schedule.name)  # type: ignore[union-attr]

        scheduler._check_schedule = AsyncMock(side_effect=_track)  # type: ignore[method-assign]

        await scheduler._check_and_run_schedules()

        # Only 'real' should be checked, orphan is skipped
        assert checked == ['real']


@pytest.mark.unit
@pytest.mark.asyncio
class TestEnsureStatesExist:
    """Tick-loop self-heal: missing state rows are re-initialized.

    Regression for the startup-init liveness gap: a schedule whose row
    creation failed at start() was invisible to the due read and stayed
    dormant until restart.
    """

    def _scheduler_with_mock_manager(
        self, names: list[str],
    ) -> tuple[Scheduler, MagicMock]:
        config = ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name=name,
                    task_name='my_task',
                    pattern=IntervalSchedule(seconds=5),
                )
                for name in names
            ],
        )
        app = _make_app(schedule_config=config)
        scheduler = Scheduler(app)
        state_manager = MagicMock()
        scheduler.state_manager = state_manager
        scheduler.broker = _make_broker_mock()
        return scheduler, state_manager

    async def test_missing_row_created_by_the_tick(self) -> None:
        """The heal sends one row per enabled schedule; the statement
        creates the missing ones."""
        scheduler, state_manager = self._scheduler_with_mock_manager(
            ['healthy', 'dormant'],
        )
        state_manager.heal_missing_states = AsyncMock(return_value=('dormant',))
        state_manager.read_due_schedules = AsyncMock(return_value=_due_read())

        await scheduler._check_and_run_schedules()

        assert _written_names(state_manager.heal_missing_states) == ['dormant', 'healthy']
        state_manager.read_due_schedules.assert_awaited_once()

    async def test_first_runs_come_from_the_due_read_database_time(self) -> None:
        """Each row of the heal gets the first run after the due read's
        database time, not after the host clock."""
        scheduler, state_manager = self._scheduler_with_mock_manager(['a'])
        state_manager.heal_missing_states = AsyncMock(return_value=('a',))
        state_manager.read_due_schedules = AsyncMock(return_value=_due_read())

        await scheduler._check_and_run_schedules()

        writes: list[ScheduleStateWrite] = state_manager.heal_missing_states.await_args.args[0]
        schedule = scheduler.schedule_config.schedules[0]
        assert writes == [
            ScheduleStateWrite(
                schedule_name='a',
                next_run_at=calculate_next_run(schedule.pattern, DATABASE_NOW, 'UTC'),
                config_hash=scheduler._compute_config_hash(schedule),
            ),
        ]

    async def test_calculation_failure_logged_and_tick_proceeds(self) -> None:
        """A schedule whose first run cannot be computed is left out; the
        others are healed and the tick goes on to its due schedules."""
        scheduler, state_manager = self._scheduler_with_mock_manager(
            ['bad', 'also_missing'],
        )
        state_manager.heal_missing_states = AsyncMock(return_value=('bad',))
        state_manager.read_due_schedules = AsyncMock(return_value=_due_read())

        with patch(
            'tests.dualrun.reference.scheduler_loop_caller.calculate_next_run',
            side_effect=_fail_on_call(1),
        ):
            await scheduler._check_and_run_schedules()

        # Sorted order: 'also_missing' failed, 'bad' was sent.
        assert _written_names(state_manager.heal_missing_states) == ['bad']
        assert scheduler._ticks_until_existence_check == 0

    async def test_heal_failure_raises_to_tick_seam(self) -> None:
        """A DB error of the heal escapes to run_forever's per-tick handler
        (the whole tick is retried next interval). The due read ran first."""
        scheduler, state_manager = self._scheduler_with_mock_manager(['a'])
        state_manager.heal_missing_states = AsyncMock(side_effect=RuntimeError('db down'))
        state_manager.read_due_schedules = AsyncMock(return_value=_due_read('a'))
        scheduler._check_schedule = AsyncMock()  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match='db down'):
            await scheduler._check_and_run_schedules()

        state_manager.read_due_schedules.assert_awaited_once()
        scheduler._check_schedule.assert_not_awaited()

    async def test_healthy_check_skipped_until_interval_elapses(self) -> None:
        """While healthy, only the first tick heals; subsequent ticks inside
        the interval skip it. The due read runs every tick."""
        scheduler, state_manager = self._scheduler_with_mock_manager(['a'])
        state_manager.heal_missing_states = AsyncMock(return_value=())
        state_manager.read_due_schedules = AsyncMock(return_value=_due_read())

        for _tick in range(5):
            await scheduler._check_and_run_schedules()

        assert state_manager.heal_missing_states.await_count == 1
        assert state_manager.read_due_schedules.await_count == 5

    async def test_healthy_check_reruns_after_interval(self) -> None:
        """The heal runs again once the tick interval elapses."""
        scheduler, state_manager = self._scheduler_with_mock_manager(['a'])
        state_manager.heal_missing_states = AsyncMock(return_value=())
        state_manager.read_due_schedules = AsyncMock(return_value=_due_read())

        for _tick in range(scheduler._existence_check_interval_ticks + 1):
            await scheduler._check_and_run_schedules()

        assert state_manager.heal_missing_states.await_count == 2

    async def test_interval_ticks_derived_from_check_interval(self) -> None:
        """The tick count preserves the ~60 s wall-clock cadence across
        tick configs; a tick longer than the target floors at 1."""
        for seconds, expected_ticks in ((1, 60), (10, 6), (60, 1)):
            config = ScheduleConfig(
                schedules=[
                    TaskSchedule(
                        name='a',
                        task_name='my_task',
                        pattern=IntervalSchedule(seconds=5),
                    ),
                ],
                check_interval_seconds=seconds,
            )
            app = _make_app(schedule_config=config)
            scheduler = Scheduler(app)
            assert (
                scheduler._existence_check_interval_ticks == expected_ticks
            ), f'check_interval_seconds={seconds}'

    async def test_unhealthy_pass_rechecks_next_tick(self) -> None:
        """A failed first-run calculation keeps the heal running every tick
        until a fully healthy pass, then the interval cadence resumes."""
        scheduler, state_manager = self._scheduler_with_mock_manager(
            ['flaky', 'ok'],
        )
        state_manager.heal_missing_states = AsyncMock(return_value=())
        state_manager.read_due_schedules = AsyncMock(return_value=_due_read())

        # Tick 1: one calculation fails -> unhealthy, heal again next tick.
        # Tick 2: every calculation succeeds -> healthy, cadence resumes.
        # Ticks 3-4: inside the interval -> no heal.
        with patch(
            'tests.dualrun.reference.scheduler_loop_caller.calculate_next_run',
            side_effect=_fail_on_call(1),
        ):
            for _tick in range(4):
                await scheduler._check_and_run_schedules()

        assert state_manager.heal_missing_states.await_count == 2

    async def test_heal_failure_reruns_on_retried_tick(self) -> None:
        """A raise in the heal leaves the countdown armed: the retried tick
        runs the heal again instead of skipping it."""
        scheduler, state_manager = self._scheduler_with_mock_manager(['a'])
        state_manager.heal_missing_states = AsyncMock(
            side_effect=[RuntimeError('db down'), ()],
        )
        state_manager.read_due_schedules = AsyncMock(return_value=_due_read())

        with pytest.raises(RuntimeError, match='db down'):
            await scheduler._check_and_run_schedules()
        await scheduler._check_and_run_schedules()

        assert state_manager.heal_missing_states.await_count == 2
        assert state_manager.read_due_schedules.await_count == 2


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
                'tests.dualrun.reference.scheduler_loop_caller.importlib.import_module',
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
                'tests.dualrun.reference.scheduler_loop_caller.import_by_path',
                side_effect=ImportError('cannot import'),
            ),
            patch(
                'tests.dualrun.reference.scheduler_loop_caller.importlib.import_module',
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
                    'tests.dualrun.reference.scheduler_loop_caller.importlib.import_module',
                ),
            ):
                scheduler._preload_task_modules()

            assert fake_cwd in sys.path
        finally:
            sys.path = original_sys_path


@pytest.mark.unit
class TestValidateSchedulesCallsSignatureValidation:
    """Cover L588: _validate_schedules calls _validate_schedule_signature."""

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
        scheduler._validate_schedules()
