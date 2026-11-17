# pyright: reportPrivateUsage=false
"""A scheduled run carries the options of its task, as a plain send does.

The worker reads the timeout, the retry policy, the execution class and the
cancel grace from the stored ``task_options`` of the row. These tests fire
slots through ``Scheduler._check_schedule`` of the Python loop (the dual-run
reference, ``tests/dualrun/reference/scheduler_loop_caller.py``) against a state manager double
that records the fire, with tasks that a real app registers and a real
broker that builds the send command (it opens no connection), so the
options text is the one the task decorator encodes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import Json, SerializationError, loads_json
from horsies.core.errors import ErrorCode, RegistryError
from horsies.core.history.periods import RetentionClassKey
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.schedule import (
    IntervalSchedule,
    ScheduleConfig,
    TaskSchedule,
)
from horsies.core.models.tasks import Execution, RetryPolicy, TaskError, TaskResult
from tests.dualrun.reference.scheduler_loop_caller import Scheduler
from horsies.core.scheduler.state import (
    DueSchedule,
    FiredSlot,
    ScheduleAdvanced,
    ScheduleFire,
    SlotOutcome,
)
from horsies.core.types.result import Err, Ok
from horsies.core.utils.fingerprint import schedule_slot_task_id

TASK_NAME = 'scheduled_task'
SCHEDULE_NAME = 'every_five_seconds'
SLOT = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
URL = 'postgresql://user:pass@localhost/db'


def _schedule(*, catch_up_missed: bool = False) -> TaskSchedule:
    return TaskSchedule(
        name=SCHEDULE_NAME,
        task_name=TASK_NAME,
        pattern=IntervalSchedule(seconds=5),
        kwargs={'value': 1},
        catch_up_missed=catch_up_missed,
    )


def _real_app(schedule: TaskSchedule) -> Horsies:
    """A real app with one schedule. Nothing here opens a connection."""
    return Horsies(
        config=AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql://user:pass@localhost/db'),
            ),
            schedule=ScheduleConfig(schedules=[schedule]),
        ),
    )


@dataclass(frozen=True)
class _Fired:
    """A started scheduler and the fire that its state manager double records."""

    scheduler: Scheduler
    state_manager: MagicMock

    def fire(self) -> ScheduleFire:
        call = self.state_manager.fire_schedule.await_args
        assert call is not None, 'no fire was sent'
        fire = call.args[0]
        assert isinstance(fire, ScheduleFire)
        return fire


def _started(app: Horsies | MagicMock, answer: ScheduleAdvanced | None = None) -> _Fired:
    """A scheduler with a real broker (no connection), the start-time text and
    commands, and a state manager whose fire answers ``answer``."""
    scheduler = Scheduler(app)  # pyright: ignore[reportArgumentType]
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(URL)))
    scheduler.broker = broker
    scheduler._validate_schedules()
    scheduler._build_commands(broker)
    state_manager = MagicMock()
    state_manager.fire_schedule = AsyncMock(return_value=Ok(answer or ScheduleAdvanced(slots=())))
    scheduler.state_manager = state_manager
    return _Fired(scheduler=scheduler, state_manager=state_manager)


async def _fire_at(fired: _Fired, schedule: TaskSchedule, database_now: datetime) -> ScheduleFire:
    """Fire ``schedule`` due at SLOT at ``database_now``; return the fire that was sent."""
    await fired.scheduler._check_schedule(schedule, DueSchedule(schedule.name, SLOT), database_now)
    return fired.fire()


def _register_isolated(app: Horsies) -> str | None:
    @app.task(
        TASK_NAME,
        timeout_ms=30_000,
        retry_policy=RetryPolicy.fixed([1, 2], auto_retry_for=['TASK_EXCEPTION']),
    )
    def scheduled_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    return scheduled_task.task_options_json


def _register_shared_async(app: Horsies) -> str | None:
    @app.task(
        TASK_NAME,
        execution=Execution.SHARED_ASYNC,
        timeout_ms=5_000,
        cancel_grace_ms=2_000,
    )
    async def scheduled_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    return scheduled_task.task_options_json


def _register_shared_thread(app: Horsies) -> str | None:
    @app.task(TASK_NAME, execution=Execution.SHARED_THREAD, timeout_ms=5_000)
    def scheduled_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    return scheduled_task.task_options_json


def _stored_options(options_json: object) -> dict[str, Json]:
    """Decode a stored options text; fail the test for anything but an object."""
    assert isinstance(options_json, str)
    match loads_json(options_json):
        case Ok(dict() as decoded):
            return decoded
        case other:
            pytest.fail(f'stored options are not a JSON object: {other!r}')


# =============================================================================
# The stored text
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestScheduledRunStoresTheTaskOptions:
    """The fire of a slot stores the options text of the task."""

    @pytest.mark.parametrize(
        ('register', 'expected'),
        [
            pytest.param(
                _register_isolated,
                {
                    'timeout_ms': 30_000,
                    'execution': 'ISOLATED',
                    'cancel_grace_ms': None,
                    'good_until': None,
                },
                id='isolated-timeout-retry',
            ),
            pytest.param(
                _register_shared_async,
                {
                    'timeout_ms': 5_000,
                    'execution': 'SHARED_ASYNC',
                    'cancel_grace_ms': 2_000,
                    'good_until': None,
                },
                id='shared-async-grace',
            ),
            pytest.param(
                _register_shared_thread,
                {
                    'timeout_ms': 5_000,
                    'execution': 'SHARED_THREAD',
                    'cancel_grace_ms': None,
                    'good_until': None,
                },
                id='shared-thread',
            ),
        ],
    )
    async def test_a_slot_stores_the_options_of_its_task(
        self,
        register: Any,
        expected: dict[str, Any],
    ) -> None:
        """Regression: a slot was enqueued with no options, so it ran with no
        timeout, no retries, and as ISOLATED."""
        schedule = _schedule()
        app = _real_app(schedule)
        task_options_json = register(app)
        fired = _started(app)

        fire = await _fire_at(fired, schedule, SLOT + timedelta(seconds=3))

        assert fire.slot_ats == (SLOT,)
        assert fire.command.task_options == task_options_json
        assert fire.command.execution.value == expected['execution']
        stored = _stored_options(fire.command.task_options)
        for key, value in expected.items():
            assert stored[key] == value, key

    async def test_the_retry_policy_of_the_task_is_stored(self) -> None:
        schedule = _schedule()
        app = _real_app(schedule)
        _ = _register_isolated(app)
        fired = _started(app)

        fire = await _fire_at(fired, schedule, SLOT + timedelta(seconds=3))

        assert fire.command.max_retries == 2
        stored = _stored_options(fire.command.task_options)
        retry_policy = stored['retry_policy']
        assert isinstance(retry_policy, dict)
        assert retry_policy['max_retries'] == 2
        assert retry_policy['intervals'] == [1, 2]
        assert retry_policy['auto_retry_for'] == ['TASK_EXCEPTION']

    async def test_the_text_is_the_text_that_a_plain_send_stores(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        schedule = _schedule()
        app = _real_app(schedule)
        _ = _register_isolated(app)
        fired = _started(app)
        send_broker = MagicMock()
        send_broker.enqueue_async = AsyncMock(return_value=Ok('sent-task'))
        monkeypatch.setattr(app, 'get_broker', lambda: send_broker)

        fire = await _fire_at(fired, schedule, SLOT + timedelta(seconds=3))
        send_result = await app.tasks[TASK_NAME].send_async(value=1)

        assert isinstance(send_result, Ok)
        send_kwargs = send_broker.enqueue_async.await_args.kwargs
        assert fire.command.task_options is not None
        assert fire.command.task_options == send_kwargs['task_options']
        assert fire.command.stored_input == send_kwargs['input_json']
        assert send_kwargs['good_until'] is None

    async def test_every_slot_of_a_catch_up_sends_one_command(self) -> None:
        schedule = _schedule(catch_up_missed=True)
        app = _real_app(schedule)
        task_options_json = _register_isolated(app)
        fired = _started(app)

        fire = await _fire_at(fired, schedule, SLOT + timedelta(seconds=12))

        assert fire.slot_ats == (SLOT, SLOT + timedelta(seconds=5), SLOT + timedelta(seconds=10))
        assert fire.next_run_at == SLOT + timedelta(seconds=15)
        assert fire.command.task_options == task_options_json
        fired.state_manager.fire_schedule.assert_awaited_once()

    async def test_the_fingerprint_covers_the_options_text(self) -> None:
        """The slot fingerprint changes: the text that an earlier version
        fingerprinted had no options."""
        schedule = _schedule()
        app = _real_app(schedule)
        task_options_json = _register_isolated(app)
        fired = _started(app)

        fire = await _fire_at(fired, schedule, SLOT + timedelta(seconds=3))

        broker = PostgresBroker(PostgresConfig(database_url=SecretStr(URL)))

        def fingerprint(task_options: str | None) -> bytes:
            match broker.prepare_send_command(
                task_name=TASK_NAME,
                queue_name=fire.command.queue_name,
                priority=fire.command.priority,
                kwargs_json=fire.command.stored_input,
                task_options=task_options,
                retention_class_key=RetentionClassKey(fire.command.retention_class_key),
            ):
                case Ok(command):
                    return command.fingerprint
                case Err(refused):
                    pytest.fail(f'the command was refused: {refused}')

        assert fire.command.fingerprint == fingerprint(task_options_json)
        assert fire.command.fingerprint != fingerprint(None)


# =============================================================================
# Options that do not encode
# =============================================================================


@pytest.mark.unit
class TestOptionsThatDoNotEncode:
    """The decorator encodes the options once; a failure refuses the schedule at start."""

    def test_the_schedule_is_refused_at_start_with_subject_and_cause(self) -> None:
        schedule = _schedule()
        app = _real_app(schedule)

        with patch(
            'horsies.core.task_decorator.serialize_task_options',
            return_value=Err(SerializationError('int too large for JSON')),
        ):
            with pytest.raises(ValueError) as definition_error:
                _ = _register_isolated(app)

        assert TASK_NAME in str(definition_error.value)
        assert 'int too large for JSON' in str(definition_error.value)
        assert TASK_NAME not in app.tasks

        scheduler = Scheduler(app)
        with pytest.raises(RegistryError) as start_error:
            scheduler._validate_schedules()

        assert start_error.value.code == ErrorCode.TASK_NOT_REGISTERED
        assert TASK_NAME in start_error.value.message
        assert any(SCHEDULE_NAME in note for note in start_error.value.notes)


# =============================================================================
# A slot whose task id already holds another command (the upgrade slot)
# =============================================================================


def _mock_task(task_options_json: str | None) -> MagicMock:
    def scheduled_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    task = MagicMock()
    task._original_fn = scheduled_task
    task.task_queue_name = None
    task.task_options_json = task_options_json
    return task


def _mock_app(schedule: TaskSchedule) -> MagicMock:
    app = MagicMock()
    app.config.schedule = ScheduleConfig(schedules=[schedule])
    app.config.payload = _real_app(schedule).config.payload
    app.config.retention = None
    app.tasks = {TASK_NAME: _mock_task('{"timeout_ms":30000}')}
    app.validate_queue_name = MagicMock(return_value='default')
    app.config.queue_mode.name = 'DEFAULT'
    return app


def _slot(slot_at: datetime, outcome: SlotOutcome) -> FiredSlot:
    return FiredSlot(slot_at, str(schedule_slot_task_id(SCHEDULE_NAME, slot_at)), outcome)


@pytest.mark.unit
@pytest.mark.asyncio
class TestSlotWithAnotherCommand:
    """PAYLOAD_MISMATCH on a slot means the slot already has its task; the fire records it."""

    async def test_the_slot_counts_as_fired_and_one_warning_is_logged(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        schedule = _schedule()
        fired = _started(
            _mock_app(schedule),
            ScheduleAdvanced(slots=(_slot(SLOT, SlotOutcome.PAYLOAD_MISMATCH),)),
        )

        with caplog.at_level(logging.WARNING, logger='horsies.scheduler'):
            fire = await _fire_at(fired, schedule, SLOT + timedelta(seconds=3))

        assert (fire.slot_ats, fire.next_run_at) == ((SLOT,), SLOT + timedelta(seconds=5))
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert SCHEDULE_NAME in warnings[0].getMessage()
        assert 'counts as fired' in warnings[0].getMessage()

    async def test_catch_up_fires_the_slots_after_the_fired_slot_in_the_same_fire(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        schedule = _schedule(catch_up_missed=True)
        slots = (SLOT, SLOT + timedelta(seconds=5), SLOT + timedelta(seconds=10))
        fired = _started(
            _mock_app(schedule),
            ScheduleAdvanced(
                slots=(
                    _slot(slots[0], SlotOutcome.PAYLOAD_MISMATCH),
                    _slot(slots[1], SlotOutcome.INSERTED),
                    _slot(slots[2], SlotOutcome.INSERTED),
                ),
            ),
        )

        with caplog.at_level(logging.INFO, logger='horsies.scheduler'):
            fire = await _fire_at(fired, schedule, SLOT + timedelta(seconds=12))

        assert (fire.slot_ats, fire.next_run_at) == (slots, SLOT + timedelta(seconds=15))
        messages = [r.getMessage() for r in caplog.records]
        assert sum('counts as fired' in message for message in messages) == 1
        assert sum('enqueued task' in message for message in messages) == 2
