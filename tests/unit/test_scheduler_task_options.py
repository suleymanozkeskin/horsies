# pyright: reportPrivateUsage=false
"""A scheduled run carries the options of its task, as a plain send does.

The worker reads the timeout, the retry policy, the execution class and the
cancel grace from the stored ``task_options`` of the row. These tests fire
slots through ``Scheduler._enqueue_scheduled_task`` (and ``_check_schedule``
for the state update) against a broker mock, with tasks that a real app
registers, so the options text is the one the task decorator encodes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.result_types import (
    BrokerInvalidTaskOptions,
    BrokerPayloadMismatch,
)
from horsies.core.codec.json_io import Json, SerializationError, dumps_json, loads_json
from horsies.core.errors import ErrorCode, RegistryError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.schedule import (
    IntervalSchedule,
    ScheduleConfig,
    TaskSchedule,
)
from horsies.core.models.tasks import Execution, RetryPolicy, TaskError, TaskResult
from horsies.core.scheduler.service import ScheduleLockDatabase, Scheduler
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok
from horsies.core.utils.fingerprint import enqueue_fingerprint, schedule_slot_task_id
from tests.broker_resources_support import attach_resources, fake_resources

TASK_NAME = 'scheduled_task'
SCHEDULE_NAME = 'every_five_seconds'
SLOT = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


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
                database_url=SecretStr('postgresql+psycopg://user:pass@localhost/db'),
            ),
            schedule=ScheduleConfig(schedules=[schedule]),
        ),
    )


def _broker_mock(enqueue_result: object) -> MagicMock:
    """A broker mock whose enqueue returns ``enqueue_result``."""
    broker = MagicMock()
    broker.session_factory = MagicMock()
    attach_resources(broker, fake_resources(session_factory=broker.session_factory))
    broker.enqueue_async = AsyncMock(return_value=enqueue_result)
    return broker


def _started_scheduler(app: Horsies, broker: MagicMock) -> Scheduler:
    """A scheduler with the broker mock and the text that start() prepares."""
    scheduler = Scheduler(app)
    scheduler.broker = broker
    scheduler._validate_schedules()
    return scheduler


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


def _json_text(value: Json) -> str:
    """The JSON text of a value that the strict encoder takes."""
    match dumps_json(value):
        case Ok(text):
            return text
        case Err(error):
            pytest.fail(f'value does not encode: {error}')


# =============================================================================
# The stored text
# =============================================================================


@pytest.mark.unit
@pytest.mark.asyncio
class TestScheduledRunStoresTheTaskOptions:
    """The enqueue of a slot stores the options text of the task."""

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
        broker = _broker_mock(Ok('task-1'))
        scheduler = _started_scheduler(app, broker)

        result = await scheduler._enqueue_scheduled_task(schedule, slot_time=SLOT)

        assert result == Ok('task-1')
        stored_text = broker.enqueue_async.await_args.kwargs['task_options']
        assert stored_text == task_options_json
        stored = _stored_options(stored_text)
        for key, value in expected.items():
            assert stored[key] == value, key

    async def test_the_retry_policy_of_the_task_is_stored(self) -> None:
        schedule = _schedule()
        app = _real_app(schedule)
        _ = _register_isolated(app)
        broker = _broker_mock(Ok('task-1'))
        scheduler = _started_scheduler(app, broker)

        _ = await scheduler._enqueue_scheduled_task(schedule, slot_time=SLOT)

        stored = _stored_options(broker.enqueue_async.await_args.kwargs['task_options'])
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
        scheduler_broker = _broker_mock(Ok('slot-task'))
        scheduler = _started_scheduler(app, scheduler_broker)
        send_broker = _broker_mock(Ok('sent-task'))
        monkeypatch.setattr(app, 'get_broker', lambda: send_broker)

        _ = await scheduler._enqueue_scheduled_task(schedule, slot_time=SLOT)
        send_result = await app.tasks[TASK_NAME].send_async(value=1)

        assert isinstance(send_result, Ok)
        slot_kwargs = scheduler_broker.enqueue_async.await_args.kwargs
        send_kwargs = send_broker.enqueue_async.await_args.kwargs
        assert slot_kwargs['task_options'] is not None
        assert slot_kwargs['task_options'] == send_kwargs['task_options']
        assert slot_kwargs['kwargs_json'] == send_kwargs['kwargs_json']
        assert send_kwargs['good_until'] is None

    async def test_every_slot_sends_the_same_text(self) -> None:
        schedule = _schedule()
        app = _real_app(schedule)
        task_options_json = _register_isolated(app)
        broker = _broker_mock(Ok('task-1'))
        scheduler = _started_scheduler(app, broker)
        later_slot = SLOT.replace(second=5)

        _ = await scheduler._enqueue_scheduled_task(schedule, slot_time=SLOT)
        _ = await scheduler._enqueue_scheduled_task(schedule, slot_time=later_slot)

        first, second = (call.kwargs for call in broker.enqueue_async.await_args_list)
        assert first['task_id'] != second['task_id']
        assert first['task_options'] == task_options_json
        assert second['task_options'] == task_options_json

    async def test_the_fingerprint_covers_the_options_text(self) -> None:
        """The slot digest changes: the text that an earlier version
        fingerprinted had no options."""
        schedule = _schedule()
        app = _real_app(schedule)
        task_options_json = _register_isolated(app)
        broker = _broker_mock(Ok('task-1'))
        scheduler = _started_scheduler(app, broker)

        _ = await scheduler._enqueue_scheduled_task(schedule, slot_time=SLOT)

        enqueue_kwargs = broker.enqueue_async.await_args.kwargs
        fingerprint_inputs: dict[str, Any] = {
            'task_name': TASK_NAME,
            'queue_name': enqueue_kwargs['queue_name'],
            'priority': enqueue_kwargs['priority'],
            'args_json': None,
            'kwargs_json': _json_text({'value': 1}),
            'sent_at': SLOT,
            'good_until': None,
            'enqueue_delay_seconds': None,
        }
        with_options = enqueue_fingerprint(
            **fingerprint_inputs,
            task_options=task_options_json,
        )
        without_options = enqueue_fingerprint(**fingerprint_inputs, task_options=None)
        assert enqueue_kwargs['enqueue_sha'] == with_options
        assert enqueue_kwargs['enqueue_sha'] != without_options


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


@dataclass(frozen=True)
class _Harness:
    """A started scheduler and the mocks that it talks to."""

    scheduler: Scheduler
    broker: MagicMock
    session: AsyncMock
    state_manager: MagicMock


def _harness(schedule: TaskSchedule, enqueue_results: list[object]) -> _Harness:
    """A started scheduler whose due state is SLOT; enqueues return the results in order."""
    session = AsyncMock()
    broker = MagicMock()
    broker.session_factory.return_value.__aenter__ = AsyncMock(return_value=session)
    broker.session_factory.return_value.__aexit__ = AsyncMock(return_value=False)
    broker.enqueue_async = AsyncMock(side_effect=enqueue_results)
    scheduler = Scheduler(_mock_app(schedule))
    scheduler.broker = broker
    scheduler._lock_database = ScheduleLockDatabase(
        engine=MagicMock(dispose=AsyncMock()),
        session_factory=broker.session_factory,
    )
    scheduler._validate_schedules()

    state = MagicMock()
    state.next_run_at = SLOT
    state_manager = MagicMock()
    state_manager.get_state = AsyncMock(return_value=state)
    state_manager.update_after_run = AsyncMock()
    state_manager.update_next_run = AsyncMock()
    scheduler.state_manager = state_manager
    return _Harness(
        scheduler=scheduler,
        broker=broker,
        session=session,
        state_manager=state_manager,
    )


def _mismatch() -> Err[BrokerPayloadMismatch]:
    return Err(
        BrokerPayloadMismatch(
            message='task_id already exists with different payload (sha mismatch)',
        ),
    )


@pytest.mark.unit
@pytest.mark.asyncio
class TestSlotWithAnotherCommand:
    """PAYLOAD_MISMATCH on a slot means the slot already has its task."""

    async def test_the_slot_counts_as_fired_and_one_warning_is_logged(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        schedule = _schedule()
        harness = _harness(schedule, [_mismatch()])

        with caplog.at_level(logging.WARNING, logger='horsies.scheduler'):
            result = await harness.scheduler._enqueue_scheduled_task(
                schedule,
                slot_time=SLOT,
            )

        assert result == Ok(schedule_slot_task_id(SCHEDULE_NAME, SLOT))
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert SCHEDULE_NAME in warnings[0].getMessage()
        assert 'counts as fired' in warnings[0].getMessage()

    async def test_another_permanent_error_is_returned_unchanged(self) -> None:
        schedule = _schedule()
        refused = Err(
            BrokerInvalidTaskOptions(
                message='task_options refused',
                exception=NoCause(),
            ),
        )
        harness = _harness(schedule, [refused])

        result = await harness.scheduler._enqueue_scheduled_task(
            schedule,
            slot_time=SLOT,
        )

        assert result == refused

    async def test_the_fired_slot_is_recorded_and_the_schedule_moves_on(self) -> None:
        schedule = _schedule()
        harness = _harness(schedule, [_mismatch()])

        await harness.scheduler._check_schedule(schedule, SLOT.replace(second=3))

        update_kwargs = harness.state_manager.update_after_run.await_args.kwargs
        assert update_kwargs['task_id'] == schedule_slot_task_id(SCHEDULE_NAME, SLOT)
        assert update_kwargs['next_run_at'] == SLOT.replace(second=5)
        harness.state_manager.update_next_run.assert_not_awaited()
        harness.session.commit.assert_awaited_once()
        harness.session.rollback.assert_not_awaited()

    async def test_catch_up_enqueues_the_slots_after_the_fired_slot(self) -> None:
        schedule = _schedule(catch_up_missed=True)
        harness = _harness(schedule, [_mismatch(), Ok('task-5'), Ok('task-10')])

        await harness.scheduler._check_schedule(schedule, SLOT.replace(second=12))

        assert harness.broker.enqueue_async.await_count == 3
        update_kwargs = harness.state_manager.update_after_run.await_args.kwargs
        assert update_kwargs['task_id'] == 'task-10'
        assert update_kwargs['next_run_at'] == SLOT.replace(second=15)
        harness.session.commit.assert_awaited_once()
        harness.session.rollback.assert_not_awaited()
