# pyright: reportPrivateUsage=false
"""The scheduler facade on ``horsies._core.CoreScheduler`` and its manifest.

``to_scheduler_manifest`` runs on a real app; the facade runs on a double of
``CoreScheduler`` whose answers each test sets, and on a broker double, so no
test connects.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import time
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies._catalog.scheduler_manifest import (
    MANIFEST_VERSION,
    ScheduleActivationDisabled,
    ScheduleActivationEnabled,
    ScheduleManifest,
    SchedulerManifest,
    ScheduleSendManifest,
    ScheduleTaskOptionsEncoded,
    ScheduleTaskOptionsNoOptions,
    dump_scheduler_manifest,
)
from horsies._core import (
    SchedulerStarted,
    SchedulerStartFailed,
    SchedulerStartRefused,
    SchedulerStopped,
)
from horsies.core.app import Horsies
from horsies.core.brokers.result_types import DatabaseErrorCode, DatabaseOperationFailed
from horsies.core.codec.json_io import SerializationError
from horsies.core.errors import ConfigurationError, HorsiesErrorCode, RegistryError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.retention import RetentionConfig
from horsies.core.models.schedule import (
    DailySchedule,
    IntervalSchedule,
    ScheduleConfig,
    TaskSchedule,
)
from horsies.core.models.tasks import OperationalErrorCode, RetryPolicy, TaskError
from horsies.core.scheduler.manifest import to_scheduler_manifest
from horsies.core.scheduler.service import Scheduler
from horsies.core.scheduler.validation import PreparedSchedule, prepare_schedules
from horsies.core.types.result import Err, Ok, Result

_DATABASE_URL = 'postgresql://user:pass@localhost/db'
_REPORT_TASK = 'reports.nightly'
_PLAIN_TASK = 'reports.plain'


def _app(
    *schedules: TaskSchedule,
    retention: RetentionConfig | None = None,
    retain_rerun_input: bool = False,
) -> Horsies:
    """An app with the two report tasks and `schedules`."""
    app = Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(
                database_url=SecretStr(_DATABASE_URL),
                retain_rerun_input_default=retain_rerun_input,
            ),
            retention=retention if retention is not None else RetentionConfig(),
            schedule=ScheduleConfig(check_interval_seconds=5, schedules=list(schedules)),
        ),
    )

    @app.task(
        _REPORT_TASK,
        timeout_ms=30_000,
        retry_policy=RetryPolicy.fixed([1, 2], auto_retry_for=[OperationalErrorCode.TASK_EXCEPTION]),
    )
    def nightly(*, day: str, limit: int) -> Result[int, TaskError]:
        return Ok(limit)

    @app.task(_PLAIN_TASK)
    def plain() -> Result[int, TaskError]:
        return Ok(1)

    _ = (nightly, plain)
    return app


def _nightly(**overrides: object) -> TaskSchedule:
    values: dict[str, object] = {
        'name': 'nightly',
        'task_name': _REPORT_TASK,
        'pattern': IntervalSchedule(seconds=60),
        'kwargs': {'day': 'monday', 'limit': 5},
        'timezone': 'Europe/Berlin',
        'catch_up_missed': True,
        'max_catch_up_runs': 7,
    }
    values.update(overrides)
    return TaskSchedule.model_validate(values)


def _plain(name: str = 'plain', *, enabled: bool = True) -> TaskSchedule:
    return TaskSchedule(
        name=name,
        task_name=_PLAIN_TASK,
        pattern=DailySchedule(time=time(9, 30)),
        enabled=enabled,
    )


def _manifest(app: Horsies) -> SchedulerManifest:
    schedule_config = app.config.schedule
    assert schedule_config is not None
    return to_scheduler_manifest(app, schedule_config, _prepared(app))


def _prepared(app: Horsies) -> dict[str, PreparedSchedule]:
    schedule_config = app.config.schedule
    assert schedule_config is not None
    result = prepare_schedules(app, schedule_config)
    assert isinstance(result, Ok), result
    return dict(result.ok_value)


def _enabled(schedule: ScheduleManifest) -> ScheduleActivationEnabled:
    match schedule.activation:
        case ScheduleActivationEnabled() as enabled:
            return enabled
        case ScheduleActivationDisabled():
            raise AssertionError(f'{schedule.name} is disabled')


# =============================================================================
# to_scheduler_manifest
# =============================================================================


@pytest.mark.unit
class TestToSchedulerManifest:
    """Each schedule's facts, as the core's start builds the send command from them."""

    def test_an_enabled_schedule_carries_its_pattern_and_its_send(self) -> None:
        app = _app(_nightly(), retain_rerun_input=True)
        prepared = _prepared(app)

        manifest = _manifest(app)

        assert manifest.manifest_version == MANIFEST_VERSION
        assert (manifest.enabled, manifest.check_interval_seconds) == (True, 5)
        [schedule] = manifest.schedules
        assert schedule.name == 'nightly'
        assert _enabled(schedule) == ScheduleActivationEnabled(
            pattern_json=(
                '{"type":"interval","seconds":60,"minutes":null,"hours":null,"days":null}'
            ),
            timezone='Europe/Berlin',
            catch_up_missed=True,
            max_catch_up_runs=7,
            send=ScheduleSendManifest(
                task_name=_REPORT_TASK,
                queue_name='default',
                priority=100,
                input_json='{"day":"monday","limit":5}',
                task_options=ScheduleTaskOptionsEncoded(
                    json=cast(str, prepared['nightly'].task_options_json),
                ),
                retention_class_key='standard_30d',
                retain_rerun_input=True,
            ),
        )

    def test_a_schedule_without_kwargs_sends_the_empty_input(self) -> None:
        manifest = _manifest(_app(_plain()))

        [schedule] = manifest.schedules
        enabled = _enabled(schedule)
        assert enabled.pattern_json == '{"type":"daily","time":"09:30:00"}'
        assert enabled.send.input_json == '{}'
        assert enabled.send.retain_rerun_input is False

    def test_a_task_without_options_text_sends_no_options(self) -> None:
        app = _app(_plain())
        prepared = _prepared(app)
        prepared['plain'] = PreparedSchedule(input_json=None, task_options_json=None)
        schedule_config = app.config.schedule
        assert schedule_config is not None

        manifest = to_scheduler_manifest(app, schedule_config, prepared)

        assert _enabled(manifest.schedules[0]).send.task_options == (
            ScheduleTaskOptionsNoOptions()
        )

    def test_a_disabled_schedule_keeps_its_place_and_only_its_name(self) -> None:
        manifest = _manifest(
            _app(_plain('first'), _plain('paused', enabled=False), _plain('last')),
        )

        assert [schedule.name for schedule in manifest.schedules] == [
            'first',
            'paused',
            'last',
        ]
        assert manifest.schedules[1].activation == ScheduleActivationDisabled()

    def test_a_queue_kept_forever_sends_the_forever_class(self) -> None:
        retention = RetentionConfig.model_validate({'queue_retention': {'default': None}})
        manifest = _manifest(_app(_plain(), retention=retention))

        assert _enabled(manifest.schedules[0]).send.retention_class_key == 'forever'

    def test_the_dump_is_the_json_text_that_the_core_parses(self) -> None:
        manifest = _manifest(_app(_nightly(), _plain('paused', enabled=False)))

        document = json.loads(dump_scheduler_manifest(manifest))

        assert document['manifest_version'] == MANIFEST_VERSION
        [nightly, paused] = document['schedules']
        assert nightly['activation']['kind'] == 'ENABLED'
        assert nightly['activation']['send']['task_options']['kind'] == 'ENCODED'
        assert paused == {'name': 'paused', 'activation': {'kind': 'DISABLED'}}

    @pytest.mark.asyncio
    async def test_the_send_of_a_schedule_holds_the_texts_that_a_plain_send_stores(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The options text and the input text of each slot are those that a
        plain send of the task with the same kwargs gives the broker."""
        app = _app(_nightly())
        send_broker = MagicMock()
        send_broker.enqueue_async = AsyncMock(return_value=Ok('sent-task'))
        monkeypatch.setattr(app, 'get_broker', lambda: send_broker)

        send = _enabled(_manifest(app).schedules[0]).send
        sent = await app.tasks[_REPORT_TASK].send_async(day='monday', limit=5)

        assert isinstance(sent, Ok)
        send_kwargs = send_broker.enqueue_async.await_args.kwargs
        assert send_kwargs['task_options'] is not None
        assert send.task_options == ScheduleTaskOptionsEncoded(json=send_kwargs['task_options'])
        assert send.input_json == send_kwargs['input_json']
        assert send_kwargs['good_until'] is None


# =============================================================================
# Options that do not encode
# =============================================================================


_OPTIONS_TASK = 'reports.options'


def _register_options_task(app: Horsies) -> str | None:
    @app.task(_OPTIONS_TASK, timeout_ms=30_000)
    def options_task(*, value: int) -> Result[int, TaskError]:
        return Ok(value)

    return options_task.task_options_json


@pytest.mark.unit
class TestOptionsThatDoNotEncode:
    """The decorator encodes the options once; a failure refuses the schedule at start."""

    def test_the_schedule_is_refused_at_start_with_subject_and_cause(self) -> None:
        schedule = TaskSchedule(
            name='every_minute',
            task_name=_OPTIONS_TASK,
            pattern=IntervalSchedule(seconds=60),
            kwargs={'value': 1},
        )
        schedule_config = ScheduleConfig(schedules=[schedule])
        app = Horsies(
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=PostgresConfig(database_url=SecretStr(_DATABASE_URL)),
                schedule=schedule_config,
            ),
        )

        with patch(
            'horsies.core.task_decorator.serialize_task_options',
            return_value=Err(SerializationError('int too large for JSON')),
        ):
            with pytest.raises(ValueError) as definition_error:
                _ = _register_options_task(app)

        assert _OPTIONS_TASK in str(definition_error.value)
        assert 'int too large for JSON' in str(definition_error.value)
        assert _OPTIONS_TASK not in app.tasks

        prepared = prepare_schedules(app, schedule_config)

        assert isinstance(prepared, Err)
        start_error = prepared.err_value[0]
        assert isinstance(start_error, RegistryError)
        assert start_error.code == HorsiesErrorCode.TASK_NOT_REGISTERED
        assert _OPTIONS_TASK in start_error.message
        assert any('every_minute' in note for note in start_error.notes)


# =============================================================================
# The facade on CoreScheduler
# =============================================================================


@dataclass
class _CoreDouble:
    """The CoreScheduler doubles that one test built, and their answers."""

    start: object = field(default_factory=lambda: MagicMock(spec=SchedulerStarted))
    run: object = field(default_factory=lambda: MagicMock(spec=SchedulerStopped))
    built: list[MagicMock] = field(default_factory=list[MagicMock])
    manifests: list[str] = field(default_factory=list[str])

    def __call__(self, handle: object, manifest_json: str) -> MagicMock:
        core = MagicMock()
        core.handle = handle
        core.start_async = AsyncMock(return_value=self.start)
        core.run_async = AsyncMock(return_value=self.run)
        core.request_stop = MagicMock()
        self.built.append(core)
        self.manifests.append(manifest_json)
        return core

    def only(self) -> MagicMock:
        [core] = self.built
        return core


@pytest.fixture
def core_double() -> Iterator[_CoreDouble]:
    double = _CoreDouble()
    with patch('horsies.core.scheduler.service.CoreScheduler', double):
        yield double


def _scheduler(*schedules: TaskSchedule) -> tuple[Scheduler, MagicMock]:
    """A facade whose app returns a broker double."""
    app = _app(*schedules)
    broker = MagicMock()
    broker._core_handle = MagicMock(name='core handle')
    broker.close_async = AsyncMock(return_value=Ok(None))
    setattr(app, 'get_broker', MagicMock(return_value=broker))
    return Scheduler(app), broker


@pytest.mark.unit
class TestInit:
    def test_an_app_without_a_schedule_config_is_refused(self) -> None:
        app = Horsies(
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=PostgresConfig(database_url=SecretStr(_DATABASE_URL)),
            ),
        )

        with pytest.raises(ConfigurationError) as exc_info:
            Scheduler(app)

        error = exc_info.value
        assert error.code == HorsiesErrorCode.CONFIG_INVALID_SCHEDULE
        assert 'schedule configuration' in error.message
        assert error.help_text is not None

    def test_a_valid_config_builds_nothing_before_the_start(
        self, core_double: _CoreDouble,
    ) -> None:
        scheduler, _broker = _scheduler(_nightly())

        assert scheduler.schedule_config is scheduler.app.config.schedule
        assert scheduler.broker is None
        assert core_double.built == []
        get_broker = scheduler.app.get_broker
        assert isinstance(get_broker, MagicMock)
        get_broker.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
class TestStart:
    async def test_a_start_builds_one_core_scheduler_on_the_broker_handle(
        self, core_double: _CoreDouble,
    ) -> None:
        scheduler, broker = _scheduler(_nightly())

        await scheduler.start()
        await scheduler.start()

        core = core_double.only()
        assert core.handle is broker._core_handle
        assert core.start_async.await_count == 2
        assert scheduler.broker is broker
        [manifest_json] = core_double.manifests
        assert json.loads(manifest_json)['schedules'][0]['name'] == 'nightly'

    async def test_a_refused_schedule_raises_the_schedule_config_error(
        self, core_double: _CoreDouble,
    ) -> None:
        core_double.start = MagicMock(
            spec=SchedulerStartRefused,
            schedule='nightly',
            reason="schedule 'nightly': unknown timezone 'Mars/Olympus'",
        )
        scheduler, _broker = _scheduler(_nightly())

        with pytest.raises(ConfigurationError) as exc_info:
            await scheduler.start()

        error = exc_info.value
        assert error.code == HorsiesErrorCode.CONFIG_INVALID_SCHEDULE
        assert error.message == "schedule 'nightly' cannot start"
        assert error.notes == ["schedule 'nightly': unknown timezone 'Mars/Olympus'"]

    async def test_a_failed_start_raises_runtime_error_with_the_cause(
        self, core_double: _CoreDouble,
    ) -> None:
        core_double.start = MagicMock(
            spec=SchedulerStartFailed,
            message='the database belongs to another client family',
            retryable=False,
        )
        scheduler, _broker = _scheduler(_nightly())

        with pytest.raises(
            RuntimeError,
            match='Scheduler start failed: the database belongs to another client family',
        ):
            await scheduler.start()

    async def test_a_schedule_that_prepare_refuses_raises_before_the_core_is_built(
        self, core_double: _CoreDouble,
    ) -> None:
        scheduler, _broker = _scheduler(_nightly(task_name='reports.unknown'))

        with pytest.raises((ConfigurationError, RegistryError)):
            await scheduler.start()

        assert core_double.built == []

    async def test_large_kwargs_are_logged_once_per_schedule_when_the_manifest_is_built(
        self, core_double: _CoreDouble,
    ) -> None:
        scheduler, _broker = _scheduler(_nightly(), _plain())

        with patch('horsies.core.scheduler.service.warn_if_payload_large') as warn:
            await scheduler.start()
            await scheduler.start()

        assert warn.call_count == 1
        assert warn.call_args.args == (scheduler.app.config.payload,)
        assert warn.call_args.kwargs == {
            'task_name': _REPORT_TASK,
            'kind': 'kwargs',
            'payload_text': '{"day":"monday","limit":5}',
        }
        assert len(core_double.built) == 1

    async def test_the_task_modules_are_imported_before_the_schedules_are_prepared(
        self, core_double: _CoreDouble,
    ) -> None:
        """The import fills the task registry that the prepare reads; the
        core scheduler is built after both."""
        scheduler, broker = _scheduler(_nightly())
        steps: list[str] = []

        def import_task_modules() -> None:
            steps.append('import')

        def prepare(app: Horsies, schedule_config: ScheduleConfig) -> object:
            steps.append('prepare')
            assert core_double.built == []
            return prepare_schedules(app, schedule_config)

        with (
            patch.object(scheduler, '_preload_task_modules', side_effect=import_task_modules),
            patch('horsies.core.scheduler.service.prepare_schedules', side_effect=prepare),
        ):
            await scheduler.start()

        assert steps == ['import', 'prepare']
        core = core_double.only()
        assert core.handle is broker._core_handle
        core.start_async.assert_awaited_once_with()


@pytest.mark.unit
@pytest.mark.asyncio
class TestRunForeverAndStop:
    async def test_a_run_that_stopped_closes_the_broker(self, core_double: _CoreDouble) -> None:
        scheduler, broker = _scheduler(_nightly())

        await scheduler.run_forever()

        core = core_double.only()
        core.run_async.assert_awaited_once_with()
        core.request_stop.assert_called_once_with()
        broker.close_async.assert_awaited_once_with()

    async def test_a_run_whose_start_was_refused_raises_and_closes_the_broker(
        self, core_double: _CoreDouble,
    ) -> None:
        core_double.run = MagicMock(
            spec=SchedulerStartRefused,
            schedule='nightly',
            reason="schedule 'nightly' has no slot after the database time",
        )
        scheduler, broker = _scheduler(_nightly())

        with pytest.raises(ConfigurationError, match="schedule 'nightly' cannot start"):
            await scheduler.run_forever()

        broker.close_async.assert_awaited_once_with()

    async def test_a_stop_request_before_the_build_reaches_the_core_scheduler(
        self, core_double: _CoreDouble,
    ) -> None:
        scheduler, _broker = _scheduler(_nightly())

        scheduler.request_stop()
        await scheduler.start()

        core_double.only().request_stop.assert_called_once_with()

    async def test_a_stop_request_after_the_build_reaches_the_core_scheduler_at_once(
        self, core_double: _CoreDouble,
    ) -> None:
        scheduler, _broker = _scheduler(_nightly())
        await scheduler.start()

        scheduler.request_stop()

        core_double.only().request_stop.assert_called_once_with()

    async def test_a_stop_without_a_build_closes_nothing(self, core_double: _CoreDouble) -> None:
        scheduler, broker = _scheduler(_nightly())

        await scheduler.stop()

        assert core_double.built == []
        broker.close_async.assert_not_awaited()

    async def test_a_stop_without_a_build_reaches_the_core_scheduler_of_a_later_start(
        self, core_double: _CoreDouble,
    ) -> None:
        scheduler, broker = _scheduler(_nightly())

        await scheduler.stop()
        await scheduler.start()

        core_double.only().request_stop.assert_called_once_with()
        broker.close_async.assert_not_awaited()

    async def test_a_failed_broker_close_is_logged_and_the_stop_returns(
        self, core_double: _CoreDouble, caplog: pytest.LogCaptureFixture,
    ) -> None:
        scheduler, broker = _scheduler(_nightly())
        broker.close_async = AsyncMock(
            return_value=Err(
                DatabaseOperationFailed(
                    code=DatabaseErrorCode.CLOSE_FAILED,
                    message='close failed',
                    retryable=False,
                    cause=ConnectionError('close failed'),
                ),
            ),
        )
        await scheduler.start()

        with caplog.at_level(logging.ERROR, logger='horsies.scheduler'):
            await scheduler.stop()

        core_double.only().request_stop.assert_called_once_with()
        broker.close_async.assert_awaited_once_with()
        assert any(
            record.levelno == logging.ERROR
            and 'Broker close failed: close failed' in record.getMessage()
            for record in caplog.records
        )
