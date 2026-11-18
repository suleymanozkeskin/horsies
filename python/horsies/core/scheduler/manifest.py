"""Resolve the schedule config of a Python app into the core's scheduler manifest.

`to_scheduler_manifest` is the one place where the schedule config (Pydantic)
and the prepared wire text of each enabled schedule (`prepare_schedules`) meet
the input of the core's scheduler (the manifest generated from horsies-core,
`python/horsies/_catalog/scheduler_manifest.py`). Per schedule, in config
order, it gives the pattern text, the timezone, the catch-up rule and the send
facts of each slot: the task, the queue and its priority, the input text, the
task's options text, the retention class key and the rerun-input posture.

The core builds the send command of each schedule from these facts at its
start, and refuses a schedule whose command or pattern it cannot use
(`SchedulerStartRefused`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from horsies._catalog.scheduler_manifest import (
    MANIFEST_VERSION,
    ScheduleActivationDisabled,
    ScheduleActivationEnabled,
    ScheduleManifest,
    SchedulerManifest,
    ScheduleSendManifest,
    ScheduleTaskOptions,
    ScheduleTaskOptionsEncoded,
    ScheduleTaskOptionsNoOptions,
)
from horsies.core.brokers.postgres import EMPTY_TASK_INPUT
from horsies.core.codec.json_io import dumps_json
from horsies.core.history.ddl.classes import resolve_retention_class_key
from horsies.core.models.retention import resolve_queue_retention_class
from horsies.core.models.schedule import ScheduleConfig, TaskSchedule
from horsies.core.scheduler.validation import PreparedSchedule, PreparedSchedules
from horsies.core.types.result import Err, Ok

if TYPE_CHECKING:
    from horsies.core.app import Horsies


def to_scheduler_manifest(
    app: Horsies,
    schedule_config: ScheduleConfig,
    prepared: PreparedSchedules,
) -> SchedulerManifest:
    """The scheduler manifest of `schedule_config`, with the send facts of each
    enabled schedule from `prepared` and `app`.

    `prepared` is the `Ok` value of `prepare_schedules(app, schedule_config)`:
    it holds every enabled schedule. Does no I/O.

    Raises:
        ConfigurationError: a schedule's queue is not valid for the app
            (`Horsies.validate_queue_name`); `prepare_schedules` already
            checked it, so this repeats its refusal.
        AssertionError: an enabled schedule has no prepared text (a defect
            of the caller).
        RuntimeError: a pattern's JSON dump has no JSON text (a defect: a
            pattern model dumps JSON values only).
    """
    return SchedulerManifest(
        manifest_version=MANIFEST_VERSION,
        enabled=schedule_config.enabled,
        check_interval_seconds=schedule_config.check_interval_seconds,
        schedules=tuple(
            _schedule_manifest(app, schedule, prepared)
            for schedule in schedule_config.schedules
        ),
    )


def resolve_schedule_queue(app: Horsies, schedule: TaskSchedule) -> str:
    """The queue of each slot of `schedule`: the schedule's queue, else the
    task's declared queue, checked against the app's queues.

    Raises:
        ConfigurationError: the queue is not valid for the app
            (`Horsies.validate_queue_name`).
    """
    task = app.tasks.get(schedule.task_name)
    task_queue = task.task_queue_name if task is not None else None
    queue_name = schedule.queue_name if schedule.queue_name is not None else task_queue
    return app.validate_queue_name(queue_name)


def pattern_json(schedule: TaskSchedule) -> str:
    """The JSON text of the schedule's pattern, as
    `pattern.model_dump(mode='json')` gives it, in compact form.

    Raises:
        RuntimeError: the dump has no JSON text (a defect).
    """
    match dumps_json(schedule.pattern.model_dump(mode='json')):
        case Ok(text):
            return text
        case Err(error):
            raise RuntimeError(
                f"the pattern of schedule '{schedule.name}' has no JSON text: {error}",
            )


def _schedule_manifest(
    app: Horsies,
    schedule: TaskSchedule,
    prepared: PreparedSchedules,
) -> ScheduleManifest:
    """One schedule of the manifest: disabled, or enabled with its send."""
    match schedule.enabled:
        case False:
            return ScheduleManifest(
                name=schedule.name,
                activation=ScheduleActivationDisabled(),
            )
        case True:
            prepared_schedule = prepared.get(schedule.name)
            assert prepared_schedule is not None, (
                f"enabled schedule '{schedule.name}' has no prepared text; "
                'pass the Ok value of prepare_schedules for the same config'
            )
            return ScheduleManifest(
                name=schedule.name,
                activation=ScheduleActivationEnabled(
                    pattern_json=pattern_json(schedule),
                    timezone=schedule.timezone,
                    catch_up_missed=schedule.catch_up_missed,
                    max_catch_up_runs=schedule.max_catch_up_runs,
                    send=_send_manifest(app, schedule, prepared_schedule),
                ),
            )


def _send_manifest(
    app: Horsies,
    schedule: TaskSchedule,
    prepared_schedule: PreparedSchedule,
) -> ScheduleSendManifest:
    """The send facts of each slot: the values that `enqueue_async` stores for
    a plain send of the schedule's kwargs on its queue."""
    from horsies.core.task_decorator import effective_priority

    queue_name = resolve_schedule_queue(app, schedule)
    return ScheduleSendManifest(
        task_name=schedule.task_name,
        queue_name=queue_name,
        priority=effective_priority(app, queue_name),
        input_json=_input_json(prepared_schedule.input_json),
        task_options=_task_options(prepared_schedule.task_options_json),
        retention_class_key=resolve_retention_class_key(
            resolve_queue_retention_class(app.config.retention, queue_name),
        ),
        retain_rerun_input=app.config.broker.retain_rerun_input_default,
    )


def _input_json(input_json: str | None) -> str:
    """The input text that a send stores: the kwargs text, or
    `EMPTY_TASK_INPUT` for a schedule without kwargs."""
    match input_json:
        case None:
            return EMPTY_TASK_INPUT
        case text:
            return text


def _task_options(task_options_json: str | None) -> ScheduleTaskOptions:
    """The task's options text, or none for a task without options."""
    match task_options_json:
        case None:
            return ScheduleTaskOptionsNoOptions()
        case text:
            return ScheduleTaskOptionsEncoded(json=text)
