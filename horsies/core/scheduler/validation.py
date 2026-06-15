# horsies/core/scheduler/validation.py
"""Schedule validation shared by scheduler boot and ``app.check()``.

The checks here are pure functions of ``(app, schedule_config)`` — they touch
no broker or engine state — so the same validation runs as a preflight phase in
``Horsies.check()`` and as an eager fail-fast guard in ``Scheduler.start()``.

``Scheduler.start()`` raises the first error (boot fail-fast); ``check`` collects
one error per bad schedule. Both consume :func:`collect_schedule_errors`.
"""
from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

from pydantic import ValidationError as _PydanticValidationError
from zoneinfo import ZoneInfo

from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.kwargs import encode_kwargs, underlying_task_fn
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    RegistryError,
)
from horsies.core.models.schedule import ScheduleConfig, TaskSchedule
from horsies.core.types.result import is_err

if TYPE_CHECKING:
    from horsies.core.app import Horsies


def collect_schedule_errors(
    app: Horsies,
    schedule_config: ScheduleConfig,
) -> list[HorsiesError]:
    """Return one error per invalid enabled schedule (empty if all valid).

    Within a single schedule the first failing check wins, matching the
    boot-time fail-fast behaviour. Disabled schedules are skipped.
    """
    errors: list[HorsiesError] = []
    for schedule in schedule_config.schedules:
        if not schedule.enabled:
            continue
        error = _first_schedule_error(app, schedule)
        if error is not None:
            errors.append(error)
    return errors


def _first_schedule_error(
    app: Horsies,
    schedule: TaskSchedule,
) -> HorsiesError | None:
    """Run the schedule checks in order, returning the first failure."""
    try:
        ZoneInfo(schedule.timezone)
    except Exception as exc:
        # Reject here so a permanently bad timezone exits 1 at boot
        # instead of failing calculate_next_run on every tick
        # (the missing-row self-heal would retry it forever).
        return ConfigurationError(
            message=f"invalid timezone for schedule '{schedule.name}'",
            code=ErrorCode.CONFIG_INVALID_SCHEDULE,
            notes=[f'timezone: {schedule.timezone!r}', f'error: {exc}'],
            help_text='use an IANA timezone name, e.g. "UTC" or "Europe/Berlin"',
        )

    if schedule.task_name not in app.tasks:
        available = list(app.tasks.keys())
        return RegistryError(
            message=f"scheduled task '{schedule.task_name}' not registered",
            code=ErrorCode.TASK_NOT_REGISTERED,
            notes=[
                f"schedule '{schedule.name}' references task '{schedule.task_name}'",
                f'available tasks: {available}'
                if available
                else 'no tasks registered',
            ],
            help_text='ensure the task is defined with @app.task and imported before scheduler starts',
        )

    queue_error = _resolve_queue_error(app, schedule)
    if queue_error is not None:
        return queue_error

    signature_error = _signature_error(app, schedule)
    if signature_error is not None:
        return signature_error

    return _serializable_error(app, schedule)


def _resolve_queue_error(
    app: Horsies,
    schedule: TaskSchedule,
) -> HorsiesError | None:
    """Resolve the schedule's queue (explicit, else task default) and validate it."""
    task = app.tasks.get(schedule.task_name)
    task_queue = getattr(task, 'task_queue_name', None) if task else None
    queue_name = (
        schedule.queue_name if schedule.queue_name is not None else task_queue
    )
    try:
        app.validate_queue_name(queue_name)
    except Exception as exc:
        return ConfigurationError(
            message=f"invalid queue configuration for schedule '{schedule.name}'",
            code=ErrorCode.CONFIG_INVALID_SCHEDULE,
            notes=[f'underlying error: {exc}'],
            help_text='check queue_name in schedule matches app.config queue settings',
        )
    return None


def _signature_error(
    app: Horsies,
    schedule: TaskSchedule,
) -> HorsiesError | None:
    """Ensure schedule args/kwargs bind cleanly to the task signature."""
    task = app.tasks.get(schedule.task_name)
    original_fn = getattr(task, '_original_fn', None) if task else None
    if original_fn is None:
        return None  # Cannot validate without original function signature

    sig = inspect.signature(original_fn)
    try:
        sig.bind(**schedule.kwargs)
    except TypeError as exc:
        return ConfigurationError(
            message=f"schedule '{schedule.name}' kwargs do not match task '{schedule.task_name}' signature",
            code=ErrorCode.CONFIG_INVALID_SCHEDULE,
            notes=[f'signature bind error: {exc}'],
            help_text='update TaskSchedule args/kwargs to match task function parameters',
        )
    return None


def _serializable_error(
    app: Horsies,
    schedule: TaskSchedule,
) -> HorsiesError | None:
    """Dry-run the kwargs wire encoding.

    ``sig.bind`` only checks parameter names/arity, not the strict-serde wire
    contract. Without this, a schedule whose kwarg targets an engine-injected
    parameter (``workflow_ctx``/``workflow_meta``), a TaskResult-typed
    parameter, or a value that cannot be JSON-serialized passes startup and
    then fails ``ENQUEUE_FAILED`` on every tick. Mirror the enqueue-time
    encode/serialize so the bad schedule is rejected once instead of forever.
    """
    if not schedule.kwargs:
        return None
    task = app.tasks.get(schedule.task_name)
    if task is None:
        return None  # missing-task case already reported above
    task_fn = underlying_task_fn(task)
    try:
        encoded = encode_kwargs(task_fn, schedule.kwargs)
    except (StrictJsonError, _PydanticValidationError) as exc:
        return ConfigurationError(
            message=f"schedule '{schedule.name}' kwargs are not encodable for task '{schedule.task_name}'",
            code=ErrorCode.CONFIG_INVALID_SCHEDULE,
            notes=[f'encode error: {exc}'],
            help_text='schedules are kwargs-only and must satisfy the task\'s typed signature; do not supply engine-injected (workflow_ctx/workflow_meta) or TaskResult-typed kwargs',
        )
    ser = dumps_json(encoded)
    if is_err(ser):
        return ConfigurationError(
            message=f"schedule '{schedule.name}' kwargs are not JSON-serializable for task '{schedule.task_name}'",
            code=ErrorCode.CONFIG_INVALID_SCHEDULE,
            notes=[f'serialization error: {ser.err_value}'],
            help_text='ensure all scheduled kwargs serialize to JSON',
        )
    return None
