# horsies/core/scheduler/validation.py
"""Schedule validation shared by scheduler boot and ``app.check()``.

The checks here are functions of ``(app, schedule_config)`` — they touch no
broker or engine state — so the same validation runs as a preflight phase in
``Horsies.check()`` and as an eager fail-fast guard in ``Scheduler.start()``.

:func:`prepare_schedules` is the one pass: it checks every enabled schedule and
encodes its kwargs to wire text. ``Scheduler.start()`` raises the first error or
keeps that text, and the tick does not encode again. ``check`` reports one error
per bad schedule through :func:`collect_schedule_errors`.
"""
from __future__ import annotations

import inspect
from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, assert_never

from zoneinfo import ZoneInfo

from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.kwargs import encode_kwargs, underlying_task_fn
from horsies.core.codec.payload_guard import (
    PayloadOverLimit,
    PayloadWithinLimit,
    check_payload_size,
)
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    RegistryError,
)
from horsies.core.models.schedule import ScheduleConfig, TaskSchedule
from horsies.core.types.result import Err, Ok, Result, is_err

if TYPE_CHECKING:
    from horsies.core.app import Horsies

type EncodedScheduleKwargs = Mapping[str, str | None]
"""Kwargs wire text of each enabled schedule by name; None means no kwargs."""


def prepare_schedules(
    app: Horsies,
    schedule_config: ScheduleConfig,
) -> Result[EncodedScheduleKwargs, tuple[HorsiesError, ...]]:
    """Check every enabled schedule once and encode its kwargs.

    Returns Ok with the wire text of every enabled schedule, or Err with one
    error per invalid schedule in config order. Within a single schedule the
    first failing check wins. Disabled schedules are skipped and have no entry.
    Does no I/O. Validators of the task's parameter types run here, once.
    """
    encoded_kwargs: dict[str, str | None] = {}
    errors: list[HorsiesError] = []
    for schedule in schedule_config.schedules:
        if not schedule.enabled:
            continue
        match _prepare_schedule(app, schedule):
            case Ok(kwargs_json):
                encoded_kwargs[schedule.name] = kwargs_json
            case Err(error):
                errors.append(error)
    if errors:
        return Err(tuple(errors))
    return Ok(MappingProxyType(encoded_kwargs))


def collect_schedule_errors(
    app: Horsies,
    schedule_config: ScheduleConfig,
) -> list[HorsiesError]:
    """Return one error per invalid enabled schedule (empty if all valid)."""
    match prepare_schedules(app, schedule_config):
        case Ok():
            return []
        case Err(errors):
            return list(errors)


def _prepare_schedule(
    app: Horsies,
    schedule: TaskSchedule,
) -> Result[str | None, HorsiesError]:
    """Run the schedule checks in order; the first failure wins."""
    definition_error = _definition_error(app, schedule)
    if definition_error is not None:
        return Err(definition_error)
    return _encode_schedule_kwargs(app, schedule)


def _definition_error(
    app: Horsies,
    schedule: TaskSchedule,
) -> HorsiesError | None:
    """Check timezone, task, queue and signature, returning the first failure."""
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

    return _signature_error(app, schedule)


def _resolve_queue_error(
    app: Horsies,
    schedule: TaskSchedule,
) -> HorsiesError | None:
    """Resolve the schedule's queue (explicit, else task default) and validate it."""
    task = app.tasks.get(schedule.task_name)
    task_queue = task.task_queue_name if task is not None else None
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


def _encode_schedule_kwargs(
    app: Horsies,
    schedule: TaskSchedule,
) -> Result[str | None, HorsiesError]:
    """Encode the kwargs of one schedule to their wire text.

    ``sig.bind`` only checks parameter names/arity, not the strict-serde wire
    contract. This refuses a kwarg that targets an engine-injected parameter
    (``workflow_ctx``/``workflow_meta``), a TaskResult-typed parameter, a value
    that cannot be JSON-serialized, and kwargs over ``payload.reject_bytes``.
    Each repeats on every fire, so the schedule is refused once at boot.

    Ok(None) means the schedule has no kwargs. The task is registered: the
    definition checks run first.
    """
    if not schedule.kwargs:
        return Ok(None)
    task_fn = underlying_task_fn(app.tasks[schedule.task_name])
    try:
        encoded = encode_kwargs(task_fn, schedule.kwargs)
    except (StrictJsonError, ValueValidationError) as exc:
        return Err(ConfigurationError(
            message=f"schedule '{schedule.name}' kwargs are not encodable for task '{schedule.task_name}'",
            code=ErrorCode.CONFIG_INVALID_SCHEDULE,
            notes=[f'encode error: {exc}'],
            help_text='schedules are kwargs-only and must satisfy the task\'s typed signature; do not supply engine-injected (workflow_ctx/workflow_meta) or TaskResult-typed kwargs',
        ))
    ser = dumps_json(encoded)
    if is_err(ser):
        return Err(ConfigurationError(
            message=f"schedule '{schedule.name}' kwargs are not JSON-serializable for task '{schedule.task_name}'",
            code=ErrorCode.CONFIG_INVALID_SCHEDULE,
            notes=[f'serialization error: {ser.err_value}'],
            help_text='ensure all scheduled kwargs serialize to JSON',
        ))
    kwargs_json = ser.ok_value
    size_error = _payload_size_error(app, schedule, kwargs_json=kwargs_json)
    if size_error is not None:
        return Err(size_error)
    return Ok(kwargs_json)


def _payload_size_error(
    app: Horsies,
    schedule: TaskSchedule,
    *,
    kwargs_json: str,
) -> ConfigurationError | None:
    """Refuse kwargs longer than ``payload.reject_bytes``; the limit is exclusive."""
    match check_payload_size(app.config.payload, encoded_len=len(kwargs_json)):
        case PayloadWithinLimit():
            return None
        case PayloadOverLimit(size_bytes=size_bytes, limit_bytes=limit_bytes):
            return ConfigurationError(
                message=f"schedule '{schedule.name}' kwargs exceed payload.reject_bytes",
                code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                notes=[
                    f'kwargs payload size: {size_bytes} bytes',
                    f'payload.reject_bytes: {limit_bytes}',
                ],
                help_text='shrink the scheduled kwargs or raise payload.reject_bytes',
            )
        case _ as unreachable:
            assert_never(unreachable)
