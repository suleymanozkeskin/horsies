"""Serialization for ``TaskOptions`` to its persisted JSON shape.

Lives next to the other codec helpers and routes through the strict
``json_io.dumps_json`` on an explicit plain dict (no class tags).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import assert_never

from pydantic import ValidationError

from horsies.core.codec.json_io import Json, SerdeResult, dumps_json
from horsies.core.codec.validation_errors import validation_summary
from horsies.core.models.tasks import (
    DEFAULT_CANCEL_GRACE_MS,
    MAX_CANCEL_GRACE_MS,
    MIN_CANCEL_GRACE_MS,
    Execution,
    RetryPolicy,
    TaskOptions,
)
from horsies.core.types.result import Err, Ok, Result


def serialize_task_options(task_options: TaskOptions) -> SerdeResult[str]:
    """Serialize TaskOptions to a JSON string.

    `execution` holds the `Execution` value. `cancel_grace_ms` holds the
    resolved grace of a `SHARED_ASYNC` task (its declared value, else
    `DEFAULT_CANCEL_GRACE_MS`), and null for the other classes. The task
    decorator has checked the class rules and the bounds before this runs.
    """
    return dumps_json(
        {
            'retry_policy': task_options.retry_policy.model_dump(
                mode='json',
                exclude_none=True,
            )
            if task_options.retry_policy
            else None,
            'good_until': task_options.good_until.isoformat()
            if task_options.good_until
            else None,
            'timeout_ms': task_options.timeout_ms,
            'execution': task_options.execution.value,
            'cancel_grace_ms': _stored_cancel_grace_ms(task_options),
        },
    )


def _stored_cancel_grace_ms(task_options: TaskOptions) -> int | None:
    """The cancel grace that the stored options hold for this class.

    A `SHARED_ASYNC` task stores its declared grace, else the default. The
    other classes have no cancel grace and store null.
    """
    match task_options.execution:
        case Execution.SHARED_ASYNC:
            match task_options.cancel_grace_ms:
                case None:
                    return DEFAULT_CANCEL_GRACE_MS
                case declared:
                    return declared
        case Execution.ISOLATED | Execution.SHARED_THREAD:
            return None
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class InvalidRetryPolicy:
    """A `retry_policy` that does not validate as `RetryPolicy`. `detail`
    names each failing field and its cause."""

    detail: str


def stored_max_retries(options: Json) -> Result[int, InvalidRetryPolicy]:
    """The `max_retries` value that enqueue stores for decoded task options.

    Options that are not an object, and a `retry_policy` that is absent or
    null, store 0 retries. Any other `retry_policy` must validate as
    `RetryPolicy` in strict mode (no type coercion); its `max_retries` is
    stored. Other option fields are not checked.
    """
    if not isinstance(options, dict):
        return Ok(0)
    policy = options.get('retry_policy')
    if policy is None:
        return Ok(0)
    if not isinstance(policy, dict):
        return Err(
            InvalidRetryPolicy(
                f'retry_policy is not an object: {type(policy).__name__}'
            )
        )
    try:
        validated = RetryPolicy.model_validate(policy, strict=True)
    except ValidationError as error:
        return Err(InvalidRetryPolicy(validation_summary(error)))
    return Ok(validated.max_retries)


@dataclass(frozen=True, slots=True)
class InvalidExecution:
    """Stored `execution` or `cancel_grace_ms` that does not validate.

    `detail` names the field and the cause. It holds no stored value, because
    an int with more than 4300 digits cannot be converted to a string.
    """

    detail: str


def stored_execution(options: Json) -> Result[Execution, InvalidExecution]:
    """The `Execution` that enqueue stores for decoded task options.

    No options (None) store `ISOLATED`, the default class. Options that are
    not an object are refused. An object without an `execution` key stores
    `ISOLATED`: options written before the class existed have no key, and
    every such task ran isolated. A present `execution` must be one of the
    `Execution` values. `cancel_grace_ms` must then fit the class: an int
    from MIN_CANCEL_GRACE_MS to MAX_CANCEL_GRACE_MS for `SHARED_ASYNC`, and
    absent or null for the other classes. Other option fields are not
    checked. Does no I/O.
    """
    match options:
        case None:
            return Ok(Execution.ISOLATED)
        case dict():
            pass
        case _:
            return Err(
                InvalidExecution(
                    f'task options are a JSON {type(options).__name__}, '
                    'not an object',
                ),
            )
    execution_r = _stored_execution_class(options)
    if isinstance(execution_r, Err):
        return execution_r
    execution = execution_r.ok_value
    grace_error = _stored_grace_error(execution, options.get('cancel_grace_ms'))
    match grace_error:
        case None:
            return Ok(execution)
        case detail:
            return Err(InvalidExecution(detail))


def _stored_execution_class(
    options: dict[str, Json],
) -> Result[Execution, InvalidExecution]:
    """Read the `execution` key; an absent key is `ISOLATED`."""
    if 'execution' not in options:
        return Ok(Execution.ISOLATED)
    match options['execution']:
        case str() as raw if raw in _EXECUTION_VALUES:
            return Ok(Execution(raw))
        case str():
            return Err(
                InvalidExecution(
                    'execution is not one of '
                    f'{", ".join(sorted(_EXECUTION_VALUES))}',
                ),
            )
        case other:
            return Err(
                InvalidExecution(
                    f'execution is a JSON {type(other).__name__}, not a string',
                ),
            )


def _stored_grace_error(execution: Execution, raw: Json) -> str | None:
    """The cause when `cancel_grace_ms` does not fit the class, else None."""
    match execution:
        case Execution.SHARED_ASYNC:
            return shared_async_grace_error(raw)
        case Execution.ISOLATED | Execution.SHARED_THREAD:
            match raw:
                case None:
                    return None
                case _:
                    return (
                        f'cancel_grace_ms is set for execution '
                        f'{execution.value}; only SHARED_ASYNC has one'
                    )
        case _ as unreachable:
            assert_never(unreachable)


def shared_async_grace_error(raw: Json) -> str | None:
    """The cause when a `SHARED_ASYNC` grace is not an int in the bounds."""
    match raw:
        case bool():
            return 'cancel_grace_ms is a bool, not an int'
        case int() if raw < MIN_CANCEL_GRACE_MS:
            return f'cancel_grace_ms is below {MIN_CANCEL_GRACE_MS} ms'
        case int() if raw > MAX_CANCEL_GRACE_MS:
            return f'cancel_grace_ms is above {MAX_CANCEL_GRACE_MS} ms'
        case int():
            return None
        case None:
            return 'cancel_grace_ms is absent for execution SHARED_ASYNC'
        case other:
            return f'cancel_grace_ms is a JSON {type(other).__name__}, not an int'


_EXECUTION_VALUES: frozenset[str] = frozenset(member.value for member in Execution)
