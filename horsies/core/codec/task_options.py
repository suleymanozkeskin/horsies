"""Serialization for ``TaskOptions`` to its persisted JSON shape.

Lives next to the other codec helpers and routes through the strict
``json_io.dumps_json`` on an explicit plain dict (no class tags).
"""

from __future__ import annotations

from dataclasses import dataclass

from horsies.core.codec.json_io import Json, SerdeResult, dumps_json
from horsies.core.models.tasks import MAX_RETRY_ATTEMPTS, RetryPolicy, TaskOptions
from horsies.core.types.result import Err, Ok, Result


def serialize_task_options(task_options: TaskOptions) -> SerdeResult[str]:
    """Serialize TaskOptions to a JSON string."""
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
        },
    )


@dataclass(frozen=True, slots=True)
class InvalidRetryLimit:
    """A `retry_policy.max_retries` that is not an integer in
    0..MAX_RETRY_ATTEMPTS. `detail` names the value."""

    detail: str


def stored_max_retries(options: Json) -> Result[int, InvalidRetryLimit]:
    """The `max_retries` value that enqueue stores for decoded task options.

    Options without a `retry_policy` object store 0 retries: options that
    are not an object, and a `retry_policy` that is absent, null, or not an
    object. A policy without `max_retries` has the `RetryPolicy` default.
    Other option fields are not checked.
    """
    if not isinstance(options, dict):
        return Ok(0)
    policy = options.get('retry_policy')
    if not isinstance(policy, dict):
        return Ok(0)
    if 'max_retries' not in policy:
        return Ok(RetryPolicy.model_fields['max_retries'].default)
    match policy['max_retries']:
        case bool() as flag:
            return Err(InvalidRetryLimit(f'max_retries is not an integer: {flag!r}'))
        case int() as limit if 0 <= limit <= MAX_RETRY_ATTEMPTS:
            return Ok(limit)
        case other:
            return Err(
                InvalidRetryLimit(
                    f'max_retries is not an integer in 0..{MAX_RETRY_ATTEMPTS}: '
                    f'{other!r}'
                )
            )
