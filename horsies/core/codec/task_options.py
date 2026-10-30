"""Serialization for ``TaskOptions`` to its persisted JSON shape.

Lives next to the other codec helpers and routes through the strict
``json_io.dumps_json`` on an explicit plain dict (no class tags).
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import ValidationError

from horsies.core.codec.json_io import Json, SerdeResult, dumps_json
from horsies.core.codec.validation_errors import validation_summary
from horsies.core.models.tasks import RetryPolicy, TaskOptions
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
