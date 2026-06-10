"""Serialization for ``TaskOptions`` to its persisted JSON shape.

Lives next to the other codec helpers and routes through the strict
``json_io.dumps_json`` on an explicit plain dict (no class tags).
"""

from __future__ import annotations

from horsies.core.codec.json_io import SerdeResult, dumps_json
from horsies.core.models.tasks import TaskOptions


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
