"""The result that a terminal dependency gives a node, and the start, pause
and resume functions of ``lifecycle.py`` under their old import path.

The child spec source (``child_spec_source.py``) reads
``terminal_dependency_result`` to give a sub-workflow child the results of
its parent's dependencies. Does no I/O.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from horsies.core.codec.json_io import loads_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import decode_task_result
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.logging import get_logger
from horsies.core.models.tasks import OperationalErrorCode, OutcomeCode
from horsies.core.models.workflow import WorkflowTaskStatus
from horsies.core.types.result import is_err

if TYPE_CHECKING:
    from horsies.core.app import Horsies
    from horsies.core.models.outcome import TaskOutcome
    from horsies.core.models.tasks import TaskError

logger = get_logger('workflow.engine')


def _missing_result_entry(
    *,
    task_index: int,
    node_id: str | None,
    task_name: str,
    status: str,
) -> 'TaskOutcome[Any, TaskError]':
    """The entry of a COMPLETED or FAILED dependency with no stored result.

    No library writer leaves such a node, so it is stored data that does not
    parse: ``RESULT_DESERIALIZATION_ERROR`` naming the source node (its id,
    or ``-`` when it has none, its index and its task) and its status. The
    baseline function ``horsies_workflow_source_value`` builds the same
    entry.
    """
    from horsies.core.models.outcome import Failed
    from horsies.core.models.tasks import TaskError

    node_label = node_id if node_id is not None else '-'
    return Failed(
        TaskError(
            error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
            message=(
                f'Dependency node {node_label} (index {task_index}, task '
                f'{task_name}) is {status} with no stored result'
            ),
            data={'dependency_index': task_index},
        ),
    )


def _decode_stored_task_result(
    stored_result: str,
    *,
    app: 'Horsies | None',
    task_name: str,
    context: dict[str, Any],
    sub_definition_key: str | None = None,
) -> 'TaskOutcome[Any, TaskError]':
    """Decode a stored task result JSON payload using the local task's OkT.

    Strict-serde phase 6+7: every engine-side decode of a worker-emitted
    result routes through ``decode_task_result`` (no legacy
    rehydration). The expected ``ok_type`` is derived from
    ``app.tasks[task_name].task_ok_type``; when ``app`` is unavailable
    (legacy callers) or the task isn't registered, the failure becomes a
    sentinel ``Failed(TaskError)`` so the upstream caller still sees a
    ``TaskOutcome``.

    Args:
        stored_result: The raw JSON text from the result column.
        app: The Horsies app instance for task registry lookup.
        task_name: The source task's registered name.
        context: Diagnostic metadata embedded into the sentinel error
            (workflow_id, task_index, status, etc.).

    Returns:
        The decoded outcome; a ``Failed`` with ``RESULT_DESERIALIZATION_ERROR``
        on any failure.
    """
    from horsies.core.models.outcome import Failed
    from horsies.core.models.tasks import TaskError

    deser = _deser_json(stored_result, 'task result json')
    if deser is None:
        return Failed(
            TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'Stored task result JSON corrupt for task '
                    f'{task_name!r}'
                ),
                data={**context, 'stage': 'json_parse'},
            ),
        )
    if app is None:
        return Failed(
            TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'No app context available to decode result for '
                    f'task {task_name!r}'
                ),
                data={**context, 'stage': 'no_app'},
            ),
        )
    from horsies.core.models.workflow.typing_utils import (
        resolve_source_ok_type,
    )

    ok_type = resolve_source_ok_type(
        app, task_name, sub_definition_key=sub_definition_key,
    )
    if ok_type is None:
        return Failed(
            TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'Source {task_name!r} is not registered in this '
                    f'process (not a known task or workflow definition); '
                    f'cannot decode stored task result'
                ),
                data={**context, 'stage': 'source_not_registered'},
            ),
        )
    try:
        return decode_task_result(deser, ok_type)
    except (StrictJsonError, ValueValidationError) as exc:
        return Failed(
            TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'decode_task_result failed for task {task_name!r}: '
                    f'{exc}'
                ),
                data={**context, 'stage': 'decode'},
            ),
        )


def _deser_json(raw: str | None, context: str, fallback: Any = None) -> Any:
    """Deserialize JSON, returning fallback for NULL and on error (an empty
    text is an error: it is not JSON)."""
    if raw is None:
        return fallback
    r = loads_json(raw)
    if is_err(r):
        logger.warning(f'JSON parse failed ({context}): {r.err_value}')
        return fallback
    return r.ok_value


def terminal_dependency_result(
    app: 'Horsies | None',
    *,
    workflow_id: str,
    task_index: int,
    node_id: str | None,
    status: str,
    stored_result: str | None,
    task_name: str,
    sub_definition_key: str | None,
) -> 'TaskOutcome[Any, TaskError]':
    """The outcome that a terminal dependency gives a node that it promotes.

    - SKIPPED: a ``Failed`` with ``UPSTREAM_SKIPPED`` naming the dependency index.
    - COMPLETED or FAILED with a stored result: the outcome decoded with the
      source's ok type (``_decode_stored_task_result``).
    - COMPLETED or FAILED with no stored result: ``RESULT_DESERIALIZATION_ERROR``
      naming the node (``_missing_result_entry``).

    Does no I/O.
    """
    from horsies.core.models.outcome import Failed
    from horsies.core.models.tasks import TaskError

    if status == WorkflowTaskStatus.SKIPPED.value:
        return Failed(
            TaskError(
                error_code=OutcomeCode.UPSTREAM_SKIPPED,
                message='Upstream dependency was SKIPPED',
                data={'dependency_index': task_index},
            )
        )
    if not stored_result:
        return _missing_result_entry(
            task_index=task_index,
            node_id=node_id,
            task_name=task_name,
            status=status,
        )
    return _decode_stored_task_result(
        stored_result,
        app=app,
        task_name=task_name,
        context={
            'workflow_id': workflow_id,
            'task_index': task_index,
            'status': status,
        },
        sub_definition_key=sub_definition_key,
    )


# --- Backward-compat barrel re-exports from lifecycle.py ---
from horsies.core.workflows import lifecycle as _lifecycle  # noqa: E402

start_workflow_async = _lifecycle.start_workflow_async
start_workflow = _lifecycle.start_workflow
pause_workflow = _lifecycle.pause_workflow
pause_workflow_sync = _lifecycle.pause_workflow_sync
resume_workflow = _lifecycle.resume_workflow
resume_workflow_sync = _lifecycle.resume_workflow_sync
