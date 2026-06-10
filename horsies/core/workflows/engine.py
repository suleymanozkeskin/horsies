"""Workflow execution engine — DAG resolution, task completion, dependency management."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, TypeVar, cast

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import TextClause

from pydantic import ValidationError as _PydanticValidationError

from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.kwargs import (
    decode_subworkflow_kwargs,
    encode_kwargs,
    encode_subworkflow_kwargs,
    underlying_task_fn,
)
from horsies.core.codec.json_io import dumps_json, loads_json, SerdeResult
from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.codec.typed import (
    Json,
    decode_task_error,
    decode_task_result,
    encode_task_error,
    encode_task_result,
    encode_value,
    validate_task_result_envelope,
)
from horsies.core.utils.fingerprint import enqueue_fingerprint
from horsies.core.types.result import is_err
from horsies.core.logging import get_logger
from horsies.core.models.workflow import (
    SubWorkflowNode,
    SubWorkflowSummary,
    WorkflowStatus,
    WorkflowTaskStatus,
    WorkflowDefinition,
    WF_TASK_TERMINAL_VALUES,
    validate_workflow_generic_output_match,
)
from horsies.core.errors import WorkflowValidationError, ErrorCode
from horsies.core.workflows.sql import (
    COUNT_CTX_TERMINAL_DEPS_SQL,
    ENQUEUE_SUBWORKFLOW_TASK_SQL,
    ENQUEUE_WORKFLOW_TASK_SQL,
    GET_CHILD_WORKFLOW_INFO_SQL,
    GET_DEP_STATUS_COUNTS_SQL,
    GET_DEPENDENCY_RESULTS_SQL,
    GET_DEPENDENCY_RESULTS_WITH_NAMES_SQL,
    GET_DEPENDENT_TASKS_SQL,
    GET_FIRST_FAILED_REQUIRED_TASK_SQL,
    GET_FIRST_FAILED_TASK_RESULT_SQL,
    GET_OUTPUT_TASK_RESULT_SQL,
    GET_PARENT_WORKFLOW_INFO_SQL,
    GET_SUBWORKFLOW_SUMMARIES_SQL,
    GET_TASK_CONFIG_SQL,
    GET_TASK_STATUSES_SQL,
    GET_TERMINAL_TASK_RESULTS_SQL,
    GET_WORKFLOW_COMPLETION_STATUS_SQL,
    GET_WORKFLOW_DEPTH_SQL,
    GET_WORKFLOW_NAME_SQL,
    GET_WORKFLOW_ON_ERROR_SQL,
    GET_WORKFLOW_OUTPUT_INDEX_SQL,
    GET_WORKFLOW_STATUS_SQL,
    GET_WORKFLOW_TASK_BY_TASK_ID_SQL,
    INSERT_CHILD_WORKFLOW_SQL,
    INSERT_TASK_FOR_WORKFLOW_SQL,
    INSERT_WORKFLOW_TASK_SQL,
    INSERT_WORKFLOW_TASK_SUBWORKFLOW_SQL,
    LINK_SUB_WORKFLOW_SQL,
    LINK_WORKFLOW_TASK_SQL,
    LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL,
    MARK_TASK_READY_SQL,
    MARK_WORKFLOW_COMPLETED_SQL,
    MARK_WORKFLOW_FAILED_SQL,
    MARK_WORKFLOW_TASK_FAILED_SQL,
    NOTIFY_WORKFLOW_DONE_SQL,
    PAUSE_WORKFLOW_ON_ERROR_SQL,
    SET_WORKFLOW_ERROR_SQL,
    SKIP_READY_WORKFLOW_TASK_SQL,
    SKIP_WORKFLOW_TASK_SQL,
    UPDATE_PARENT_NODE_RESULT_SQL,
    UPDATE_WORKFLOW_TASK_RESULT_SQL,
)

logger = get_logger('workflow.engine')

# CAS-miss log-level convention:
#   warning — when failure-handling code hits a terminal-state CAS guard
#     (exceptional-on-exceptional: _fail_enqueued_task, _fail_subworkflow_load).
#   debug — when routine completion paths hit one (broker callback redelivery /
#     replay: on_workflow_task_complete, on_subworkflow_complete).

from horsies.core.models.tasks import OperationalErrorCode, OutcomeCode

if TYPE_CHECKING:
    from horsies.core.app import Horsies
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.tasks import TaskResult, TaskError

OutT = TypeVar('OutT')


# Wire keys for engine-injected transport (strict-serde §8). Renamed
# from the legacy `__horsies_*` prefix in this phase; user data carrying
# either prefix is rejected by `_scan_reserved_keys` so we keep the
# emit/consume convention narrow.
_ENGINE_KEY_WORKFLOW_CTX = '__h_workflow_ctx__'
_ENGINE_KEY_WORKFLOW_META = '__h_workflow_meta__'
_ENGINE_KEY_TASKRESULT_ENVELOPE = '__h_taskresult_envelope__'


def _decode_stored_task_result(
    stored_result: str,
    *,
    app: 'Horsies | None',
    task_name: str,
    context: dict[str, Any],
    sub_definition_key: str | None = None,
) -> 'TaskResult[Any, TaskError]':
    """Decode a stored TaskResult JSON payload using the local task's OkT.

    Strict-serde phase 6+7: every engine-side decode of a worker-emitted
    TaskResult routes through ``decode_task_result`` (no legacy
    rehydration). The expected ``ok_type`` is derived from
    ``app.tasks[task_name].task_ok_type``; when ``app`` is unavailable
    (legacy callers) or the task isn't registered, the failure becomes a
    sentinel ``TaskError`` so the upstream caller still sees a
    TaskResult shape.

    Args:
        stored_result: The raw JSON text from the result column.
        app: The Horsies app instance for task registry lookup.
        task_name: The source task's registered name.
        context: Diagnostic metadata embedded into the sentinel error
            (workflow_id, task_index, status, etc.).

    Returns:
        The decoded TaskResult; a ``RESULT_DESERIALIZATION_ERROR``
        TaskResult on any failure.
    """
    from horsies.core.models.tasks import TaskError, TaskResult

    deser = _deser_json(stored_result, 'task result json')
    if deser is None:
        return TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'Stored TaskResult JSON corrupt for task '
                    f'{task_name!r}'
                ),
                data={**context, 'stage': 'json_parse'},
            ),
        )
    if app is None:
        return TaskResult(
            err=TaskError(
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
        return TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'Source {task_name!r} is not registered in this '
                    f'process (not a known task or workflow definition); '
                    f'cannot decode stored TaskResult'
                ),
                data={**context, 'stage': 'source_not_registered'},
            ),
        )
    try:
        return decode_task_result(deser, ok_type)
    except (StrictJsonError, _PydanticValidationError) as exc:
        return TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                message=(
                    f'decode_task_result failed for task {task_name!r}: '
                    f'{exc}'
                ),
                data={**context, 'stage': 'decode'},
            ),
        )


def _resolve_source_ok_type(
    app: 'Horsies | None',
    task_name: str,
    sub_definition_key: str | None = None,
) -> Any | None:
    """Look up the source OkT for envelope encoding.

    Resolves a TaskNode source via the task registry and a SubWorkflowNode
    source via its unique ``sub_definition_key`` (the child workflow's
    ``definition_key``). The caller must pass ``sub_definition_key`` for
    subworkflow sources so the lookup is unambiguous.

    Returns ``None`` when the source isn't registered; the engine encode
    path then writes ``envelope=None`` for that entry, which the decode
    side surfaces as a sentinel TaskError.
    """
    from horsies.core.models.workflow.typing_utils import resolve_source_ok_type

    return resolve_source_ok_type(
        app, task_name, sub_definition_key=sub_definition_key,
    )


def _decode_err_only(
    stored_result: str,
) -> 'TaskResult[Any, TaskError] | None':
    """Decode the err slot of a stored TaskResult envelope without ok_type.

    Mirrors the app-layer err-without-ok-type fast path (design §6):
    ``TaskError`` is a fixed-schema internal type, so an err-only
    payload can be reconstructed even when the source task isn't
    registered locally. Returns ``None`` if the envelope is missing,
    malformed, or carries an ok payload (caller must supply ok_type for
    those).

    Envelope shape is checked via ``validate_task_result_envelope`` —
    same strict criteria as ``decode_task_result``: exactly the keys
    ``{__h_task_result__, ok, err}``, marker is True, at most one slot
    populated. Malformed envelopes return ``None`` so the caller falls
    through to legacy handling rather than crashing.

    Routes through ``decode_task_error`` (polymorphic) so subclass
    types like ``SubWorkflowError`` survive the decode. The base
    ``decode_value(_, TaskError)`` path would silently drop
    subclass-only fields (`sub_workflow_id`, `sub_workflow_summary`).
    """
    from horsies.core.models.tasks import TaskError, TaskResult

    deser = _deser_json(stored_result, 'err-only result json')
    if deser is None:
        return None
    try:
        envelope = validate_task_result_envelope(deser)
    except StrictJsonError:
        return None
    err_slot = envelope.get('err')
    if err_slot is None:
        return None
    try:
        decoded_err = decode_task_error(err_slot)
    except (StrictJsonError, _PydanticValidationError):
        return None
    if not isinstance(decoded_err, TaskError):
        return None
    return TaskResult(err=decoded_err)


def _ser(result: SerdeResult[str], context: str, fallback: str | None = None) -> str | None:
    """Extract a serde serialization Result, logging and returning fallback on error."""
    if is_err(result):
        logger.error(f'Serialization failed ({context}): {result.err_value}')
        return fallback
    return result.ok_value


def _deser_json(raw: str | None, context: str, fallback: Any = None) -> Any:
    """Deserialize JSON, returning fallback on error."""
    if not raw:
        return fallback
    r = loads_json(raw)
    if is_err(r):
        logger.warning(f'JSON parse failed ({context}): {r.err_value}')
        return fallback
    return r.ok_value


def _validate_args_from_map(raw: dict[str, Any]) -> dict[str, int] | None:
    """Validate args_from shape: all keys str, all values int.

    Returns the validated dict on success, None if any entry has wrong types.
    """
    for key, val in raw.items():
        if not isinstance(key, str) or not isinstance(val, int):
            return None
    return cast(dict[str, int], raw)


async def _mark_workflow_task_failed_if_nonterminal(
    session: Any,
    workflow_id: str,
    task_index: int,
    result: str,
) -> bool:
    """Mark the workflow task FAILED with `result` only if the row is not already terminal.

    Returns True if the row transitioned to FAILED; False if the row was already
    COMPLETED/FAILED/SKIPPED. Callers must skip failure propagation on False, because
    whichever path set the terminal state has already propagated the appropriate outcome.
    """
    mark_result = await session.execute(
        MARK_WORKFLOW_TASK_FAILED_SQL,
        {
            'wf_id': workflow_id,
            'idx': task_index,
            'result': result,
            'terminal_states': WF_TASK_TERMINAL_VALUES,
        },
    )
    return mark_result.fetchone() is not None


async def _fail_enqueued_task(
    session: Any,
    workflow_id: str,
    task_index: int,
    message: str,
    broker: 'PostgresBroker | None' = None,
    error_code: 'OperationalErrorCode | str' = OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED,
) -> None:
    """Mark an ENQUEUED workflow task as FAILED when pre-enqueue parsing fails.

    Without this, an early return after the READY→ENQUEUED transition
    strands the task (ENQUEUED, task_id IS NULL, no recovery path).

    When broker is provided, also runs the full failure-propagation chain
    (on_error handling, dependent cascade, workflow completion check).
    """
    from horsies.core.models.tasks import TaskResult, TaskError

    tr: TaskResult[None, TaskError] = TaskResult(
        err=TaskError(
            error_code=error_code,
            message=message,
            data={'workflow_id': workflow_id, 'task_index': task_index},
        ),
    )
    failure_payload = serialize_error_payload(tr)
    marked_failed = await _mark_workflow_task_failed_if_nonterminal(
        session,
        workflow_id,
        task_index,
        failure_payload,
    )
    if not marked_failed:
        logger.warning(
            'Skipped failure mark for terminal workflow task %s:%s',
            workflow_id,
            task_index,
        )
        logger.debug(
            'Dropped failure payload for %s:%s: %s',
            workflow_id,
            task_index,
            failure_payload,
        )
        return
    if broker is not None:
        should_continue = await _handle_workflow_task_failure(
            session, workflow_id, tr,
        )
        if should_continue:
            await _process_dependents(session, workflow_id, task_index, broker)
            await check_workflow_completion(session, workflow_id, broker)
    else:
        # Loud, not silent: the node is FAILED but on_error handling,
        # dependent cascade, and the completion check did not run. The
        # workflow stalls until the next reaper pass with a broker.
        logger.warning(
            'Marked workflow task %s:%s FAILED without a broker; failure '
            'propagation deferred to the next recovery pass',
            workflow_id,
            task_index,
        )


async def _fail_subworkflow_load(
    session: AsyncSession,
    broker: 'PostgresBroker',
    workflow_id: str,
    task_index: int,
    workflow_name: str,
    sub_definition_key: str | None,
) -> None:
    """Mark a SubWorkflowNode FAILED when its child definition cannot be loaded.

    Mirrors `_fail_enqueued_task`: writes FAILED via the terminal-state CAS guard,
    then runs the failure-propagation chain. On CAS-miss (row already terminal),
    skips propagation because whichever path set the terminal state has already
    propagated the appropriate outcome.
    """
    from horsies.core.models.tasks import TaskError, TaskResult

    error = TaskError(
        error_code=OperationalErrorCode.SUBWORKFLOW_LOAD_FAILED,
        message=f'Failed to load subworkflow definition for {workflow_name}:{task_index}',
        data={'definition_key': sub_definition_key},
    )
    # Strict-serde §5: emit through ``serialize_error_payload`` (which
    # routes ok=None / err=TaskError through ``encode_task_result``
    # into the ``__h_task_result__`` envelope). ``dumps_json(TaskResult(...))``
    # would route through the legacy ``__task_result__`` /
    # ``__task_error__`` envelope that the strict decoder rejects.
    failure_payload: str = serialize_error_payload(TaskResult(err=error))
    marked_failed = await _mark_workflow_task_failed_if_nonterminal(
        session, workflow_id, task_index, failure_payload,
    )
    if not marked_failed:
        logger.warning(
            'Skipped subworkflow load failure mark for terminal workflow task %s:%s',
            workflow_id,
            task_index,
        )
        logger.debug(
            'Dropped subworkflow load failure payload for %s:%s: %s',
            workflow_id,
            task_index,
            failure_payload,
        )
        return
    logger.error(f'SubWorkflowNode load failed for {workflow_name}:{task_index}')

    failure_result: TaskResult[Any, TaskError] = TaskResult(err=error)
    should_continue = await _handle_workflow_task_failure(
        session, workflow_id, failure_result,
    )
    if should_continue:
        await _process_dependents(session, workflow_id, task_index, broker)
        await check_workflow_completion(session, workflow_id, broker)




async def enqueue_workflow_task(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
    all_dep_results: dict[int, 'TaskResult[Any, TaskError]'],
    all_dep_task_names: dict[int, str] | None = None,
    broker: 'PostgresBroker | None' = None,
    all_dep_definition_keys: dict[int, str | None] | None = None,
) -> str | None:
    """
    Enqueue a single workflow task.

    Args:
        session: Database session
        workflow_id: Workflow ID
        task_index: Task index to enqueue
        all_dep_results: Results from ALL completed dependencies (by index)

    Returns:
        task_id if enqueued, None if already enqueued, not ready, or workflow not RUNNING.
    """
    # Atomic: READY → ENQUEUED only if still READY AND workflow is RUNNING
    # PAUSE guard: JOIN with workflows ensures we don't enqueue while paused
    result = await session.execute(
        ENQUEUE_WORKFLOW_TASK_SQL,
        {'wf_id': workflow_id, 'idx': task_index},
    )

    row = result.fetchone()
    if row is None:
        return None  # Already enqueued, not ready, or workflow not RUNNING

    # Parse retry config and good_until from task_options
    task_options_str: str | None = row.task_options
    max_retries = 0
    good_until_str: str | None = None
    good_until_dt: datetime | None = None
    if task_options_str:
        options_data = _deser_json(task_options_str, 'task_options')
        if options_data is None:
            await _fail_enqueued_task(
                session,
                workflow_id,
                task_index,
                f'Corrupt task_options JSON for task {task_index}',
                broker,
                error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
            )
            return None
        if not isinstance(options_data, dict):
            await _fail_enqueued_task(
                session,
                workflow_id,
                task_index,
                f'Invalid task_options payload for task {task_index}: expected object',
                broker,
                error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
            )
            return None
        retry_policy = options_data.get('retry_policy')
        if isinstance(retry_policy, dict):
            # Default 3 mirrors RetryPolicy.max_retries; a non-int value
            # (corrupt row) would otherwise surface as an INSERT error and
            # loop the finalize retries.
            max_retries_raw = retry_policy.get('max_retries', 3)
            if isinstance(max_retries_raw, bool) or not isinstance(
                max_retries_raw, int
            ):
                await _fail_enqueued_task(
                    session,
                    workflow_id,
                    task_index,
                    (
                        f'Invalid max_retries in retry_policy for task '
                        f'{task_index}: {max_retries_raw!r}'
                    ),
                    broker,
                    error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                )
                return None
            max_retries = max_retries_raw
        good_until_raw = options_data.get('good_until')
        if good_until_raw is not None:
            good_until_str = str(good_until_raw)
            try:
                good_until_dt = datetime.fromisoformat(good_until_str)
            except (ValueError, TypeError) as exc:
                # SHA will use None; good_until_str is still passed to the
                # DB column. Loud — fingerprint and stored deadline diverge.
                logger.warning(
                    'Unparseable good_until for workflow=%s task_index=%s '
                    '(%r): %s',
                    workflow_id,
                    task_index,
                    good_until_raw,
                    exc,
                )

    # Start with static kwargs — corrupt kwargs is fatal for this task
    raw_kwargs = _deser_json(row.task_kwargs, 'workflow task kwargs')
    if raw_kwargs is None and row.task_kwargs:
        await _fail_enqueued_task(
            session,
            workflow_id,
            task_index,
            f'Corrupt kwargs JSON for task {task_index}',
            broker,
            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
        )
        return None
    kwargs: dict[str, Any] = raw_kwargs if isinstance(raw_kwargs, dict) else {}

    # Inject args_from: map kwarg_name -> TaskResult from dependency
    if row.args_from:
        args_from_raw = row.args_from
        # args_from is stored as JSONB, may come back as dict directly
        if isinstance(args_from_raw, str):
            args_from_map = _deser_json(args_from_raw, 'args_from')
            if args_from_map is None:
                await _fail_enqueued_task(
                    session,
                    workflow_id,
                    task_index,
                    f'Corrupt args_from JSON for task {task_index}',
                    broker,
                    error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                )
                return None
        else:
            args_from_map = args_from_raw

        if isinstance(args_from_map, dict):
            args_from_typed = _validate_args_from_map(args_from_map)
            if args_from_typed is None:
                await _fail_enqueued_task(
                    session,
                    workflow_id,
                    task_index,
                    f'args_from has non-int values for task {task_index}',
                    broker,
                    error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                )
                return None
            for kwarg_name, dep_index in args_from_typed.items():
                if kwarg_name in kwargs:
                    await _fail_enqueued_task(
                        session,
                        workflow_id,
                        task_index,
                        (
                            f"args_from key '{kwarg_name}' conflicts with static kwarg "
                            f"(task {task_index})"
                        ),
                        broker,
                    )
                    return None
                dep_result = all_dep_results.get(dep_index)
                if dep_result is not None:
                    source_task_name = (
                        all_dep_task_names.get(dep_index)
                        if all_dep_task_names is not None
                        else None
                    )
                    if source_task_name is None:
                        await _fail_enqueued_task(
                            session, workflow_id, task_index,
                            (
                                f'Missing source task name for dep_index '
                                f'{dep_index} (kwarg {kwarg_name!r}, '
                                f'task {task_index}); cannot encode '
                                f'args_from envelope'
                            ),
                            broker,
                            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                        )
                        return None
                    app = broker.app if broker is not None else None
                    source_definition_key = (
                        all_dep_definition_keys.get(dep_index)
                        if all_dep_definition_keys is not None
                        else None
                    )
                    source_ok_type = _resolve_source_ok_type(
                        app, source_task_name,
                        sub_definition_key=source_definition_key,
                    )
                    if source_ok_type is None:
                        await _fail_enqueued_task(
                            session, workflow_id, task_index,
                            (
                                f'Source {source_task_name!r} not '
                                f'registered or missing OkT; '
                                f'cannot encode args_from envelope for '
                                f'kwarg {kwarg_name!r} (task '
                                f'{task_index})'
                            ),
                            broker,
                            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                        )
                        return None
                    try:
                        inner = encode_task_result(
                            dep_result, source_ok_type,
                        )
                    except (StrictJsonError, _PydanticValidationError) as exc:
                        await _fail_enqueued_task(
                            session, workflow_id, task_index,
                            (
                                f'encode_task_result failed for '
                                f'kwarg {kwarg_name!r} (task '
                                f'{task_index}): {exc}'
                            ),
                            broker,
                            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                        )
                        return None
                    kwargs[kwarg_name] = {
                        _ENGINE_KEY_TASKRESULT_ENVELOPE: True,
                        'source_task_name': source_task_name,
                        'source_definition_key': source_definition_key,
                        'inner': inner,
                    }

    # Inject workflow_ctx if workflow_ctx_from is set
    if row.workflow_ctx_from:
        ctx_from_ids = cast(list[str], row.workflow_ctx_from)  # Already a list from PostgreSQL ARRAY
        ctx_data = await _build_workflow_context_data(
            session=session,
            workflow_id=workflow_id,
            task_index=task_index,
            task_name=row.task_name,
            ctx_from_ids=ctx_from_ids,
            app=broker.app if broker is not None else None,
        )
        # Will be filtered out by worker if task doesn't declare workflow_ctx param
        kwargs[_ENGINE_KEY_WORKFLOW_CTX] = ctx_data

    # Inject workflow metadata for tasks that declare workflow_meta.
    # Worker will filter it out if the function does not accept workflow_meta.
    kwargs[_ENGINE_KEY_WORKFLOW_META] = {
        'workflow_id': workflow_id,
        'task_index': task_index,
        'task_name': row.task_name,
    }

    # Serialize kwargs before creating the task — fail early on corrupt data
    kwargs_json = _ser(dumps_json(kwargs), 'workflow task kwargs')
    if kwargs_json is None:
        await _fail_enqueued_task(
            session, workflow_id, task_index,
            f'Failed to serialize kwargs for task {task_index}', broker,
            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
        )
        return None

    # Create actual task in tasks table
    task_id = str(uuid.uuid4())
    sent_at = datetime.now(timezone.utc)
    sha = enqueue_fingerprint(
        task_name=row.task_name,
        queue_name=row.queue_name,
        priority=row.priority,
        args_json=row.task_args,
        kwargs_json=kwargs_json,
        sent_at=sent_at,
        good_until=good_until_dt,
        enqueue_delay_seconds=None,
        task_options=task_options_str,
    )
    await session.execute(
        INSERT_TASK_FOR_WORKFLOW_SQL,
        {
            'id': task_id,
            'name': row.task_name,
            'queue': row.queue_name,
            'priority': row.priority,
            # Compat: new writes store [] for task_args (kwargs-only).
            # Old persisted rows may still carry non-empty task_args;
            # passing them through preserves backward compatibility.
            'args': row.task_args,
            'kwargs': kwargs_json,
            'sent_at': sent_at,
            'max_retries': max_retries,
            'task_options': task_options_str,
            'good_until': good_until_str,
            'enqueue_sha': sha,
        },
    )

    # Link workflow_task to actual task
    await session.execute(
        LINK_WORKFLOW_TASK_SQL,
        {'tid': task_id, 'wf_id': workflow_id, 'idx': task_index},
    )

    return task_id








async def enqueue_subworkflow_task(
    session: AsyncSession,
    broker: 'PostgresBroker',
    workflow_id: str,
    task_index: int,
    all_dep_results: dict[int, 'TaskResult[Any, TaskError]'],
    all_dep_task_names: dict[int, str] | None = None,
    parent_depth: int = 0,
    root_workflow_id: str | None = None,
) -> str | None:
    """
    Start a child workflow for a SubWorkflowNode.

    Args:
        session: Database session
        broker: PostgreSQL broker for database operations
        workflow_id: Parent workflow ID
        task_index: Task index of the SubWorkflowNode in parent
        all_dep_results: Results from ALL completed dependencies (by index)
        parent_depth: Nesting depth of the parent workflow
        root_workflow_id: Root workflow ID (for efficient queries)

    Returns:
        child_workflow_id if started, None if already started or not ready.
    """
    # 1. Atomically mark parent node as ENQUEUED (with workflow RUNNING guard)
    result = await session.execute(
        ENQUEUE_SUBWORKFLOW_TASK_SQL,
        {'wf_id': workflow_id, 'idx': task_index},
    )

    row = result.fetchone()
    if row is None:
        return None  # Already enqueued, not ready, or workflow not RUNNING

    _wt_id = row.id  # Unused but kept for row unpacking clarity
    _sub_workflow_name = row.sub_workflow_name  # Unused but kept for row unpacking clarity
    task_args_json = row.task_args
    task_kwargs_json = row.task_kwargs
    args_from_raw = row.args_from
    _node_id = row.node_id  # Unused but kept for row unpacking clarity
    sub_definition_key = row.sub_definition_key
    # 2. Load the child workflow definition by its explicit definition_key.
    workflow_name_result = await session.execute(
        GET_WORKFLOW_NAME_SQL,
        {'wf_id': workflow_id},
    )
    workflow_name_row = workflow_name_result.fetchone()
    if workflow_name_row is None:
        await _fail_enqueued_task(
            session, workflow_id, task_index,
            f'Workflow {workflow_id} not found during subworkflow enqueue', broker,
            error_code=OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED,
        )
        return None

    workflow_name = workflow_name_row.name

    workflow_def: type[WorkflowDefinition[Any]] | None = None
    if sub_definition_key:
        workflow_def = _load_workflow_def_from_key(sub_definition_key)

    if workflow_def is None:
        # Critical: Revert ENQUEUED → FAILED to prevent stuck task
        # This can happen if the child definition_key is not registered.
        await _fail_subworkflow_load(
            session, broker, workflow_id, task_index, workflow_name, sub_definition_key,
        )
        return None

    # 3. Parse static kwargs and merge args_from
    # Compat: new writes store [] for task_args (kwargs-only).
    # Old persisted rows may still carry positional args for build_with();
    # they are passed through via *task_args to preserve backward compat.
    raw_args = _deser_json(task_args_json, 'subworkflow args', fallback=[])
    task_args: tuple[Any, ...] = ()
    if isinstance(raw_args, list):
        task_args = tuple(raw_args)
    raw_kwargs = _deser_json(task_kwargs_json, 'subworkflow kwargs')
    if raw_kwargs is None and task_kwargs_json:
        await _fail_enqueued_task(
            session,
            workflow_id,
            task_index,
            f'Corrupt kwargs JSON for subworkflow task {task_index}',
            broker,
            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
        )
        return None
    raw_kwargs_dict: dict[str, Any] = (
        raw_kwargs if isinstance(raw_kwargs, dict) else {}
    )
    # Typed decode against the child ``build_with`` signature. Annotated
    # params decode via their declared type; opaque ``**params: Any``
    # slots route through ``JsonValue`` (raw JSON-native shapes only).
    try:
        kwargs: dict[str, Any] = decode_subworkflow_kwargs(
            workflow_def, raw_kwargs_dict,
        )
    except (StrictJsonError, _PydanticValidationError) as exc:
        await _fail_enqueued_task(
            session,
            workflow_id,
            task_index,
            (
                f'Typed decode of subworkflow kwargs failed for task '
                f'{task_index}: {exc}'
            ),
            broker,
            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
        )
        return None

    if args_from_raw:
        # args_from is stored as JSONB, may come back as dict directly
        if isinstance(args_from_raw, str):
            args_from_map = _deser_json(args_from_raw, 'subworkflow args_from')
            if args_from_map is None:
                await _fail_enqueued_task(
                    session,
                    workflow_id,
                    task_index,
                    f'Corrupt args_from JSON for subworkflow task {task_index}',
                    broker,
                    error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                )
                return None
        else:
            args_from_map = args_from_raw

        if isinstance(args_from_map, dict):
            args_from_typed = _validate_args_from_map(args_from_map)
            if args_from_typed is None:
                await _fail_enqueued_task(
                    session,
                    workflow_id,
                    task_index,
                    f'args_from has non-int values for subworkflow task {task_index}',
                    broker,
                    error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                )
                return None
            for kwarg_name, dep_index in args_from_typed.items():
                if kwarg_name in kwargs:
                    await _fail_enqueued_task(
                        session,
                        workflow_id,
                        task_index,
                        (
                            f"args_from key '{kwarg_name}' conflicts with static kwarg "
                            f"(subworkflow task {task_index})"
                        ),
                        broker,
                    )
                    return None
                dep_result = all_dep_results.get(dep_index)
                if dep_result is not None:
                    # Strict-serde phase 7: subworkflow ``build_with`` runs
                    # in-process — no DB/worker boundary to cross. Pass the
                    # typed ``TaskResult`` instance straight through;
                    # ``build_with`` signatures bind against
                    # ``TaskResult[T, TaskError]`` per the SubWorkflowNode
                    # ``args_from`` contract (see SubWorkflowNode docstring).
                    # The wire ``__h_taskresult_envelope__`` form is reserved
                    # for the worker-bound TaskNode ``args_from`` path that
                    # crosses the broker.
                    kwargs[kwarg_name] = dep_result

    # 4. Build child WorkflowSpec (parameterized)
    try:
        if broker.app is None:
            raise WorkflowValidationError(
                message='Broker missing app reference for subworkflow',
                code=ErrorCode.WORKFLOW_SUBWORKFLOW_APP_MISSING,
                notes=[
                    'Subworkflows require a Horsies app instance to build the child spec',
                    'Ensure broker.app is set (Horsies.get_broker() does this automatically)',
                ],
                help_text='use app.get_broker() or attach app to broker before starting workflows',
            )

        # Defense-in-depth: parameterized subworkflows must override build_with.
        default_build_with = getattr(
            WorkflowDefinition.build_with,
            '__func__',
            WorkflowDefinition.build_with,
        )
        build_with_fn = getattr(
            workflow_def,
            '_original_build_with',
            workflow_def.build_with,
        )
        if (task_args or kwargs) and build_with_fn is default_build_with:
            raise RuntimeError(
                f"subworkflow '{workflow_def.name}' received runtime parameters "
                'but uses default build_with(); override build_with(app, ...) to accept them'
            )

        child_spec = workflow_def.build_with(broker.app, *task_args, **kwargs)

        # Validate that build_with override didn't produce a type mismatch
        validate_workflow_generic_output_match(workflow_def, child_spec)
    except Exception as exc:
        logger.error(
            f'Subworkflow enqueue build failed for {workflow_name}:{task_index}: '
            f'{type(exc).__name__}: {exc}'
        )
        await _fail_enqueued_task(
            session,
            workflow_id,
            task_index,
            (
                f'Subworkflow build failed: {type(exc).__name__}: {str(exc)[:200]} '
                f'(workflow {workflow_name}:{task_index})'
            ),
            broker,
            error_code=OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED,
        )
        return None

    # 5. Prepare all child workflow_task inserts before creating the child
    # workflow row. Any validation/serialization failure below must not leave a
    # committed child workflow with only a partial task set.
    child_id = str(uuid.uuid4())

    # Serialize child's success_policy
    child_success_policy_json: dict[str, Any] | None = None
    if child_spec.success_policy is not None:
        child_success_policy_json = {
            'cases': [
                {
                    'required_indices': [
                        t.index for t in case.required if t.index is not None
                    ]
                }
                for case in child_spec.success_policy.cases
            ],
        }
        if child_spec.success_policy.optional:
            child_success_policy_json['optional_indices'] = [
                t.index
                for t in child_spec.success_policy.optional
                if t.index is not None
            ]

    child_output_index = child_spec.output.index if child_spec.output else None
    child_sent_at = datetime.now(timezone.utc)

    child_workflow_task_inserts: list[tuple[TextClause, dict[str, Any]]] = []
    for child_node in child_spec.tasks:
        child_dep_indices = [
            d.index for d in child_node.waits_for if d.index is not None
        ]
        child_args_from_indices = {
            k: v.index for k, v in child_node.args_from.items() if v.index is not None
        }
        child_ctx_from_ids = (
            [n.node_id for n in child_node.workflow_ctx_from if n.node_id is not None]
            if child_node.workflow_ctx_from
            else None
        )

        child_wt_id = str(uuid.uuid4())
        child_is_subworkflow = isinstance(child_node, SubWorkflowNode)

        if child_is_subworkflow:
            child_sub = child_node
            try:
                guard_no_positional_args(child_sub.name, child_sub.args)
            except ValueError as exc:
                await _fail_enqueued_task(
                    session,
                    workflow_id,
                    task_index,
                    (
                        f'Invalid child subworkflow node args for {child_sub.name}: '
                        f'{str(exc)[:200]}'
                    ),
                    broker,
                    error_code=OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED,
                )
                return None
            try:
                encoded_child_sub_kwargs = encode_subworkflow_kwargs(
                    child_sub.workflow_def, child_sub.kwargs,
                )
            except (StrictJsonError, _PydanticValidationError) as exc:
                await _fail_enqueued_task(
                    session, workflow_id, task_index,
                    (
                        f'Failed to encode kwargs for child sub '
                        f'{child_sub.name}: {str(exc)[:200]}'
                    ),
                    broker,
                    error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                )
                return None
            child_kwargs_json = _ser(
                dumps_json(encoded_child_sub_kwargs), 'child sub kwargs',
            )
            if child_kwargs_json is None:
                await _fail_enqueued_task(
                    session, workflow_id, task_index,
                    f'Failed to serialize kwargs for child sub {child_sub.name}', broker,
                    error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                )
                return None
            child_workflow_task_inserts.append((
                INSERT_WORKFLOW_TASK_SUBWORKFLOW_SQL,
                {
                    'id': child_wt_id,
                    'wf_id': child_id,
                    'idx': child_sub.index,
                    'node_id': child_sub.node_id,
                    'name': child_sub.name,
                    'args': _ser(dumps_json([]), 'positional args', fallback='[]'),
                    'kwargs': child_kwargs_json,
                    'queue': 'default',
                    'priority': 100,
                    'deps': child_dep_indices,
                    'args_from': _ser(dumps_json(child_args_from_indices), 'child args_from')
                    if child_args_from_indices
                    else None,
                    'ctx_from': child_ctx_from_ids,
                    'allow_failed': child_sub.allow_failed_deps,
                    'join_type': child_sub.join,
                    'min_success': child_sub.min_success,
                    'task_options': None,
                    'status': 'PENDING' if child_dep_indices else 'READY',
                    'sub_wf_name': child_sub.workflow_def.name,
                    'sub_def_key': child_sub.workflow_def._require_definition_key(),
                },
            ))
        else:
            child_task = child_node
            try:
                guard_no_positional_args(child_task.name, child_task.args)
            except ValueError as exc:
                await _fail_enqueued_task(
                    session,
                    workflow_id,
                    task_index,
                    (
                        f'Invalid child task node args for {child_task.name}: '
                        f'{str(exc)[:200]}'
                    ),
                    broker,
                    error_code=OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED,
                )
                return None
            child_task_options_json: str | None = getattr(
                child_task.fn, 'task_options_json', None
            )
            if child_task.good_until is not None:
                child_base_options: dict[str, Any] = {}
                if child_task_options_json:
                    parsed = _deser_json(child_task_options_json, 'child task_options')
                    if parsed is None:
                        await _fail_enqueued_task(
                            session,
                            workflow_id,
                            task_index,
                            f'Corrupt child task_options for {child_task.name}',
                            broker,
                            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                        )
                        return None
                    if not isinstance(parsed, dict):
                        await _fail_enqueued_task(
                            session,
                            workflow_id,
                            task_index,
                            (
                                f'Invalid child task_options payload for {child_task.name}: '
                                'expected object'
                            ),
                            broker,
                            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                        )
                        return None
                    child_base_options = cast(dict[str, Any], parsed)
                child_base_options['good_until'] = child_task.good_until.isoformat()
                child_task_options_json = _ser(dumps_json(child_base_options), 'child task_options')

            # Strict-serde: route child TaskNode kwargs through the typed
            # codec so reserved-namespace keys (`__h_*` / `__horsies_*`)
            # in user-data positions fail closed before storage. Without
            # this, smuggled args_from envelopes in static kwargs would
            # reach the worker via `child_runner.py`'s args_from path
            # and bypass the typed decode there.
            try:
                child_task_underlying_fn = underlying_task_fn(child_task.fn)
                encoded_child_task_kwargs = dict(
                    encode_kwargs(child_task_underlying_fn, child_task.kwargs),
                )
            except (StrictJsonError, _PydanticValidationError) as exc:
                await _fail_enqueued_task(
                    session, workflow_id, task_index,
                    (
                        f'Failed to encode kwargs for child task '
                        f'{child_task.name}: {exc}'
                    ),
                    broker,
                    error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                )
                return None
            child_kwargs_json = _ser(
                dumps_json(encoded_child_task_kwargs),
                'child task kwargs',
            )
            if child_kwargs_json is None:
                await _fail_enqueued_task(
                    session, workflow_id, task_index,
                    f'Failed to serialize kwargs for child task {child_task.name}', broker,
                    error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                )
                return None
            child_workflow_task_inserts.append((
                INSERT_WORKFLOW_TASK_SQL,
                {
                    'id': child_wt_id,
                    'wf_id': child_id,
                    'idx': child_task.index,
                    'node_id': child_task.node_id,
                    'name': child_task.name,
                    'args': _ser(dumps_json([]), 'positional args', fallback='[]'),
                    'kwargs': child_kwargs_json,
                    'queue': child_task.queue
                    or getattr(child_task.fn, 'task_queue_name', None)
                    or 'default',
                    'priority': child_task.priority
                    if child_task.priority is not None
                    else 100,
                    'deps': child_dep_indices,
                    'args_from': _ser(dumps_json(child_args_from_indices), 'child args_from')
                    if child_args_from_indices
                    else None,
                    'ctx_from': child_ctx_from_ids,
                    'allow_failed': child_task.allow_failed_deps,
                    'join_type': child_task.join,
                    'min_success': child_task.min_success,
                    'task_options': child_task_options_json,
                    'status': 'PENDING' if child_dep_indices else 'READY',
                },
            ))

    # 6. Create child workflow with parent reference, then insert its complete
    # task set.
    await session.execute(
        INSERT_CHILD_WORKFLOW_SQL,
        {
            'id': child_id,
            'name': child_spec.name,
            'on_error': child_spec.on_error.value,
            'output_idx': child_output_index,
            'success_policy': _ser(dumps_json(child_success_policy_json), 'child success_policy')
            if child_success_policy_json
            else None,
            'definition_key': child_spec.definition_key,
            'parent_wf_id': workflow_id,
            'parent_idx': task_index,
            'depth': parent_depth + 1,
            'root_wf_id': root_workflow_id,
            'sent_at': child_sent_at,
        },
    )

    for stmt, params in child_workflow_task_inserts:
        await session.execute(stmt, params)

    # 7. Update parent's workflow_task with child_workflow_id and mark RUNNING
    await session.execute(
        LINK_SUB_WORKFLOW_SQL,
        {'child_id': child_id, 'wf_id': workflow_id, 'idx': task_index},
    )

    # 8. Enqueue child's root tasks
    child_root_nodes = [t for t in child_spec.tasks if not t.waits_for]
    for child_root in child_root_nodes:
        if child_root.index is not None:
            if isinstance(child_root, SubWorkflowNode):
                await enqueue_subworkflow_task(
                    session,
                    broker,
                    child_id,
                    child_root.index,
                    {},
                    {},
                    parent_depth + 1,
                    root_workflow_id,
                )
            else:
                await enqueue_workflow_task(
                    session, child_id, child_root.index, {}, {}, broker,
                )

    logger.info(f'Started child workflow {child_id} for {workflow_name}:{task_index}')
    return child_id




async def _build_workflow_context_data(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
    task_name: str,
    ctx_from_ids: list[str],
    *,
    app: 'Horsies | None' = None,
) -> dict[str, Any]:
    """
    Build serializable workflow context data.

    Returns a plain dict that can be JSON-serialized and reconstructed
    on the worker side into a WorkflowContext.

    Results are keyed by node_id for stable lookup in WorkflowContext.
    Also fetches SubWorkflowSummary for SubWorkflowNodes.

    Strict-serde phase 6: each entry in ``results_by_id`` is the wire
    envelope produced by ``encode_task_result(tr, source_ok_type)``,
    not the legacy `dumps_json(tr)` string. The worker decodes per
    entry against the source-node ``task_ok_type``.
    """
    # Fetch results for the specified node_ids
    dep_results = await get_dependency_results_with_names(
        session, workflow_id, ctx_from_ids, app=app,
    )

    results_by_id: dict[str, dict[str, Json] | None] = {}
    for node_id, result in dep_results.by_id.items():
        source_task_name = dep_results.task_name_by_id.get(node_id)
        source_definition_key = dep_results.definition_key_by_id.get(node_id)
        source_ok_type = (
            _resolve_source_ok_type(
                app, source_task_name,
                sub_definition_key=source_definition_key,
            )
            if source_task_name is not None
            else None
        )
        if source_ok_type is None:
            # No typed encode possible — store None; downstream worker
            # surfaces a sentinel TaskError when this entry is read.
            results_by_id[node_id] = None
            continue
        try:
            results_by_id[node_id] = encode_task_result(
                result, source_ok_type,
            )
        except (StrictJsonError, _PydanticValidationError) as exc:
            logger.warning(
                f'encode_task_result failed for ctx node {node_id!r}: '
                f'{exc}; storing null'
            )
            results_by_id[node_id] = None

    # Fetch summaries for SubWorkflowNodes
    summaries_by_id: dict[str, str] = {}
    if ctx_from_ids:
        summary_result = await session.execute(
            GET_SUBWORKFLOW_SUMMARIES_SQL,
            {'wf_id': workflow_id, 'node_ids': ctx_from_ids},
        )
        for row in summary_result.fetchall():
            node_id = row.node_id
            summary_json = row.sub_workflow_summary
            if node_id and summary_json:
                summaries_by_id[node_id] = summary_json

    return {
        'workflow_id': workflow_id,
        'task_index': task_index,
        'task_name': task_name,
        'results_by_id': results_by_id,
        # Strict-serde phase 6: child runner needs task names per
        # node_id to look up source `task_ok_type` for typed decode of
        # each envelope in `results_by_id`.
        'task_name_by_id': dict(dep_results.task_name_by_id),
        # Per-node sub_definition_key (None for TaskNode sources) so the
        # child runner resolves SubWorkflowNode OkT by the unique
        # definition_key, not the ambiguous task_name.
        'definition_key_by_id': dict(dep_results.definition_key_by_id),
        'summaries_by_id': summaries_by_id,
    }






async def on_workflow_task_complete(
    session: AsyncSession,
    task_id: str,
    result: 'TaskResult[Any, TaskError]',
    broker: 'PostgresBroker | None',
) -> None:
    """
    Called from worker._finalize_after when a task completes.
    Handles workflow task status update and dependency resolution.

    Strict-serde phase 7: ``broker`` is required (explicit `None` allowed for
    unit-test branches that return before dependency propagation). Removing
    the implicit default surfaces missing-broker bugs at the call site instead
    of degrading to a `WORKER_SERIALIZATION_ERROR` during args_from envelope
    encoding when `broker.app` is needed to resolve the source task's
    `task_ok_type`.
    """
    # 1. Find workflow_task by task_id
    wt_result = await session.execute(
        GET_WORKFLOW_TASK_BY_TASK_ID_SQL,
        {'tid': task_id},
    )

    row = wt_result.fetchone()
    if row is None:
        return  # Not a workflow task

    workflow_id = row.workflow_id
    task_index = row.task_index
    task_name = row.task_name

    # Serialize completion handling per workflow so concurrent task completions
    # don't race dependency promotion (PENDING -> READY/SKIPPED).
    lock_result = await session.execute(
        LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL,
        {'wf_id': workflow_id},
    )
    if lock_result.fetchone() is None:
        return

    # 2. Update workflow_task status and store result (CAS: skip if already terminal)
    new_status = 'COMPLETED' if result.is_ok() else 'FAILED'
    # Strict-serde phase 7: re-encode through the wire envelope so downstream
    # readers (`_decode_stored_task_result`, `_decode_err_only`, child→parent
    # pass-through at on_subworkflow_complete) see the `__h_task_result__`
    # marker. Prefer the source task's declared `task_ok_type` for write-time
    # validation; fall back to `Any` when the broker/app is unavailable
    # (test/recovery paths) so completion never fails on missing type metadata.
    ok_type_for_encode: Any = Any
    if broker is not None and broker.app is not None:
        resolved = _resolve_source_ok_type(broker.app, task_name)
        if resolved is not None:
            ok_type_for_encode = resolved
    try:
        encoded_result = encode_task_result(result, ok_type_for_encode)
    except Exception as exc:
        # A deterministic encode failure would otherwise loop the worker's
        # phase-2 finalize retries forever (this is the only completion-path
        # encode that was unwrapped). Degrade to a FAILED node carrying a
        # serialization error; the err-only envelope encode below is
        # fixed-schema and total.
        from horsies.core.models.tasks import TaskError, TaskResult

        logger.error(
            'Failed to encode completion result for workflow=%s '
            'task_index=%s: %s; storing a serialization-error envelope',
            workflow_id,
            task_index,
            exc,
        )
        result = TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                message=f'Failed to encode completion result: {exc}',
                data={'workflow_id': workflow_id, 'task_index': task_index},
            ),
        )
        new_status = 'FAILED'
        encoded_result = encode_task_result(result, type(None))
    update_result = await session.execute(
        UPDATE_WORKFLOW_TASK_RESULT_SQL,
        {
            'status': new_status,
            'result': _ser(
                dumps_json(encoded_result),
                'task completion result',
                fallback='null',
            ),
            'wf_id': workflow_id,
            'idx': task_index,
            'terminal_states': WF_TASK_TERMINAL_VALUES,
        },
    )
    if update_result.fetchone() is None:
        logger.debug(
            'Workflow task already terminal, skipping progression: '
            'workflow=%s task_index=%s task_id=%s',
            workflow_id, task_index, task_id,
        )
        return

    # 3. Handle failure based on on_error policy
    if result.is_err():
        should_continue = await _handle_workflow_task_failure(
            session, workflow_id, result
        )
        if not should_continue:
            # PAUSE mode - stop processing, don't propagate to dependents
            return

    # 4. Check if workflow is PAUSED (may have been paused by another task)
    status_check = await session.execute(
        GET_WORKFLOW_STATUS_SQL,
        {'wf_id': workflow_id},
    )
    status_row = status_check.fetchone()
    if status_row and status_row.status == 'PAUSED':
        return  # Don't propagate - workflow is paused

    # 5. Find and potentially enqueue dependent tasks
    await _process_dependents(session, workflow_id, task_index, broker)

    # 6. Check if workflow is complete
    await check_workflow_completion(session, workflow_id, broker)





async def _process_dependents(
    session: AsyncSession,
    workflow_id: str,
    completed_task_index: int,
    broker: 'PostgresBroker | None' = None,
) -> None:
    """
    Find tasks that depend on the completed task and enqueue if ready.

    Args:
        session: Database session
        workflow_id: Workflow ID
        completed_task_index: Index of the task that just completed
        broker: PostgreSQL broker (required for SubWorkflowNode enqueue)
    """
    # Find tasks that have completed_task_index in their dependencies
    dependents = await session.execute(
        GET_DEPENDENT_TASKS_SQL,
        {'wf_id': workflow_id, 'completed_idx': completed_task_index},
    )

    # Get workflow depth and root for subworkflow support
    wf_info = await session.execute(
        GET_WORKFLOW_DEPTH_SQL,
        {'wf_id': workflow_id},
    )
    wf_row = wf_info.fetchone()
    depth = wf_row.depth if wf_row else 0
    root_wf_id = wf_row.root_workflow_id if wf_row else workflow_id

    for row in dependents.fetchall():
        await try_make_ready_and_enqueue(
            session, broker, workflow_id, row.task_index, depth, root_wf_id,
        )










async def try_make_ready_and_enqueue(
    session: AsyncSession,
    broker: 'PostgresBroker | None',
    workflow_id: str,
    task_index: int,
    depth: int = 0,
    root_workflow_id: str | None = None,
) -> None:
    """
    Check if task's join condition is satisfied and handle accordingly.

    Supports three join modes:
    - "all": task runs when ALL dependencies are terminal (default)
    - "any": task runs when ANY dependency succeeds (COMPLETED)
    - "quorum": task runs when at least min_success dependencies succeed

    For any/quorum, task is SKIPPED if it becomes impossible to meet threshold.

    PAUSE guard: Only proceeds if workflow status is RUNNING.
    If workflow is PAUSED/CANCELLED/etc, task remains PENDING.

    Args:
        session: Database session
        broker: PostgreSQL broker (required for SubWorkflowNode enqueue)
        workflow_id: Workflow ID
        task_index: Task index to check
        depth: Current workflow nesting depth (for child workflows)
        root_workflow_id: Root workflow ID (for efficient queries)
    """
    # 1. Fetch task configuration
    config_result = await session.execute(
        GET_TASK_CONFIG_SQL,
        {'wf_id': workflow_id, 'idx': task_index},
    )
    config_row = config_result.fetchone()
    if config_row is None:
        return

    task_status = config_row.status
    raw_deps = config_row.dependencies
    allow_failed_deps: bool = config_row.allow_failed_deps if config_row.allow_failed_deps is not None else False
    join_type: str = config_row.join_type or 'all'
    min_success: int | None = config_row.min_success
    raw_ctx_from = config_row.workflow_ctx_from
    is_subworkflow: bool = config_row.is_subworkflow if config_row.is_subworkflow is not None else False
    wf_status = config_row.wf_status

    # Guard: only proceed if task is PENDING and workflow is RUNNING
    if task_status != 'PENDING' or wf_status != 'RUNNING':
        return

    dependencies: list[int] = (
        cast(list[int], raw_deps) if isinstance(raw_deps, list) else []
    )

    if not dependencies:
        # No dependencies - should already be READY (root task)
        return

    # 2. Get dependency status counts
    dep_status_result = await session.execute(
        GET_DEP_STATUS_COUNTS_SQL,
        {'wf_id': workflow_id, 'deps': dependencies},
    )
    status_counts: dict[str, int] = {
        row.status: row.cnt for row in dep_status_result.fetchall()
    }

    completed = status_counts.get(WorkflowTaskStatus.COMPLETED.value, 0)
    failed = status_counts.get(WorkflowTaskStatus.FAILED.value, 0)
    skipped = status_counts.get(WorkflowTaskStatus.SKIPPED.value, 0)
    terminal = completed + failed + skipped
    total_deps = len(dependencies)

    # 3. Determine readiness based on join type
    should_become_ready = False
    should_skip = False

    if join_type == 'all':
        # All deps must be terminal
        if terminal == total_deps:
            should_become_ready = True

    elif join_type == 'any':
        # At least one dep must be COMPLETED
        if completed >= 1:
            should_become_ready = True
        elif terminal == total_deps and completed == 0:
            # All deps terminal but none succeeded
            should_skip = True

    elif join_type == 'quorum':
        # At least min_success deps must be COMPLETED
        threshold = min_success or 1
        if completed >= threshold:
            should_become_ready = True
        else:
            # Check if it's impossible to reach threshold
            remaining = total_deps - terminal
            max_possible = completed + remaining
            if max_possible < threshold:
                should_skip = True

    if should_skip:
        # Mark task as SKIPPED (impossible to meet join condition)
        await session.execute(
            SKIP_WORKFLOW_TASK_SQL,
            {'wf_id': workflow_id, 'idx': task_index},
        )
        # Propagate SKIPPED to dependents
        await _process_dependents(session, workflow_id, task_index, broker)
        return

    if not should_become_ready:
        return  # Stay PENDING

    # Ensure workflow_ctx_from deps are terminal before enqueueing
    ctx_from_ids = as_str_list(raw_ctx_from)
    if ctx_from_ids:
        ctx_terminal_result = await session.execute(
            COUNT_CTX_TERMINAL_DEPS_SQL,
            {
                'wf_id': workflow_id,
                'node_ids': ctx_from_ids,
                'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES,
            },
        )
        ctx_terminal = ctx_terminal_result.scalar_one()
        if ctx_terminal < len(ctx_from_ids):
            return  # Context deps not ready; stay PENDING

    will_skip_failed_all_deps = (
        join_type == 'all'
        and (failed + skipped) > 0
        and not allow_failed_deps
    )
    if is_subworkflow and broker is None and not will_skip_failed_all_deps:
        raise RuntimeError(
            'SubWorkflowNode requires a broker to start its child workflow'
        )

    # 4. Mark task as READY
    ready_result = await session.execute(
        MARK_TASK_READY_SQL,
        {'wf_id': workflow_id, 'idx': task_index},
    )
    if ready_result.fetchone() is None:
        return  # Already processed by another worker

    # 5. For join="all", check failed deps and skip if not allow_failed_deps
    # For any/quorum, we don't skip on failed deps (they're expected)
    if join_type == 'all':
        failed_or_skipped = failed + skipped
        if failed_or_skipped > 0 and not allow_failed_deps:
            await session.execute(
                SKIP_READY_WORKFLOW_TASK_SQL,
                {'wf_id': workflow_id, 'idx': task_index},
            )
            await _process_dependents(session, workflow_id, task_index, broker)
            return

    # 6. Fetch dependency results and enqueue
    dep_results, dep_task_names, dep_definition_keys = await get_dependency_results(
        session,
        workflow_id,
        dependencies,
        app=broker.app if broker is not None else None,
    )

    if is_subworkflow and broker is not None:
        # SubWorkflowNode: start child workflow
        actual_root = root_workflow_id or workflow_id
        await enqueue_subworkflow_task(
            session, broker, workflow_id, task_index, dep_results,
            dep_task_names, depth, actual_root,
        )
    else:
        # Regular TaskNode: enqueue as task
        await enqueue_workflow_task(
            session, workflow_id, task_index, dep_results, dep_task_names, broker,
            all_dep_definition_keys=dep_definition_keys,
        )




class DependencyResults:
    """Container for dependency results with index, name, and node_id mappings."""

    def __init__(self) -> None:
        self.by_index: dict[int, 'TaskResult[Any, TaskError]'] = {}
        self.by_name: dict[str, 'TaskResult[Any, TaskError]'] = {}
        self.by_id: dict[str, 'TaskResult[Any, TaskError]'] = {}
        # node_id → task_name mapping. Strict-serde phase 6 needs the
        # task name per node so the engine can look up
        # ``task_ok_type`` for per-entry ``encode_task_result``
        # encoding when building the workflow_ctx_data envelope.
        self.task_name_by_id: dict[str, str] = {}
        # node_id → sub_definition_key (None for TaskNode sources). For
        # SubWorkflowNode sources this is the child workflow's unique
        # definition_key, used to resolve OkT unambiguously when encoding
        # the workflow_ctx envelope (task_name alone is ambiguous since a
        # workflow .name can collide with a task name).
        self.definition_key_by_id: dict[str, str | None] = {}




async def get_dependency_results(
    session: AsyncSession,
    workflow_id: str,
    dependency_indices: list[int],
    *,
    app: 'Horsies | None' = None,
) -> tuple[
    dict[int, 'TaskResult[Any, TaskError]'],
    dict[int, str],
    dict[int, str | None],
]:
    """
    Fetch TaskResults for dependencies in terminal states.

    - COMPLETED/FAILED: returns actual TaskResult from stored result
    - SKIPPED: returns sentinel TaskResult with UPSTREAM_SKIPPED error

    Strict-serde phase 6: decode each row's stored result via
    ``decode_task_result(raw, ok_type)``. ``ok_type`` is resolved from
    the source's ``sub_definition_key`` (SubWorkflowNode) or ``task_name``
    (TaskNode). ``app`` is required for typed decode of task sources; when
    omitted (legacy callers), falls back to a
    ``RESULT_DESERIALIZATION_ERROR`` sentinel so the caller keeps explicit
    failure visibility instead of silently returning wrongly-typed values.

    Returns:
        Tuple of:
          - dict mapping task_index → TaskResult
          - dict mapping task_index → task_name (envelope source metadata)
          - dict mapping task_index → sub_definition_key | None (envelope
            source metadata so args_from decode resolves SubWorkflowNode
            OkT by the unique definition_key, not the ambiguous name)
    """
    from horsies.core.models.tasks import TaskError, TaskResult

    if not dependency_indices:
        return ({}, {}, {})

    result = await session.execute(
        GET_DEPENDENCY_RESULTS_SQL,
        {
            'wf_id': workflow_id,
            'indices': dependency_indices,
            'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES,
        },
    )

    results: dict[int, TaskResult[Any, TaskError]] = {}
    task_names_by_index: dict[int, str] = {}
    definition_keys_by_index: dict[int, str | None] = {}
    for row in result.fetchall():
        task_index = row.task_index
        task_name = row.task_name
        status = row.status
        stored_result = row.result
        sub_definition_key = row.sub_definition_key
        task_names_by_index[task_index] = task_name
        definition_keys_by_index[task_index] = sub_definition_key

        if status == WorkflowTaskStatus.SKIPPED.value:
            # Inject sentinel TaskResult for SKIPPED dependencies
            results[task_index] = TaskResult(
                err=TaskError(
                    error_code=OutcomeCode.UPSTREAM_SKIPPED,
                    message='Upstream dependency was SKIPPED',
                    data={'dependency_index': task_index},
                )
            )
        elif stored_result:
            decoded_tr = _decode_stored_task_result(
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
            results[task_index] = decoded_tr

    return (results, task_names_by_index, definition_keys_by_index)




async def get_dependency_results_with_names(
    session: AsyncSession,
    workflow_id: str,
    dependency_node_ids: list[str],
    *,
    app: 'Horsies | None' = None,
) -> DependencyResults:
    """
    Fetch TaskResults with index, name, and node_id mappings.
    Used for building WorkflowContext.

    - COMPLETED/FAILED: returns actual TaskResult from stored result
    - SKIPPED: returns sentinel TaskResult with UPSTREAM_SKIPPED error

    Strict-serde phase 6: each stored result is decoded via
    ``decode_task_result`` against the source task's ``task_ok_type``
    (looked up by ``task_name`` in ``app.tasks``). ``app`` is required
    for typed decode; when omitted, decode failures surface as
    ``RESULT_DESERIALIZATION_ERROR`` sentinels.
    """
    from horsies.core.models.tasks import TaskError, TaskResult

    dep_results = DependencyResults()

    if not dependency_node_ids:
        return dep_results

    result = await session.execute(
        GET_DEPENDENCY_RESULTS_WITH_NAMES_SQL,
        {
            'wf_id': workflow_id,
            'node_ids': dependency_node_ids,
            'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES,
        },
    )

    for row in result.fetchall():
        task_index = row.task_index
        task_name = row.task_name
        node_id = row.node_id
        status = row.status
        stored_result = row.result
        sub_definition_key = row.sub_definition_key

        if status == WorkflowTaskStatus.SKIPPED.value:
            # Inject sentinel TaskResult for SKIPPED dependencies
            task_result: TaskResult[Any, TaskError] = TaskResult(
                err=TaskError(
                    error_code=OutcomeCode.UPSTREAM_SKIPPED,
                    message='Upstream dependency was SKIPPED',
                    data={'dependency_index': task_index},
                )
            )
        elif stored_result:
            task_result = _decode_stored_task_result(
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
        else:
            continue  # No result to include

        dep_results.by_index[task_index] = task_result
        if node_id is not None:
            dep_results.by_id[node_id] = task_result
            dep_results.task_name_by_id[node_id] = task_name
            dep_results.definition_key_by_id[node_id] = sub_definition_key
        # Use unique key to avoid collisions when same task appears multiple times
        unique_key = f'{task_name}#{task_index}'
        dep_results.by_name[unique_key] = task_result

    return dep_results








async def check_workflow_completion(
    session: AsyncSession,
    workflow_id: str,
    broker: 'PostgresBroker | None' = None,
) -> None:
    """
    Check if all workflow tasks are complete and update workflow status.
    Handles cases where workflow may already be FAILED but DAG is still resolving.

    If success_policy is set, evaluates success cases instead of "any failure → FAILED".

    Args:
        session: Active DB session.
        workflow_id: Workflow id to finalize.
        broker: Optional broker for parent propagation hooks.
    """
    # Serialize completion checks per workflow to avoid races when multiple
    # tasks finish concurrently in separate transactions.
    lock_result = await session.execute(
        LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL,
        {'wf_id': workflow_id},
    )
    if lock_result.fetchone() is None:
        return

    # Get current workflow status, error, success_policy, and task counts
    result = await session.execute(
        GET_WORKFLOW_COMPLETION_STATUS_SQL,
        {
            'wf_id': workflow_id,
            'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES,
        },
    )

    row = result.fetchone()
    if row is None:
        return

    current_status = row.status
    already_completed = row.completed_at is not None
    has_error = row.error is not None
    success_policy_data = row.success_policy  # JSONB, may be None
    workflow_name = row.name
    incomplete = row.incomplete or 0
    failed = row.failed or 0
    completed = row.completed or 0
    total = row.total or 0

    # Only a RUNNING workflow may be finalized. PAUSED waits for manual
    # intervention; CANCELLED/COMPLETED/FAILED are final — a late task
    # completion must not resurrect them (CANCELLED has completed_at NULL,
    # so the already_completed check below does not cover it).
    if current_status != 'RUNNING':
        return

    if incomplete > 0:
        return  # Still running

    if already_completed:
        return  # Already finalized

    # All tasks done - determine final result
    final_result = await get_workflow_final_result(session, workflow_id)

    # Determine final status using success policy or default behavior
    workflow_succeeded = await evaluate_workflow_success(
        session, workflow_id, success_policy_data, has_error, failed
    )

    if workflow_succeeded:
        await session.execute(
            MARK_WORKFLOW_COMPLETED_SQL,
            {'wf_id': workflow_id, 'result': final_result},
        )
        logger.info(
            f"Workflow '{workflow_name}' ({workflow_id[:8]}) COMPLETED: "
            f'{completed}/{total} tasks succeeded, {failed} failed'
        )
    else:
        # Recompute failure error from terminal task results for deterministic
        # final error selection (e.g., first failed task by task_index).
        error_json = await get_workflow_failure_error(
            session, workflow_id, success_policy_data
        )

        await session.execute(
            MARK_WORKFLOW_FAILED_SQL,
            {'wf_id': workflow_id, 'result': final_result, 'error': error_json},
        )
        logger.info(
            f"Workflow '{workflow_name}' ({workflow_id[:8]}) FAILED: "
            f'{completed}/{total} tasks succeeded, {failed} failed'
        )

    # Send NOTIFY for workflow completion
    await session.execute(
        NOTIFY_WORKFLOW_DONE_SQL,
        {'wf_id': workflow_id},
    )

    # If this is a child workflow, notify parent
    parent_result = await session.execute(
        GET_PARENT_WORKFLOW_INFO_SQL,
        {'wf_id': workflow_id},
    )
    parent_row = parent_result.fetchone()
    if parent_row and parent_row.parent_workflow_id is not None:
        # This is a child workflow - notify parent
        await on_subworkflow_complete(session, workflow_id, broker)





async def on_subworkflow_complete(
    session: AsyncSession,
    child_workflow_id: str,
    broker: 'PostgresBroker | None' = None,
) -> None:
    """
    Called when a child workflow completes.
    Updates parent node status and propagates to parent DAG.
    """
    from horsies.core.models.tasks import TaskResult, SubWorkflowError

    # 1. Get child workflow info and task counts
    child_result = await session.execute(
        GET_CHILD_WORKFLOW_INFO_SQL,
        {'child_id': child_workflow_id},
    )

    row = child_result.fetchone()
    if row is None:
        logger.error(f'Child workflow {child_workflow_id} not found')
        return

    child_status = row.status
    child_result_json = row.result
    child_error = row.error
    parent_wf_id = row.parent_workflow_id
    parent_task_idx = row.parent_task_index
    total_tasks = row.total or 0
    completed_tasks = row.completed or 0
    failed_tasks = row.failed or 0
    skipped_tasks = row.skipped or 0

    if parent_wf_id is None:
        # Not a child workflow (or already detached)
        return

    # 2. Build SubWorkflowSummary
    # SubWorkflowSummary.output is `Any` — typed decode is the parent
    # task's concern (it'll get the value through args_from / ctx_from
    # with the workflow's declared OutT). Here we extract the raw ok
    # payload from the envelope without typed decode; the parent
    # decodes properly when it consumes the summary downstream.
    child_output: Any = None
    if child_result_json:
        deser = _deser_json(child_result_json, 'child result json')
        if isinstance(deser, dict) and deser.get('__h_task_result__') is True:
            envelope = cast('dict[str, Any]', deser)
            ok_slot = envelope.get('ok')
            err_slot = envelope.get('err')
            if err_slot is None:
                # Success path — ok slot is the raw encoded value.
                child_output = ok_slot

    error_summary: str | None = None
    if child_error:
        error_data = _deser_json(child_error, 'child error')
        if isinstance(error_data, dict):
            msg = error_data.get('message')
            if isinstance(msg, str):
                error_summary = msg
        elif error_data is None:
            error_summary = str(child_error)[:200]

    child_summary = SubWorkflowSummary(
        status=WorkflowStatus(child_status),
        output=child_output,
        total_tasks=total_tasks,
        completed_tasks=completed_tasks,
        failed_tasks=failed_tasks,
        skipped_tasks=skipped_tasks,
        error_summary=error_summary,
    )

    # 3. Determine parent node status and result
    if child_status == 'COMPLETED':
        parent_node_status = 'COMPLETED'
        # Pass through child's output as TaskResult
        parent_node_result = child_result_json
    else:
        parent_node_status = 'FAILED'
        # Create SubWorkflowError
        error = SubWorkflowError(
            error_code=OutcomeCode.SUBWORKFLOW_FAILED,
            message=f'Subworkflow {child_workflow_id} failed with status {child_status}',
            sub_workflow_id=child_workflow_id,
            sub_workflow_summary=child_summary,
        )
        # Strict-serde §5: build the `__h_task_result__` envelope
        # directly. ``serialize_error_payload`` runs the err slot
        # through ``encode_value(err, TaskError)`` which drops the
        # SubWorkflowError-specific fields (``sub_workflow_id`` /
        # ``sub_workflow_summary``); ``model_dump`` on the concrete
        # class preserves them. The reserved-key scan isn't applied
        # here because this is an engine-internal emit, not user data.
        parent_node_result = _ser(
            dumps_json({
                '__h_task_result__': True,
                'ok': None,
                'err': error.model_dump(mode='json'),
            }),
            'parent node error',
            fallback='null',
        )

    # 4. Serialize parent-side promotion with other completions of the same
    # parent: on_workflow_task_complete takes this lock for exactly that
    # reason, and the failure/completion paths below (steps 5 and 8) acquire
    # it anyway, in the same child→parent order. Without it, two sibling
    # child workflows finishing concurrently can each observe the other's
    # node as non-terminal and leave a fan-in dependent PENDING until the
    # next reaper sweep.
    parent_lock = await session.execute(
        LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL,
        {'wf_id': parent_wf_id},
    )
    if parent_lock.fetchone() is None:
        logger.error(
            f'Parent workflow {parent_wf_id} not found for child '
            f'{child_workflow_id}; skipping subworkflow progression'
        )
        return

    # 5. Update parent node
    update_result = await session.execute(
        UPDATE_PARENT_NODE_RESULT_SQL,
        {
            'status': parent_node_status,
            'result': parent_node_result,
            'summary': _ser(dumps_json(child_summary.to_json()), 'child summary', fallback='null'),
            'wf_id': parent_wf_id,
            'idx': parent_task_idx,
            'terminal_states': WF_TASK_TERMINAL_VALUES,
        },
    )
    if update_result.fetchone() is None:
        # Identity-only log: never log the dropped result/summary payloads, since
        # they carry arbitrary user task output. Operators can recover the source
        # row via horsies_workflows.id = child_workflow_id when needed.
        logger.debug(
            'Parent workflow task already terminal, skipping subworkflow progression: '
            'parent_workflow=%s task_index=%s child_workflow=%s attempted_status=%s',
            parent_wf_id, parent_task_idx, child_workflow_id, parent_node_status,
        )
        return

    # 6. Handle failure (same as task failure)
    if parent_node_status == 'FAILED':
        # Create a TaskResult for the failure handler
        failure_result: TaskResult[Any, TaskError] = TaskResult(
            err=SubWorkflowError(
                error_code=OutcomeCode.SUBWORKFLOW_FAILED,
                message=f'Subworkflow {child_workflow_id} failed',
                sub_workflow_id=child_workflow_id,
                sub_workflow_summary=child_summary,
            )
        )
        should_continue = await _handle_workflow_task_failure(
            session, parent_wf_id, failure_result
        )
        if not should_continue:
            # PAUSE mode - stop processing
            return

    # 7. Check if parent workflow is PAUSED
    parent_status_check = await session.execute(
        GET_WORKFLOW_STATUS_SQL,
        {'wf_id': parent_wf_id},
    )
    parent_status_row = parent_status_check.fetchone()
    if parent_status_row and parent_status_row.status == 'PAUSED':
        return  # Don't propagate - parent is paused

    # 8. Process parent dependents
    await _process_dependents(session, parent_wf_id, parent_task_idx, broker)

    # 9. Check parent completion
    await check_workflow_completion(session, parent_wf_id, broker)




async def evaluate_workflow_success(
    session: AsyncSession,
    workflow_id: str,
    success_policy_data: dict[str, Any] | str | None,
    has_error: bool,
    failed: int,
) -> bool:
    """
    Evaluate whether workflow succeeded.

    If success_policy is None: default behavior (any failed → False)
    If success_policy is set: True if any SuccessCase is satisfied
    """
    if success_policy_data is None:
        # Default behavior: succeed only if no failures and no stored error
        return not has_error and failed == 0

    # Guard: JSONB may come back as string depending on driver
    policy: dict[str, Any] | None
    if isinstance(success_policy_data, str):
        policy = _deser_json(success_policy_data, 'success_policy')
    else:
        policy = success_policy_data
    if not isinstance(policy, dict):
        logger.warning('Corrupt success_policy for workflow, treating as no policy')
        return not has_error and failed == 0

    # Build status map by task_index
    result = await session.execute(
        GET_TASK_STATUSES_SQL,
        {'wf_id': workflow_id},
    )

    status_by_index: dict[int, str] = {row.task_index: row.status for row in result.fetchall()}

    # Note: optional_indices from success_policy are ignored here because
    # optional tasks can fail without affecting whether a success case is satisfied.
    # We only check if required tasks in each case are COMPLETED.

    # Evaluate each success case
    cases = policy.get('cases', [])
    for case in cases:
        required_indices = case.get('required_indices', [])
        if not required_indices:
            continue

        # Case is satisfied if ALL required tasks are COMPLETED
        all_completed = all(
            status_by_index.get(idx) == 'COMPLETED' for idx in required_indices
        )
        if all_completed:
            return True

    # No case satisfied
    return False





async def get_workflow_failure_error(
    session: AsyncSession,
    workflow_id: str,
    success_policy_data: dict[str, Any] | str | None,
) -> str | None:
    """
    Get error for a failed workflow.

    If success_policy is set but no case satisfied, returns WORKFLOW_SUCCESS_CASE_NOT_MET.
    Otherwise, returns the first failed required task's error.
    """
    from horsies.core.models.tasks import TaskError

    if success_policy_data is None:
        # Default: get first failed task's error
        result = await session.execute(
            GET_FIRST_FAILED_TASK_RESULT_SQL,
            {'wf_id': workflow_id},
        )
        row = result.fetchone()
        if row and row.result:
            # Failed-task path needs only the err slot — TaskError has a
            # fixed schema so decode without ok_type via _decode_err_only.
            err_tr = _decode_err_only(row.result)
            if err_tr is not None and err_tr.is_err() and err_tr.err:
                # Re-encode using the runtime type so SubWorkflowError
                # subclass fields (`sub_workflow_id` /
                # `sub_workflow_summary`) survive the round-trip.
                # `encode_value(err, TaskError)` would silently downcast.
                return _ser(
                    dumps_json(encode_value(err_tr.err, type(err_tr.err))),
                    'task error',
                    fallback='null',
                )
        return None

    # Guard: JSONB may come back as string depending on driver
    policy: dict[str, Any] | None
    if isinstance(success_policy_data, str):
        policy = _deser_json(success_policy_data, 'success_policy')
    else:
        policy = success_policy_data
    if not isinstance(policy, dict):
        logger.warning('Corrupt success_policy for workflow, treating as no policy')
        return None

    # With success_policy: find first failed required task or use sentinel error
    # Collect all required indices across all cases
    all_required: set[int] = set()
    for case in policy.get('cases', []):
        all_required.update(case.get('required_indices', []))

    if all_required:
        # Get first failed required task
        result = await session.execute(
            GET_FIRST_FAILED_REQUIRED_TASK_SQL,
            {'wf_id': workflow_id, 'required': list(all_required)},
        )
        row = result.fetchone()
        if row and row.result:
            # Failed-required-task path needs only the err slot (TaskError
            # has a fixed schema; ok_type not needed).
            err_tr = _decode_err_only(row.result)
            if err_tr is not None and err_tr.is_err() and err_tr.err:
                # Re-encode using the runtime type so SubWorkflowError
                # subclass fields (`sub_workflow_id` /
                # `sub_workflow_summary`) survive the round-trip.
                # `encode_value(err, TaskError)` would silently downcast.
                return _ser(
                    dumps_json(encode_value(err_tr.err, type(err_tr.err))),
                    'task error',
                    fallback='null',
                )

    # No required task failed, but no case was satisfied (all SKIPPED?)
    return _ser(
        dumps_json(
            encode_value(
                TaskError(
                    error_code=OutcomeCode.WORKFLOW_SUCCESS_CASE_NOT_MET,
                    message='No success case was satisfied',
                ),
                TaskError,
            ),
        ),
        'success case not met error',
        fallback='null',
    )






async def get_workflow_final_result(
    session: AsyncSession,
    workflow_id: str,
) -> str:
    """
    Get the final workflow result.

    If output_task_index is set: return that task's result
    Otherwise: return dict of terminal task results (tasks with no dependents)
    """
    # Check for explicit output task
    wf_result = await session.execute(
        GET_WORKFLOW_OUTPUT_INDEX_SQL,
        {'wf_id': workflow_id},
    )
    wf_row = wf_result.fetchone()

    if wf_row and wf_row.output_task_index is not None:
        # Return explicit output task's result
        output_result = await session.execute(
            GET_OUTPUT_TASK_RESULT_SQL,
            {'wf_id': workflow_id, 'idx': wf_row.output_task_index},
        )
        output_row = output_result.fetchone()
        return output_row.result if output_row and output_row.result else 'null'

    # Find terminal tasks (not in any other task's dependencies).
    # Strict-serde phase 5/6: each row already carries a
    # `__h_task_result__` envelope (worker emits these via
    # `encode_task_result`). Per-node typed decode happens at the
    # WorkflowHandle layer using the spec's source-node ok_types.
    # Here we store the raw envelopes verbatim into the workflow's
    # outer envelope so the handle can dispatch on them.
    terminal_results = await session.execute(
        GET_TERMINAL_TASK_RESULTS_SQL,
        {'wf_id': workflow_id},
    )

    results_dict: dict[str, Json] = {}
    task_name_by_id: dict[str, str] = {}
    definition_key_by_id: dict[str, str | None] = {}
    for row in terminal_results.fetchall():
        node_id = row.node_id
        if not isinstance(node_id, str):
            continue
        task_name_by_id[node_id] = row.task_name
        definition_key_by_id[node_id] = row.sub_definition_key
        if row.result:
            deser = _deser_json(row.result, 'terminal task result json')
            if isinstance(deser, dict) and deser.get('__h_task_result__') is True:
                results_dict[node_id] = cast('Json', deser)
            else:
                # Corrupt / pre-strict row — store null; handle layer
                # surfaces a RESULT_DESERIALIZATION_ERROR per-entry.
                logger.warning(
                    f'Terminal task {node_id!r} result is not a '
                    f'strict-serde envelope; storing null'
                )
                results_dict[node_id] = None
        else:
            results_dict[node_id] = None

    # Outputless workflow envelope. Outer marker is the standard
    # ``__h_outputless_terminals__`` so the handle's decode path can
    # distinguish from a normal-output workflow. Each entry in
    # ``results_by_id`` is itself a ``__h_task_result__`` envelope;
    # ``definition_key_by_id`` lets the handle resolve per-node OkT for a
    # SubWorkflowNode terminal by its unique definition_key (task_name is
    # ambiguous). ``task_name_by_id`` resolves TaskNode terminals.
    outer_envelope: dict[str, Json] = {
        '__h_task_result__': True,
        '__h_outputless_terminals__': True,
        'ok': cast('Json', {
            'results_by_id': cast('Json', results_dict),
            'task_name_by_id': cast('Json', task_name_by_id),
            'definition_key_by_id': cast('Json', definition_key_by_id),
        }),
        'err': None,
    }
    return _ser(dumps_json(outer_envelope), 'outputless workflow result', fallback='null')






async def _handle_workflow_task_failure(
    session: AsyncSession,
    workflow_id: str,
    result: 'TaskResult[Any, TaskError]',
) -> bool:
    """
    Handle a failed workflow task based on on_error policy.

    Returns True if dependency propagation should continue, False to stop (PAUSE mode).

    Note: The failed task's result is already stored. Dependents will receive
    the TaskResult with is_err()=True if they have args_from pointing to this task.
    """
    from horsies.core.models.tasks import TaskError

    lock_result = await session.execute(
        LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL,
        {'wf_id': workflow_id},
    )
    if lock_result.fetchone() is None:
        return True

    # Get workflow's on_error policy
    wf_result = await session.execute(
        GET_WORKFLOW_ON_ERROR_SQL,
        {'wf_id': workflow_id},
    )

    wf_row = wf_result.fetchone()
    if wf_row is None:
        return True

    on_error = wf_row.on_error

    # Extract TaskError for storage (not the full TaskResult). Route
    # through ``encode_task_error`` so SubWorkflowError subclass fields
    # (``sub_workflow_id`` / ``sub_workflow_summary``) survive the
    # round-trip; ``encode_value(err, TaskError)`` would silently
    # downcast.
    error_payload = (
        _ser(dumps_json(encode_task_error(result.err)), 'error payload')
        if result.is_err() and result.err
        else None
    )

    if on_error == 'fail':
        # Store error but keep status RUNNING until DAG fully resolves
        # This allows allow_failed_deps tasks to run and produce meaningful final result
        # Status will be set to FAILED in check_workflow_completion when all tasks are terminal
        first_failure_payload = await get_workflow_failure_error(
            session, workflow_id, None
        )
        await session.execute(
            SET_WORKFLOW_ERROR_SQL,
            {'wf_id': workflow_id, 'error': first_failure_payload or error_payload},
        )
        return True  # Continue dependency propagation

    elif on_error == 'pause':
        # Pause workflow for manual intervention - STOP all processing
        pause_result = await session.execute(
            PAUSE_WORKFLOW_ON_ERROR_SQL,
            {'wf_id': workflow_id, 'error': error_payload},
        )
        if pause_result.fetchone() is None:
            return False

        await cascade_pause_to_children(session, workflow_id)

        # Notify of pause (so clients can react via get())
        await session.execute(
            NOTIFY_WORKFLOW_DONE_SQL,
            {'wf_id': workflow_id},
        )

        return (
            False  # Stop dependency propagation - pending tasks stay pending for resume
        )

    return True  # Default: continue


def _load_workflow_def_from_key(
    definition_key: str,
) -> 'type[WorkflowDefinition[Any]] | None':
    """Resolve a workflow definition through the explicit definition_key registry."""
    from horsies.core.workflows.registry import get_workflow_definition

    return get_workflow_definition(definition_key)


# --- Backward-compat barrel re-exports from lifecycle.py ---
from horsies.core.workflows import lifecycle as _lifecycle  # noqa: E402

as_str_list = _lifecycle.as_str_list
guard_no_positional_args = _lifecycle.guard_no_positional_args
start_workflow_async = _lifecycle.start_workflow_async
start_workflow = _lifecycle.start_workflow
pause_workflow = _lifecycle.pause_workflow
pause_workflow_sync = _lifecycle.pause_workflow_sync
resume_workflow = _lifecycle.resume_workflow
resume_workflow_sync = _lifecycle.resume_workflow_sync
cascade_pause_to_children = _lifecycle.cascade_pause_to_children
cascade_resume_to_children = _lifecycle.cascade_resume_to_children
