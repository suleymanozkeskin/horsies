"""Workflow execution engine — DAG resolution, task completion, dependency management."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, TypeVar, cast

from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.codec.serde import dumps_json, loads_json, task_result_from_json, serialize_error_payload, SerdeResult
from horsies.core.types.result import is_err
from horsies.core.logging import get_logger
from horsies.core.models.workflow import (
    SubWorkflowNode,
    SubWorkflowSummary,
    WorkflowStatus,
    WorkflowTaskStatus,
    WorkflowDefinition,
    AnyNode,
    WF_TASK_TERMINAL_VALUES,
    validate_workflow_generic_output_match,
)
from horsies.core.errors import WorkflowValidationError, ErrorCode
from horsies.core.workflows.sql import *  # noqa: F401, F403

logger = get_logger('workflow.engine')

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.tasks import TaskResult, TaskError

OutT = TypeVar('OutT')


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


async def _fail_enqueued_task(
    session: Any,
    workflow_id: str,
    task_index: int,
    message: str,
    broker: 'PostgresBroker | None' = None,
    error_code: str = 'WORKFLOW_ENQUEUE_FAILED',
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
    await session.execute(
        MARK_WORKFLOW_TASK_FAILED_SQL,
        {
            'wf_id': workflow_id,
            'idx': task_index,
            'result': serialize_error_payload(tr),
        },
    )
    if broker is not None:
        should_continue = await _handle_workflow_task_failure(
            session, workflow_id, task_index, tr,
        )
        if should_continue:
            await _process_dependents(session, workflow_id, task_index, broker)
            await check_workflow_completion(session, workflow_id, broker)






async def enqueue_workflow_task(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
    all_dep_results: dict[int, 'TaskResult[Any, TaskError]'],
    broker: 'PostgresBroker | None' = None,
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

    # Parse retry config and good_until from task_options (row[8])
    task_options_str: str | None = row[8]
    max_retries = 0
    good_until_str: str | None = None
    if task_options_str:
        options_data = _deser_json(task_options_str, 'task_options', fallback={})
        if isinstance(options_data, dict):
            retry_policy = options_data.get('retry_policy')
            if isinstance(retry_policy, dict):
                max_retries = retry_policy.get('max_retries', 3)
            good_until_raw = options_data.get('good_until')
            if good_until_raw is not None:
                good_until_str = str(good_until_raw)

    # Start with static kwargs — corrupt kwargs is fatal for this task
    raw_kwargs = _deser_json(row[3], 'workflow task kwargs')
    if raw_kwargs is None and row[3]:
        await _fail_enqueued_task(
            session,
            workflow_id,
            task_index,
            f'Corrupt kwargs JSON for task {task_index}',
            broker,
            error_code='WORKER_SERIALIZATION_ERROR',
        )
        return None
    kwargs: dict[str, Any] = raw_kwargs if isinstance(raw_kwargs, dict) else {}

    # Inject args_from: map kwarg_name -> TaskResult from dependency
    if row[6]:  # args_from
        args_from_raw = row[6]
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
                    error_code='WORKER_SERIALIZATION_ERROR',
                )
                return None
        else:
            args_from_map = args_from_raw

        if isinstance(args_from_map, dict):
            # Cast to proper type - args_from stores {kwarg_name: task_index}
            args_from_typed = cast(dict[str, int], args_from_map)
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
                    # Serialize TaskResult for transport
                    dep_ser = _ser(dumps_json(dep_result), f'dep_result for {kwarg_name}')
                    if dep_ser is None:
                        await _fail_enqueued_task(
                            session, workflow_id, task_index,
                            f'Failed to serialize dep_result for kwarg {kwarg_name!r} (task {task_index})',
                            broker,
                            error_code='WORKER_SERIALIZATION_ERROR',
                        )
                        return None
                    kwargs[kwarg_name] = {
                        '__horsies_taskresult__': True,
                        'data': dep_ser,
                    }

    # Inject workflow_ctx if workflow_ctx_from is set
    if row[7]:  # workflow_ctx_from
        ctx_from_ids = cast(list[str], row[7])  # Already a list from PostgreSQL ARRAY
        ctx_data = await _build_workflow_context_data(
            session=session,
            workflow_id=workflow_id,
            task_index=task_index,
            task_name=row[1],
            ctx_from_ids=ctx_from_ids,
        )
        # Will be filtered out by worker if task doesn't declare workflow_ctx param
        kwargs['__horsies_workflow_ctx__'] = ctx_data

    # Inject workflow metadata for tasks that declare workflow_meta.
    # Worker will filter it out if the function does not accept workflow_meta.
    kwargs['__horsies_workflow_meta__'] = {
        'workflow_id': workflow_id,
        'task_index': task_index,
        'task_name': row[1],
    }

    # Serialize kwargs before creating the task — fail early on corrupt data
    kwargs_json = _ser(dumps_json(kwargs), 'workflow task kwargs')
    if kwargs_json is None:
        await _fail_enqueued_task(
            session, workflow_id, task_index,
            f'Failed to serialize kwargs for task {task_index}', broker,
            error_code='WORKER_SERIALIZATION_ERROR',
        )
        return None

    # Create actual task in tasks table
    task_id = str(uuid.uuid4())
    sent_at = datetime.now(timezone.utc)
    await session.execute(
        INSERT_TASK_FOR_WORKFLOW_SQL,
        {
            'id': task_id,
            'name': row[1],  # task_name
            'queue': row[4],  # queue_name
            'priority': row[5],  # priority
            # Compat: new writes store [] for task_args (kwargs-only).
            # Old persisted rows may still carry non-empty task_args;
            # passing them through preserves backward compatibility.
            'args': row[2],  # task_args (already JSON string)
            'kwargs': kwargs_json,
            'sent_at': sent_at,
            'max_retries': max_retries,
            'task_options': task_options_str,
            'good_until': good_until_str,
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
    parent_depth: int,
    root_workflow_id: str,
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

    _wt_id = row[0]  # Unused but kept for row unpacking clarity
    _sub_workflow_name = row[1]  # Unused but kept for row unpacking clarity
    task_args_json = row[2]
    task_kwargs_json = row[3]
    args_from_raw = row[4]
    _node_id = row[5]  # Unused but kept for row unpacking clarity
    sub_workflow_module = row[6]
    sub_workflow_qualname = row[7]
    _sub_workflow_retry_mode = row[8]  # Currently unused (retry_mode not implemented)

    # 2. Try to get workflow_def from registry (fast path) or import fallback
    #
    # Registry lookup succeeds when:
    #   - Parent workflow module is imported in worker
    #   - WorkflowSpec.build(app) was called (registers nodes)
    # This typically works in tests but rarely in production workers.
    #
    # Fallback import path (sub_workflow_module/qualname stored in DB) handles
    # the common case where registry is empty.
    workflow_name_result = await session.execute(
        GET_WORKFLOW_NAME_SQL,
        {'wf_id': workflow_id},
    )
    workflow_name_row = workflow_name_result.fetchone()
    if workflow_name_row is None:
        await _fail_enqueued_task(
            session, workflow_id, task_index,
            f'Workflow {workflow_id} not found during subworkflow enqueue', broker,
            error_code='WORKFLOW_ENQUEUE_FAILED',
        )
        return None

    workflow_name = workflow_name_row[0]

    from horsies.core.workflows.registry import get_subworkflow_node

    subworkflow_node = get_subworkflow_node(workflow_name, task_index)

    workflow_def: type[WorkflowDefinition[Any]] | None = None
    if subworkflow_node is not None:
        # Registry hit: use cached workflow_def
        workflow_def = subworkflow_node.workflow_def
    elif sub_workflow_module and sub_workflow_qualname:
        # Registry miss: load child workflow def via import path (stored in DB)
        workflow_def = _load_workflow_def_from_path(
            sub_workflow_module, sub_workflow_qualname
        )

    if workflow_def is None:
        # Critical: Revert ENQUEUED → FAILED to prevent stuck task
        # This can happen if the subworkflow module cannot be imported
        from horsies.core.models.tasks import TaskError, TaskResult

        error = TaskError(
            error_code='SUBWORKFLOW_LOAD_FAILED',
            message=f'Failed to load subworkflow definition for {workflow_name}:{task_index}',
            data={'module': sub_workflow_module, 'qualname': sub_workflow_qualname},
        )
        await session.execute(
            MARK_WORKFLOW_TASK_FAILED_SQL,
            {
                'wf_id': workflow_id,
                'idx': task_index,
                'result': _ser(dumps_json(TaskResult(err=error)), 'subworkflow load error', fallback='null'),
            },
        )
        logger.error(f'SubWorkflowNode load failed for {workflow_name}:{task_index}')

        # Handle failure and propagate to dependents
        failure_result: TaskResult[Any, TaskError] = TaskResult(err=error)
        should_continue = await _handle_workflow_task_failure(
            session, workflow_id, task_index, failure_result
        )
        if should_continue:
            await _process_dependents(session, workflow_id, task_index, broker)
            await check_workflow_completion(session, workflow_id, broker)

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
            error_code='WORKER_SERIALIZATION_ERROR',
        )
        return None
    kwargs: dict[str, Any] = raw_kwargs if isinstance(raw_kwargs, dict) else {}

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
                    error_code='WORKER_SERIALIZATION_ERROR',
                )
                return None
        else:
            args_from_map = args_from_raw

        if isinstance(args_from_map, dict):
            args_from_typed = cast(dict[str, int], args_from_map)
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
                    # Serialize TaskResult for transport
                    dep_ser = _ser(dumps_json(dep_result), f'dep_result for {kwarg_name}')
                    if dep_ser is None:
                        await _fail_enqueued_task(
                            session, workflow_id, task_index,
                            f'Failed to serialize dep_result for kwarg {kwarg_name!r} (subworkflow task {task_index})',
                            broker,
                            error_code='WORKER_SERIALIZATION_ERROR',
                        )
                        return None
                    kwargs[kwarg_name] = {
                        '__horsies_taskresult__': True,
                        'data': dep_ser,
                    }

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
            error_code='WORKFLOW_ENQUEUE_FAILED',
        )
        return None

    # 5. Create child workflow with parent reference
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
            'wf_module': child_spec.workflow_def_module,
            'wf_qualname': child_spec.workflow_def_qualname,
            'parent_wf_id': workflow_id,
            'parent_idx': task_index,
            'depth': parent_depth + 1,
            'root_wf_id': root_workflow_id,
            'sent_at': child_sent_at,
        },
    )

    # 6. Insert child workflow_tasks
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
                    error_code='WORKFLOW_ENQUEUE_FAILED',
                )
                return None
            child_kwargs_json = _ser(dumps_json(child_sub.kwargs), 'child sub kwargs')
            if child_kwargs_json is None:
                await _fail_enqueued_task(
                    session, workflow_id, task_index,
                    f'Failed to serialize kwargs for child sub {child_sub.name}', broker,
                    error_code='WORKER_SERIALIZATION_ERROR',
                )
                return None
            await session.execute(
                INSERT_WORKFLOW_TASK_SUBWORKFLOW_SQL,
                {
                    'id': child_wt_id,
                    'wf_id': child_id,
                    'idx': child_sub.index,
                    'node_id': child_sub.node_id,
                    'name': child_sub.name,
                    'args': _ser(dumps_json(()), 'positional args', fallback='[]'),
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
                    'sub_wf_retry_mode': child_sub.retry_mode.value,
                    'sub_wf_module': child_sub.workflow_def.__module__,
                    'sub_wf_qualname': child_sub.workflow_def.__qualname__,
                },
            )
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
                    error_code='WORKFLOW_ENQUEUE_FAILED',
                )
                return None
            child_task_options_json: str | None = getattr(
                child_task.fn, 'task_options_json', None
            )
            if child_task.good_until is not None:
                child_base_options: dict[str, Any] = {}
                if child_task_options_json:
                    parsed = _deser_json(child_task_options_json, 'child task_options', fallback={})
                    if isinstance(parsed, dict):
                        child_base_options = parsed
                child_base_options['good_until'] = child_task.good_until.isoformat()
                child_task_options_json = _ser(dumps_json(child_base_options), 'child task_options')

            child_kwargs_json = _ser(dumps_json(child_task.kwargs), 'child task kwargs')
            if child_kwargs_json is None:
                await _fail_enqueued_task(
                    session, workflow_id, task_index,
                    f'Failed to serialize kwargs for child task {child_task.name}', broker,
                    error_code='WORKER_SERIALIZATION_ERROR',
                )
                return None
            await session.execute(
                INSERT_WORKFLOW_TASK_SQL,
                {
                    'id': child_wt_id,
                    'wf_id': child_id,
                    'idx': child_task.index,
                    'node_id': child_task.node_id,
                    'name': child_task.name,
                    'args': _ser(dumps_json(()), 'positional args', fallback='[]'),
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
            )

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
                    parent_depth + 1,
                    root_workflow_id,
                )
            else:
                await enqueue_workflow_task(session, child_id, child_root.index, {}, broker)

    logger.info(f'Started child workflow {child_id} for {workflow_name}:{task_index}')
    return child_id




async def _build_workflow_context_data(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
    task_name: str,
    ctx_from_ids: list[str],
) -> dict[str, Any]:
    """
    Build serializable workflow context data.

    Returns a plain dict that can be JSON-serialized and reconstructed
    on the worker side into a WorkflowContext.

    Results are keyed by node_id for stable lookup in WorkflowContext.
    Also fetches SubWorkflowSummary for SubWorkflowNodes.
    """
    # Fetch results for the specified node_ids
    dep_results = await get_dependency_results_with_names(
        session, workflow_id, ctx_from_ids
    )

    results_by_id: dict[str, str] = {}
    for node_id, result in dep_results.by_id.items():
        ser = _ser(dumps_json(result), 'workflow ctx result', fallback='null')
        if ser is not None:
            results_by_id[node_id] = ser

    # Fetch summaries for SubWorkflowNodes
    summaries_by_id: dict[str, str] = {}
    if ctx_from_ids:
        summary_result = await session.execute(
            GET_SUBWORKFLOW_SUMMARIES_SQL,
            {'wf_id': workflow_id, 'node_ids': ctx_from_ids},
        )
        for row in summary_result.fetchall():
            node_id = row[0]
            summary_json = row[1]
            if node_id and summary_json:
                summaries_by_id[node_id] = summary_json

    return {
        'workflow_id': workflow_id,
        'task_index': task_index,
        'task_name': task_name,
        'results_by_id': results_by_id,
        'summaries_by_id': summaries_by_id,
    }






async def on_workflow_task_complete(
    session: AsyncSession,
    task_id: str,
    result: 'TaskResult[Any, TaskError]',
    broker: 'PostgresBroker | None' = None,
) -> None:
    """
    Called from worker._finalize_after when a task completes.
    Handles workflow task status update and dependency resolution.
    """
    # 1. Find workflow_task by task_id
    wt_result = await session.execute(
        GET_WORKFLOW_TASK_BY_TASK_ID_SQL,
        {'tid': task_id},
    )

    row = wt_result.fetchone()
    if row is None:
        return  # Not a workflow task

    workflow_id = row[0]
    task_index = row[1]

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
    update_result = await session.execute(
        UPDATE_WORKFLOW_TASK_RESULT_SQL,
        {
            'status': new_status,
            'result': _ser(dumps_json(result), 'task completion result', fallback='null'),
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
            session, workflow_id, task_index, result
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
    if status_row and status_row[0] == 'PAUSED':
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
    depth = wf_row[0] if wf_row else 0
    root_wf_id = wf_row[1] if wf_row else workflow_id

    for row in dependents.fetchall():
        await try_make_ready_and_enqueue(
            session, broker, workflow_id, row[0], depth, root_wf_id
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

    task_status = config_row[0]
    raw_deps = config_row[1]
    allow_failed_deps: bool = config_row[2] if config_row[2] is not None else False
    join_type: str = config_row[3] or 'all'
    min_success: int | None = config_row[4]
    raw_ctx_from = config_row[5]
    is_subworkflow: bool = config_row[6] if config_row[6] is not None else False
    wf_status = config_row[7]

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
        row[0]: row[1] for row in dep_status_result.fetchall()
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

    # 6. Evaluate conditions (run_when/skip_when) if set
    # Requires workflow name to look up TaskNode from registry
    workflow_name_result = await session.execute(
        GET_WORKFLOW_NAME_SQL,
        {'wf_id': workflow_id},
    )
    workflow_name_row = workflow_name_result.fetchone()
    workflow_name = workflow_name_row[0] if workflow_name_row else None

    if workflow_name:
        condition_action, condition_result = await _evaluate_conditions(
            session, workflow_id, workflow_name, task_index, dependencies
        )
        if condition_action == 'skip':
            if condition_result is None:
                return
            update_result = await session.execute(
                SET_READY_WORKFLOW_TASK_TERMINAL_SQL,
                {
                    'status': 'SKIPPED',
                    'result': _ser(dumps_json(condition_result), 'skip_when result', fallback='null'),
                    'wf_id': workflow_id,
                    'idx': task_index,
                },
            )
            if update_result.fetchone() is None:
                return
            await _process_dependents(session, workflow_id, task_index, broker)
            return
        if condition_action == 'fail':
            if condition_result is None:
                return
            update_result = await session.execute(
                SET_READY_WORKFLOW_TASK_TERMINAL_SQL,
                {
                    'status': 'FAILED',
                    'result': _ser(dumps_json(condition_result), 'run_when result', fallback='null'),
                    'wf_id': workflow_id,
                    'idx': task_index,
                },
            )
            if update_result.fetchone() is None:
                return
            should_continue = await _handle_workflow_task_failure(
                session, workflow_id, task_index, condition_result
            )
            if not should_continue:
                return
            await _process_dependents(session, workflow_id, task_index, broker)
            await check_workflow_completion(session, workflow_id, broker)
            return

    # 7. Fetch dependency results and enqueue
    dep_results = await get_dependency_results(session, workflow_id, dependencies)

    if is_subworkflow and broker is not None:
        # SubWorkflowNode: start child workflow
        actual_root = root_workflow_id or workflow_id
        await enqueue_subworkflow_task(
            session, broker, workflow_id, task_index, dep_results, depth, actual_root
        )
    else:
        # Regular TaskNode: enqueue as task
        await enqueue_workflow_task(session, workflow_id, task_index, dep_results, broker)




async def _evaluate_conditions(
    session: AsyncSession,
    workflow_id: str,
    workflow_name: str,
    task_index: int,
    dependencies: list[int],
) -> tuple[str, 'TaskResult[Any, TaskError] | None']:
    """
    Evaluate run_when/skip_when conditions for a task.

    Returns:
      ('proceed', None) when task should be enqueued.
      ('skip', TaskResult(err=...)) when task is conditionally skipped.
      ('fail', TaskResult(err=...)) when condition evaluation raises.

    Conditions are evaluated in order:
    1. If skip_when returns True → skip
    2. Else if run_when returns False → skip
    3. Otherwise → proceed
    """
    from horsies.core.workflows.registry import get_task_node
    from horsies.core.models.workflow import WorkflowContext

    wf_def: type[WorkflowDefinition[Any]] | None = None
    node = get_task_node(workflow_name, task_index)
    if node is None:
        # Try to load workflow definition by import path (Option B.1)
        def_result = await session.execute(
            GET_WORKFLOW_DEF_PATH_SQL,
            {'wf_id': workflow_id},
        )
        def_row = def_result.fetchone()
        if def_row and def_row[0] and def_row[1]:
            wf_def = _load_workflow_def_from_path(def_row[0], def_row[1])
            if wf_def is not None:
                node = _node_from_workflow_def(wf_def, task_index)
        if node is None:
            # Node not registered (workflow module not imported in this process)
            # Proceed without condition evaluation
            return ('proceed', None)

    node_any: AnyNode = node

    if node_any.skip_when is None and node_any.run_when is None:
        # No conditions to evaluate
        return ('proceed', None)

    # Build WorkflowContext for condition evaluation
    # Use workflow_ctx_from if set, otherwise use all dependencies
    ctx_from_ids: list[str]
    if node_any.workflow_ctx_from:
        ctx_from_ids = [
            n.node_id for n in node_any.workflow_ctx_from if n.node_id is not None
        ]
    else:
        ctx_from_ids = []
        for dep_index in dependencies:
            dep_node = get_task_node(workflow_name, dep_index)
            if dep_node is None:
                if 'wf_def' in locals() and wf_def is not None:
                    dep_node = _node_from_workflow_def(wf_def, dep_index)
            if dep_node and dep_node.node_id is not None:
                ctx_from_ids.append(dep_node.node_id)

    # Fetch results for context
    dep_results = await get_dependency_results_with_names(
        session, workflow_id, ctx_from_ids
    )

    # Fetch summaries for SubWorkflowNodes in context
    summaries_by_id: dict[str, SubWorkflowSummary[Any]] = {}
    if ctx_from_ids:
        summary_result = await session.execute(
            GET_SUBWORKFLOW_SUMMARIES_SQL,
            {'wf_id': workflow_id, 'node_ids': ctx_from_ids},
        )
        for row in summary_result.fetchall():
            node_id = row[0]
            summary_json = row[1]
            if node_id and summary_json:
                parsed = _deser_json(summary_json, 'summary')
                if isinstance(parsed, dict):
                    try:
                        summaries_by_id[node_id] = SubWorkflowSummary.from_json(parsed)
                    except Exception:
                        continue

    # Get task name for context
    task_name = node_any.name

    # Build context
    ctx = WorkflowContext(
        workflow_id=workflow_id,
        task_index=task_index,
        task_name=task_name,
        results_by_id=dep_results.by_id,
        summaries_by_id=summaries_by_id,
    )

    from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode

    if node_any.skip_when is not None:
        try:
            if node_any.skip_when(ctx):
                skip_result: TaskResult[Any, TaskError] = TaskResult(
                    err=TaskError(
                        error_code=LibraryErrorCode.WORKFLOW_CONDITION_SKIP_WHEN_TRUE,
                        message='Task skipped because skip_when returned True',
                        data={'condition': 'skip_when'},
                    )
                )
                return ('skip', skip_result)
        except Exception as e:
            logger.warning(f'Condition evaluation failed for task {task_name}: {e}')
            fail_result: TaskResult[Any, TaskError] = TaskResult(
                err=TaskError(
                    exception=e,
                    error_code=LibraryErrorCode.WORKFLOW_CONDITION_EVALUATION_ERROR,
                    message=f'Condition evaluation failed in skip_when: {type(e).__name__}: {e}',
                    data={'condition': 'skip_when', 'exception_type': type(e).__name__},
                )
            )
            return ('fail', fail_result)

    if node_any.run_when is not None:
        try:
            if not node_any.run_when(ctx):
                skip_result = TaskResult(
                    err=TaskError(
                        error_code=LibraryErrorCode.WORKFLOW_CONDITION_RUN_WHEN_FALSE,
                        message='Task skipped because run_when returned False',
                        data={'condition': 'run_when'},
                    )
                )
                return ('skip', skip_result)
        except Exception as e:
            logger.warning(f'Condition evaluation failed for task {task_name}: {e}')
            fail_result = TaskResult(
                err=TaskError(
                    exception=e,
                    error_code=LibraryErrorCode.WORKFLOW_CONDITION_EVALUATION_ERROR,
                    message=f'Condition evaluation failed in run_when: {type(e).__name__}: {e}',
                    data={'condition': 'run_when', 'exception_type': type(e).__name__},
                )
            )
            return ('fail', fail_result)

    return ('proceed', None)


class DependencyResults:
    """Container for dependency results with index, name, and node_id mappings."""

    def __init__(self) -> None:
        self.by_index: dict[int, 'TaskResult[Any, TaskError]'] = {}
        self.by_name: dict[str, 'TaskResult[Any, TaskError]'] = {}
        self.by_id: dict[str, 'TaskResult[Any, TaskError]'] = {}




async def get_dependency_results(
    session: AsyncSession,
    workflow_id: str,
    dependency_indices: list[int],
) -> dict[int, 'TaskResult[Any, TaskError]']:
    """
    Fetch TaskResults for dependencies in terminal states.

    - COMPLETED/FAILED: returns actual TaskResult from stored result
    - SKIPPED: returns sentinel TaskResult with UPSTREAM_SKIPPED error
    """
    from horsies.core.models.tasks import LibraryErrorCode, TaskError, TaskResult

    if not dependency_indices:
        return {}

    result = await session.execute(
        GET_DEPENDENCY_RESULTS_SQL,
        {
            'wf_id': workflow_id,
            'indices': dependency_indices,
            'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES,
        },
    )

    results: dict[int, TaskResult[Any, TaskError]] = {}
    for row in result.fetchall():
        task_index = row[0]
        status = row[1]
        stored_result = row[2]

        if status == WorkflowTaskStatus.SKIPPED.value:
            # Inject sentinel TaskResult for SKIPPED dependencies
            results[task_index] = TaskResult(
                err=TaskError(
                    error_code=LibraryErrorCode.UPSTREAM_SKIPPED,
                    message='Upstream dependency was SKIPPED',
                    data={'dependency_index': task_index},
                )
            )
        elif stored_result:
            deser = _deser_json(stored_result, 'dep result json')
            if deser is None:
                results[task_index] = TaskResult(
                    err=TaskError(
                        error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                        message=f'Dependency result JSON corrupt (task_index={task_index})',
                        data={'workflow_id': workflow_id, 'task_index': task_index, 'stage': 'json_parse', 'status': status},
                    ),
                )
                continue
            parsed_tr = task_result_from_json(deser)
            if is_err(parsed_tr):
                results[task_index] = TaskResult(
                    err=TaskError(
                        error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                        message=f'Dependency result deser failed (task_index={task_index}): {parsed_tr.err_value}',
                        data={'workflow_id': workflow_id, 'task_index': task_index, 'stage': 'task_result_parse', 'status': status},
                    ),
                )
                continue
            results[task_index] = parsed_tr.ok_value

    return results




async def get_dependency_results_with_names(
    session: AsyncSession,
    workflow_id: str,
    dependency_node_ids: list[str],
) -> DependencyResults:
    """
    Fetch TaskResults with index, name, and node_id mappings.
    Used for building WorkflowContext.

    - COMPLETED/FAILED: returns actual TaskResult from stored result
    - SKIPPED: returns sentinel TaskResult with UPSTREAM_SKIPPED error
    """
    from horsies.core.models.tasks import TaskError, LibraryErrorCode, TaskResult

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
        task_index = row[0]
        task_name = row[1]
        node_id = row[2]
        status = row[3]
        stored_result = row[4]

        if status == WorkflowTaskStatus.SKIPPED.value:
            # Inject sentinel TaskResult for SKIPPED dependencies
            task_result: TaskResult[Any, TaskError] = TaskResult(
                err=TaskError(
                    error_code=LibraryErrorCode.UPSTREAM_SKIPPED,
                    message='Upstream dependency was SKIPPED',
                    data={'dependency_index': task_index},
                )
            )
        elif stored_result:
            deser = _deser_json(stored_result, 'dep result with names json')
            if deser is None:
                task_result = TaskResult(
                    err=TaskError(
                        error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                        message=f'Dependency result JSON corrupt (task_index={task_index})',
                        data={'workflow_id': workflow_id, 'task_index': task_index, 'stage': 'json_parse', 'status': status},
                    ),
                )
            else:
                parsed_tr = task_result_from_json(deser)
                if is_err(parsed_tr):
                    task_result = TaskResult(
                        err=TaskError(
                            error_code=LibraryErrorCode.RESULT_DESERIALIZATION_ERROR,
                            message=f'Dependency result deser failed (task_index={task_index}): {parsed_tr.err_value}',
                            data={'workflow_id': workflow_id, 'task_index': task_index, 'stage': 'task_result_parse', 'status': status},
                        ),
                    )
                else:
                    task_result = parsed_tr.ok_value
        else:
            continue  # No result to include

        dep_results.by_index[task_index] = task_result
        if node_id is not None:
            dep_results.by_id[node_id] = task_result
        # Use unique key to avoid collisions when same task appears multiple times
        unique_key = f'{task_name}#{task_index}'
        dep_results.by_name[unique_key] = task_result

    return dep_results








async def check_workflow_completion(
    session: AsyncSession,
    workflow_id: str,
    broker: 'PostgresBroker | None' = None,
    preserve_existing_error: bool = False,
) -> None:
    """
    Check if all workflow tasks are complete and update workflow status.
    Handles cases where workflow may already be FAILED but DAG is still resolving.

    If success_policy is set, evaluates success cases instead of "any failure → FAILED".

    Args:
        session: Active DB session.
        workflow_id: Workflow id to finalize.
        broker: Optional broker for parent propagation hooks.
        preserve_existing_error: When True and workflow.error is already set,
            keep existing error payload instead of recomputing from failed tasks.
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

    current_status = row[0]
    already_completed = row[1] is not None
    has_error = row[2] is not None
    success_policy_data = row[3]  # JSONB, may be None
    workflow_name = row[4]
    incomplete = row[5] or 0
    failed = row[6] or 0
    completed = row[7] or 0
    total = row[8] or 0

    # Don't process if workflow is PAUSED (waiting for manual intervention)
    if current_status == 'PAUSED':
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
        error_json: str | None
        if preserve_existing_error and has_error:
            # MARK_WORKFLOW_FAILED_SQL uses COALESCE(:error, error), so passing
            # None preserves an already-set error payload.
            error_json = None
        else:
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
    if parent_row and parent_row[0] is not None:
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

    child_status = row[0]
    child_result_json = row[1]
    child_error = row[2]
    parent_wf_id = row[3]
    parent_task_idx = row[4]
    total_tasks = row[5] or 0
    completed_tasks = row[6] or 0
    failed_tasks = row[7] or 0
    skipped_tasks = row[8] or 0

    if parent_wf_id is None:
        # Not a child workflow (or already detached)
        return

    # 2. Build SubWorkflowSummary
    child_output: Any = None
    if child_result_json:
        deser = _deser_json(child_result_json, 'child result json')
        if deser is not None:
            parsed_tr = task_result_from_json(deser)
            if not is_err(parsed_tr):
                parsed_result = parsed_tr.ok_value
                if parsed_result.is_ok():
                    child_output = parsed_result.ok

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
        success_case=None,  # TODO: extract from child if success_policy matched
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
            error_code='SUBWORKFLOW_FAILED',
            message=f'Subworkflow {child_workflow_id} failed with status {child_status}',
            sub_workflow_id=child_workflow_id,
            sub_workflow_summary=child_summary,
        )
        parent_node_result = _ser(dumps_json(TaskResult(err=error)), 'parent node error', fallback='null')

    # 4. Update parent node
    await session.execute(
        UPDATE_PARENT_NODE_RESULT_SQL,
        {
            'status': parent_node_status,
            'result': parent_node_result,
            'summary': _ser(dumps_json(child_summary), 'child summary', fallback='null'),
            'wf_id': parent_wf_id,
            'idx': parent_task_idx,
        },
    )

    # 5. Handle failure (same as task failure)
    if parent_node_status == 'FAILED':
        # Create a TaskResult for the failure handler
        failure_result: TaskResult[Any, TaskError] = TaskResult(
            err=SubWorkflowError(
                error_code='SUBWORKFLOW_FAILED',
                message=f'Subworkflow {child_workflow_id} failed',
                sub_workflow_id=child_workflow_id,
                sub_workflow_summary=child_summary,
            )
        )
        should_continue = await _handle_workflow_task_failure(
            session, parent_wf_id, parent_task_idx, failure_result
        )
        if not should_continue:
            # PAUSE mode - stop processing
            return

    # 6. Check if parent workflow is PAUSED
    parent_status_check = await session.execute(
        GET_WORKFLOW_STATUS_SQL,
        {'wf_id': parent_wf_id},
    )
    parent_status_row = parent_status_check.fetchone()
    if parent_status_row and parent_status_row[0] == 'PAUSED':
        return  # Don't propagate - parent is paused

    # 7. Process parent dependents
    await _process_dependents(session, parent_wf_id, parent_task_idx, broker)

    # 8. Check parent completion
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
        logger.warning(f'Corrupt success_policy for workflow, treating as no policy')
        return not has_error and failed == 0

    # Build status map by task_index
    result = await session.execute(
        GET_TASK_STATUSES_SQL,
        {'wf_id': workflow_id},
    )

    status_by_index: dict[int, str] = {row[0]: row[1] for row in result.fetchall()}

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
    from horsies.core.models.tasks import LibraryErrorCode, TaskError

    if success_policy_data is None:
        # Default: get first failed task's error
        result = await session.execute(
            GET_FIRST_FAILED_TASK_RESULT_SQL,
            {'wf_id': workflow_id},
        )
        row = result.fetchone()
        if row and row[0]:
            deser = _deser_json(row[0], 'first failed task result')
            if deser is not None:
                parsed_tr = task_result_from_json(deser)
                if not is_err(parsed_tr):
                    task_result = parsed_tr.ok_value
                    if task_result.is_err() and task_result.err:
                        return _ser(dumps_json(task_result.err), 'task error', fallback='null')
        return None

    # Guard: JSONB may come back as string depending on driver
    policy: dict[str, Any] | None
    if isinstance(success_policy_data, str):
        policy = _deser_json(success_policy_data, 'success_policy')
    else:
        policy = success_policy_data
    if not isinstance(policy, dict):
        logger.warning(f'Corrupt success_policy for workflow, treating as no policy')
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
        if row and row[0]:
            deser = _deser_json(row[0], 'first failed required task result')
            if deser is not None:
                parsed_tr = task_result_from_json(deser)
                if not is_err(parsed_tr):
                    task_result = parsed_tr.ok_value
                    if task_result.is_err() and task_result.err:
                        return _ser(dumps_json(task_result.err), 'task error', fallback='null')

    # No required task failed, but no case was satisfied (all SKIPPED?)
    return _ser(
        dumps_json(
            TaskError(
                error_code=LibraryErrorCode.WORKFLOW_SUCCESS_CASE_NOT_MET,
                message='No success case was satisfied',
            )
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

    if wf_row and wf_row[0] is not None:
        # Return explicit output task's result
        output_result = await session.execute(
            GET_OUTPUT_TASK_RESULT_SQL,
            {'wf_id': workflow_id, 'idx': wf_row[0]},
        )
        output_row = output_result.fetchone()
        return output_row[0] if output_row and output_row[0] else 'null'

    # Find terminal tasks (not in any other task's dependencies)
    terminal_results = await session.execute(
        GET_TERMINAL_TASK_RESULTS_SQL,
        {'wf_id': workflow_id},
    )

    # Build dict of terminal results, keyed by node_id
    # This ensures WorkflowHandle.get() returns dict[str, TaskResult], not raw dicts
    results_dict: dict[str, Any] = {}
    for row in terminal_results.fetchall():
        node_id = row[0]
        if not isinstance(node_id, str):
            continue
        unique_key = node_id
        if row[2]:
            # Rehydrate to TaskResult and serialize back (will be parsed on get())
            deser = _deser_json(row[2], 'terminal task result json')
            if deser is not None:
                parsed_tr = task_result_from_json(deser)
                if not is_err(parsed_tr):
                    results_dict[unique_key] = parsed_tr.ok_value
                else:
                    logger.warning(f'task_result_from_json failed (terminal task): {parsed_tr.err_value}')
                    results_dict[unique_key] = None
            else:
                results_dict[unique_key] = None
        else:
            results_dict[unique_key] = None

    # Wrap in TaskResult so WorkflowHandle._get_result() can parse it
    from horsies.core.models.tasks import TaskResult

    wrapped_result: TaskResult[dict[str, Any], Any] = TaskResult(ok=results_dict)
    return _ser(dumps_json(wrapped_result), 'wrapped result', fallback='null')






async def _handle_workflow_task_failure(
    session: AsyncSession,
    workflow_id: str,
    _task_index: int,  # Unused but kept for API consistency
    result: 'TaskResult[Any, TaskError]',
) -> bool:
    """
    Handle a failed workflow task based on on_error policy.

    Returns True if dependency propagation should continue, False to stop (PAUSE mode).

    Note: The failed task's result is already stored. Dependents will receive
    the TaskResult with is_err()=True if they have args_from pointing to this task.
    """
    # Get workflow's on_error policy
    wf_result = await session.execute(
        GET_WORKFLOW_ON_ERROR_SQL,
        {'wf_id': workflow_id},
    )

    wf_row = wf_result.fetchone()
    if wf_row is None:
        return True

    on_error = wf_row[0]

    # Extract TaskError for storage (not the full TaskResult)
    error_payload = _ser(dumps_json(result.err), 'error payload') if result.is_err() and result.err else None

    if on_error == 'fail':
        # Store error but keep status RUNNING until DAG fully resolves
        # This allows allow_failed_deps tasks to run and produce meaningful final result
        # Status will be set to FAILED in check_workflow_completion when all tasks are terminal
        await session.execute(
            SET_WORKFLOW_ERROR_SQL,
            {'wf_id': workflow_id, 'error': error_payload},
        )
        return True  # Continue dependency propagation

    elif on_error == 'pause':
        # Pause workflow for manual intervention - STOP all processing
        await session.execute(
            PAUSE_WORKFLOW_ON_ERROR_SQL,
            {'wf_id': workflow_id, 'error': error_payload},
        )

        # Notify of pause (so clients can react via get())
        await session.execute(
            NOTIFY_WORKFLOW_DONE_SQL,
            {'wf_id': workflow_id},
        )

        return (
            False  # Stop dependency propagation - pending tasks stay pending for resume
        )

    return True  # Default: continue


def _load_workflow_def_from_path(
    module_path: str,
    qualname: str,
) -> 'type[WorkflowDefinition[Any]] | None':
    """
    Load a SubWorkflowNode via module path fallback when registry is missing.
    Returns a SubWorkflowNode instance if found, otherwise None.
    """
    try:
        import importlib

        module = importlib.import_module(module_path)
        obj: Any = module
        for attr in qualname.split('.'):
            obj = getattr(obj, attr, None)
            if obj is None:
                return None

        if isinstance(obj, type) and issubclass(obj, WorkflowDefinition):
            wf_def = cast('type[WorkflowDefinition[Any]]', obj)
            return wf_def
    except Exception as exc:
        logger.error(f'Failed to load subworkflow def {module_path}:{qualname}: {exc}')
        return None

    return None


def _node_from_workflow_def(
    workflow_def: type[WorkflowDefinition[Any]],
    task_index: int,
) -> 'AnyNode | None':
    """
    Reconstruct a node by index from a WorkflowDefinition class.

    Assigns indices and node_ids in definition order to mirror WorkflowSpec.
    """
    nodes = workflow_def.get_workflow_nodes()
    if not nodes:
        return None

    nodes_typed: list[tuple[str, AnyNode]] = nodes
    for idx, (attr_name, node) in enumerate(nodes_typed):
        if node.index is None:
            node.index = idx
        if node.node_id is None:
            node.node_id = attr_name
        if idx == task_index:
            return node
    return None

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
