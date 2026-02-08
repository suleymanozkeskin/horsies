"""Workflow execution engine.

This module handles workflow lifecycle:
- Starting workflows
- Enqueuing tasks based on DAG dependencies
- Handling task completion and dependency resolution
- Workflow completion detection
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.codec.serde import dumps_json, loads_json, task_result_from_json
from horsies.core.logging import get_logger
from horsies.core.models.workflow import (
    WorkflowHandle,
    SubWorkflowNode,
    SubWorkflowSummary,
    WorkflowStatus,
    WorkflowTaskStatus,
    WorkflowDefinition,
    AnyNode,
    WORKFLOW_TASK_TERMINAL_STATES,
)
from horsies.core.errors import WorkflowValidationError, ErrorCode

logger = get_logger('workflow.engine')

_WF_TASK_TERMINAL_VALUES: list[str] = [s.value for s in WORKFLOW_TASK_TERMINAL_STATES]

if TYPE_CHECKING:
    from horsies.core.models.workflow import WorkflowSpec
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.tasks import TaskResult, TaskError


def _as_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    str_items: list[str] = []
    for item in cast(list[object], value):
        if not isinstance(item, str):
            return []
        str_items.append(item)
    return str_items


# -- SQL constants for start_workflow_async --

CHECK_WORKFLOW_EXISTS_SQL = text(
    """SELECT id FROM horsies_workflows WHERE id = :wf_id"""
)

INSERT_WORKFLOW_SQL = text("""
    INSERT INTO horsies_workflows (id, name, status, on_error, output_task_index,
                             success_policy, workflow_def_module, workflow_def_qualname,
                             depth, root_workflow_id,
                             created_at, started_at, updated_at)
    VALUES (:id, :name, 'RUNNING', :on_error, :output_idx,
            :success_policy, :wf_module, :wf_qualname,
            0, :id,
            NOW(), NOW(), NOW())
""")

INSERT_WORKFLOW_TASK_SUBWORKFLOW_SQL = text("""
    INSERT INTO horsies_workflow_tasks
    (id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
     queue_name, priority, dependencies, args_from, workflow_ctx_from,
     allow_failed_deps, join_type, min_success, task_options, status,
     is_subworkflow, sub_workflow_name, sub_workflow_retry_mode,
     sub_workflow_module, sub_workflow_qualname, created_at)
    VALUES (:id, :wf_id, :idx, :node_id, :name, :args, :kwargs, :queue, :priority,
            :deps, :args_from, :ctx_from, :allow_failed, :join_type, :min_success,
            :task_options, :status, TRUE, :sub_wf_name, :sub_wf_retry_mode,
            :sub_wf_module, :sub_wf_qualname, NOW())
""")

INSERT_WORKFLOW_TASK_SQL = text("""
    INSERT INTO horsies_workflow_tasks
    (id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
     queue_name, priority, dependencies, args_from, workflow_ctx_from,
     allow_failed_deps, join_type, min_success, task_options, status,
     is_subworkflow, created_at)
    VALUES (:id, :wf_id, :idx, :node_id, :name, :args, :kwargs, :queue, :priority,
            :deps, :args_from, :ctx_from, :allow_failed, :join_type, :min_success,
            :task_options, :status, FALSE, NOW())
""")


async def start_workflow_async(
    spec: 'WorkflowSpec',
    broker: 'PostgresBroker',
    workflow_id: str | None = None,
) -> WorkflowHandle:
    """
    Start a workflow asynchronously.

    Creates workflow and workflow_tasks records, then enqueues root tasks.

    Args:
        spec: The workflow specification
        broker: PostgreSQL broker for database operations
        workflow_id: Optional custom workflow ID

    Returns:
        WorkflowHandle for tracking and retrieving results

    Notes:
        If workflow_id is provided and already exists, returns existing handle
        (idempotent operation).

    Raises:
        ValueError: If WorkflowSpec was not created via app.workflow() (missing
                    queue/priority resolution).
    """
    # Validate that WorkflowSpec was created via app.workflow()
    # TaskNodes must have resolved queue and priority (SubWorkflowNodes don't have these)
    for node in spec.tasks:
        if isinstance(node, SubWorkflowNode):
            continue  # SubWorkflowNodes don't have queue/priority
        task = node
        if task.queue is None:
            raise WorkflowValidationError(
                message='TaskNode has unresolved queue',
                code=ErrorCode.WORKFLOW_UNRESOLVED_QUEUE,
                notes=[f"TaskNode '{task.name}' has queue=None"],
                help_text='use app.workflow() to create WorkflowSpec with proper validation',
            )
        if task.priority is None:
            raise WorkflowValidationError(
                message='TaskNode has unresolved priority',
                code=ErrorCode.WORKFLOW_UNRESOLVED_PRIORITY,
                notes=[f"TaskNode '{task.name}' has priority=None"],
                help_text='use app.workflow() to create WorkflowSpec with proper validation',
            )

    wf_id = workflow_id or str(uuid.uuid4())

    await broker.ensure_schema_initialized()

    async with broker.session_factory() as session:
        # Check if workflow already exists (idempotent start)
        if workflow_id:
            existing = await session.execute(
                CHECK_WORKFLOW_EXISTS_SQL,
                {'wf_id': wf_id},
            )
            if existing.fetchone():
                logger.warning(
                    f'Workflow {wf_id} already exists, returning existing handle'
                )
                return WorkflowHandle(workflow_id=wf_id, broker=broker)

        # 1. Insert workflow record
        output_index = spec.output.index if spec.output else None

        # Serialize success_policy to index-based format
        success_policy_json: dict[str, Any] | None = None
        if spec.success_policy is not None:
            success_policy_json = {
                'cases': [
                    {
                        'required_indices': [
                            t.index for t in case.required if t.index is not None
                        ]
                    }
                    for case in spec.success_policy.cases
                ],
            }
            if spec.success_policy.optional:
                success_policy_json['optional_indices'] = [
                    t.index for t in spec.success_policy.optional if t.index is not None
                ]

        await session.execute(
            INSERT_WORKFLOW_SQL,
            {
                'id': wf_id,
                'name': spec.name,
                'on_error': spec.on_error.value,
                'output_idx': output_index,
                'success_policy': dumps_json(success_policy_json)
                if success_policy_json
                else None,
                'wf_module': spec.workflow_def_module,
                'wf_qualname': spec.workflow_def_qualname,
            },
        )

        # 2. Insert all workflow_tasks
        for node in spec.tasks:
            dep_indices = [d.index for d in node.waits_for if d.index is not None]
            args_from_indices = {
                k: v.index for k, v in node.args_from.items() if v.index is not None
            }
            ctx_from_ids = (
                [n.node_id for n in node.workflow_ctx_from if n.node_id is not None]
                if node.workflow_ctx_from
                else None
            )

            wt_id = str(uuid.uuid4())

            if isinstance(node, SubWorkflowNode):
                # SubWorkflowNode: no fn, queue, priority, good_until
                await session.execute(
                    INSERT_WORKFLOW_TASK_SUBWORKFLOW_SQL,
                    {
                        'id': wt_id,
                        'wf_id': wf_id,
                        'idx': node.index,
                        'node_id': node.node_id,
                        'name': node.name,
                        'args': dumps_json(node.args),
                        'kwargs': dumps_json(node.kwargs),
                        'queue': 'default',  # SubWorkflowNode doesn't have queue
                        'priority': 100,  # SubWorkflowNode doesn't have priority
                        'deps': dep_indices,
                        'args_from': dumps_json(args_from_indices)
                        if args_from_indices
                        else None,
                        'ctx_from': ctx_from_ids,
                        'allow_failed': node.allow_failed_deps,
                        'join_type': node.join,
                        'min_success': node.min_success,
                        'task_options': None,
                        'status': 'PENDING' if dep_indices else 'READY',
                        'sub_wf_name': node.workflow_def.name,
                        'sub_wf_retry_mode': node.retry_mode.value,
                        'sub_wf_module': node.workflow_def.__module__,
                        'sub_wf_qualname': node.workflow_def.__qualname__,
                    },
                )
            else:
                # TaskNode: has fn, queue, priority, good_until
                task = node

                # Get task_options_json from the task function (set by @task decorator)
                # and merge in TaskNode.good_until if set
                task_options_json: str | None = getattr(
                    task.fn, 'task_options_json', None
                )
                if task.good_until is not None:
                    # Merge good_until from TaskNode into task_options
                    base_options: dict[str, Any] = {}
                    if task_options_json:
                        parsed = loads_json(task_options_json)
                        if isinstance(parsed, dict):
                            base_options = parsed
                    base_options['good_until'] = task.good_until.isoformat()
                    task_options_json = dumps_json(base_options)

                await session.execute(
                    INSERT_WORKFLOW_TASK_SQL,
                    {
                        'id': wt_id,
                        'wf_id': wf_id,
                        'idx': task.index,
                        'node_id': task.node_id,
                        'name': task.name,
                        'args': dumps_json(task.args),
                        'kwargs': dumps_json(task.kwargs),
                        # Queue: use override, else task's declared queue, else "default"
                        'queue': task.queue
                        or getattr(task.fn, 'task_queue_name', None)
                        or 'default',
                        # Priority: use override, else default
                        'priority': task.priority if task.priority is not None else 100,
                        'deps': dep_indices,
                        'args_from': dumps_json(args_from_indices)
                        if args_from_indices
                        else None,
                        'ctx_from': ctx_from_ids,
                        'allow_failed': task.allow_failed_deps,
                        'join_type': task.join,
                        'min_success': task.min_success,
                        'task_options': task_options_json,
                        'status': 'PENDING' if dep_indices else 'READY',
                    },
                )

        # 3. Enqueue root tasks (no dependencies)
        root_nodes = [t for t in spec.tasks if not t.waits_for]
        for root_node in root_nodes:
            if root_node.index is not None:
                if isinstance(root_node, SubWorkflowNode):
                    # Start child workflow
                    await _enqueue_subworkflow_task(
                        session, broker, wf_id, root_node.index, {}, 0, wf_id
                    )
                else:
                    # Enqueue regular task
                    await _enqueue_workflow_task(session, wf_id, root_node.index, {})

        await session.commit()

    return WorkflowHandle(workflow_id=wf_id, broker=broker)


def start_workflow(
    spec: 'WorkflowSpec',
    broker: 'PostgresBroker',
    workflow_id: str | None = None,
) -> WorkflowHandle:
    """
    Start a workflow synchronously.

    Sync wrapper around start_workflow_async().

    Args:
        spec: The workflow specification
        broker: PostgreSQL broker for database operations
        workflow_id: Optional custom workflow ID

    Returns:
        WorkflowHandle for tracking and retrieving results
    """
    from horsies.core.utils.loop_runner import LoopRunner

    runner = LoopRunner()
    try:
        return runner.call(start_workflow_async, spec, broker, workflow_id)
    finally:
        runner.stop()


# -- SQL constants for pause_workflow --

PAUSE_WORKFLOW_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'PAUSED', updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING'
    RETURNING id
""")

NOTIFY_WORKFLOW_DONE_SQL = text("""SELECT pg_notify('workflow_done', :wf_id)""")


async def pause_workflow(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> bool:
    """
    Pause a running workflow.

    Transitions workflow from RUNNING to PAUSED state. Already-running tasks
    will continue to completion, but:
    - No new PENDING tasks will become READY
    - No READY tasks will be enqueued
    - Workflow completion check is skipped while PAUSED

    Also cascades pause to all running child workflows (iteratively, not recursively).

    Use resume_workflow() to continue execution.

    Args:
        broker: PostgreSQL broker for database operations
        workflow_id: The workflow ID to pause

    Returns:
        True if workflow was paused, False if not RUNNING (no-op)
    """
    async with broker.session_factory() as session:
        result = await session.execute(
            PAUSE_WORKFLOW_SQL,
            {'wf_id': workflow_id},
        )
        row = result.fetchone()
        if row is None:
            return False

        # Cascade pause to running child workflows (iterative BFS)
        await _cascade_pause_to_children(session, workflow_id)

        # Notify clients of pause (so get() returns immediately with WORKFLOW_PAUSED)
        await session.execute(
            NOTIFY_WORKFLOW_DONE_SQL,
            {'wf_id': workflow_id},
        )

        await session.commit()
        return True


# -- SQL constants for _cascade_pause_to_children --

GET_RUNNING_CHILD_WORKFLOWS_SQL = text("""
    SELECT id FROM horsies_workflows
    WHERE parent_workflow_id = :wf_id AND status = 'RUNNING'
""")

PAUSE_CHILD_WORKFLOW_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'PAUSED', updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING'
""")


async def _cascade_pause_to_children(
    session: AsyncSession,
    workflow_id: str,
) -> None:
    """
    Iteratively pause all running child workflows using BFS.
    Avoids deep recursion for deeply nested workflows.
    """
    queue = [workflow_id]

    while queue:
        current_id = queue.pop(0)

        # Find running child workflows
        children = await session.execute(
            GET_RUNNING_CHILD_WORKFLOWS_SQL,
            {'wf_id': current_id},
        )

        for child_row in children.fetchall():
            child_id = child_row[0]

            # Pause child
            await session.execute(
                PAUSE_CHILD_WORKFLOW_SQL,
                {'wf_id': child_id},
            )

            # Notify of child pause
            await session.execute(
                NOTIFY_WORKFLOW_DONE_SQL,
                {'wf_id': child_id},
            )

            # Add to queue to pause its children
            queue.append(child_id)


def pause_workflow_sync(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> bool:
    """
    Pause a running workflow synchronously.

    Sync wrapper around pause_workflow().

    Args:
        broker: PostgreSQL broker for database operations
        workflow_id: The workflow ID to pause

    Returns:
        True if workflow was paused, False if not RUNNING (no-op)
    """
    from horsies.core.utils.loop_runner import LoopRunner

    runner = LoopRunner()
    try:
        return runner.call(pause_workflow, broker, workflow_id)
    finally:
        runner.stop()


# -- SQL constants for resume_workflow --

RESUME_WORKFLOW_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'RUNNING', updated_at = NOW()
    WHERE id = :wf_id AND status = 'PAUSED'
    RETURNING id, depth, root_workflow_id
""")

GET_PENDING_WORKFLOW_TASKS_SQL = text("""
    SELECT task_index FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND status = 'PENDING'
""")

GET_READY_WORKFLOW_TASKS_SQL = text("""
    SELECT task_index, dependencies, is_subworkflow FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND status = 'READY'
""")


async def resume_workflow(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> bool:
    """
    Resume a paused workflow.

    Re-evaluates all PENDING tasks (marks READY if deps are terminal) and
    enqueues all READY tasks. Only works if workflow is currently PAUSED.

    Also cascades resume to all paused child workflows (iteratively, not recursively).

    Args:
        broker: PostgreSQL broker for database operations
        workflow_id: The workflow ID to resume

    Returns:
        True if workflow was resumed, False if not PAUSED (no-op)
    """
    async with broker.session_factory() as session:
        # 1. Transition PAUSED → RUNNING (only if currently PAUSED)
        result = await session.execute(
            RESUME_WORKFLOW_SQL,
            {'wf_id': workflow_id},
        )
        row = result.fetchone()
        if row is None:
            # Not PAUSED, no-op
            return False

        depth = row[1] or 0
        root_wf_id = row[2] or workflow_id

        # 2. Find all PENDING tasks and try to make them READY
        pending_result = await session.execute(
            GET_PENDING_WORKFLOW_TASKS_SQL,
            {'wf_id': workflow_id},
        )
        pending_indices = [r[0] for r in pending_result.fetchall()]

        for task_index in pending_indices:
            await _try_make_ready_and_enqueue(
                session, broker, workflow_id, task_index, depth, root_wf_id
            )

        # 3. Find all READY tasks and enqueue them
        # (These may be tasks that were READY at pause time, or tasks that
        # couldn't be enqueued during step 2 due to failed deps check)
        ready_result = await session.execute(
            GET_READY_WORKFLOW_TASKS_SQL,
            {'wf_id': workflow_id},
        )
        ready_tasks = ready_result.fetchall()

        for task_index, dependencies, is_subworkflow in ready_tasks:
            # Fetch dependency results for this task
            dep_indices: list[int] = (
                cast(list[int], dependencies) if isinstance(dependencies, list) else []
            )
            dep_results = await _get_dependency_results(
                session, workflow_id, dep_indices
            )

            if is_subworkflow:
                await _enqueue_subworkflow_task(
                    session,
                    broker,
                    workflow_id,
                    task_index,
                    dep_results,
                    depth,
                    root_wf_id,
                )
            else:
                await _enqueue_workflow_task(
                    session, workflow_id, task_index, dep_results
                )

        # 4. Cascade resume to paused child workflows
        await _cascade_resume_to_children(session, broker, workflow_id)

        # 5. Re-check completion after resume processing.
        # Resume may transition pending tasks directly to SKIPPED/terminal without
        # any subsequent task completion callback to trigger finalization.
        await _check_workflow_completion(session, workflow_id, broker)

        await session.commit()
        return True


# -- SQL constants for _cascade_resume_to_children --

GET_PAUSED_CHILD_WORKFLOWS_SQL = text("""
    SELECT id, depth, root_workflow_id FROM horsies_workflows
    WHERE parent_workflow_id = :wf_id AND status = 'PAUSED'
""")

RESUME_CHILD_WORKFLOW_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'RUNNING', updated_at = NOW()
    WHERE id = :wf_id AND status = 'PAUSED'
""")


async def _cascade_resume_to_children(
    session: AsyncSession,
    broker: 'PostgresBroker',
    workflow_id: str,
) -> None:
    """
    Iteratively resume all paused child workflows using BFS.
    Avoids deep recursion for deeply nested workflows.
    """
    queue = [workflow_id]

    while queue:
        current_id = queue.pop(0)

        # Find paused child workflows
        children = await session.execute(
            GET_PAUSED_CHILD_WORKFLOWS_SQL,
            {'wf_id': current_id},
        )

        for child_row in children.fetchall():
            child_id = child_row[0]
            child_depth = child_row[1] or 0
            child_root = child_row[2] or child_id

            # Resume child
            await session.execute(
                RESUME_CHILD_WORKFLOW_SQL,
                {'wf_id': child_id},
            )

            # Re-evaluate and enqueue child's PENDING/READY tasks
            child_pending = await session.execute(
                GET_PENDING_WORKFLOW_TASKS_SQL,
                {'wf_id': child_id},
            )
            for pending_row in child_pending.fetchall():
                await _try_make_ready_and_enqueue(
                    session, broker, child_id, pending_row[0], child_depth, child_root
                )

            child_ready = await session.execute(
                GET_READY_WORKFLOW_TASKS_SQL,
                {'wf_id': child_id},
            )
            for ready_row in child_ready.fetchall():
                task_idx = ready_row[0]
                deps = ready_row[1]
                is_sub = ready_row[2]
                dep_indices: list[int] = (
                    cast(list[int], deps) if isinstance(deps, list) else []
                )
                dep_res = await _get_dependency_results(session, child_id, dep_indices)

                if is_sub:
                    await _enqueue_subworkflow_task(
                        session,
                        broker,
                        child_id,
                        task_idx,
                        dep_res,
                        child_depth,
                        child_root,
                    )
                else:
                    await _enqueue_workflow_task(session, child_id, task_idx, dep_res)

            # Add to queue to resume its children
            queue.append(child_id)


def resume_workflow_sync(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> bool:
    """
    Resume a paused workflow synchronously.

    Sync wrapper around resume_workflow().

    Args:
        broker: PostgreSQL broker for database operations
        workflow_id: The workflow ID to resume

    Returns:
        True if workflow was resumed, False if not PAUSED (no-op)
    """
    from horsies.core.utils.loop_runner import LoopRunner

    runner = LoopRunner()
    try:
        return runner.call(resume_workflow, broker, workflow_id)
    finally:
        runner.stop()


# -- SQL constants for _enqueue_workflow_task --

ENQUEUE_WORKFLOW_TASK_SQL = text("""
    UPDATE horsies_workflow_tasks wt
    SET status = 'ENQUEUED', started_at = NOW()
    FROM horsies_workflows w
    WHERE wt.workflow_id = :wf_id
      AND wt.task_index = :idx
      AND wt.status = 'READY'
      AND w.id = wt.workflow_id
      AND w.status = 'RUNNING'
    RETURNING wt.id, wt.task_name, wt.task_args, wt.task_kwargs, wt.queue_name, wt.priority,
              wt.args_from, wt.workflow_ctx_from, wt.task_options
""")

INSERT_TASK_FOR_WORKFLOW_SQL = text("""
    INSERT INTO horsies_tasks (id, task_name, queue_name, priority, args, kwargs, status,
                       sent_at, created_at, updated_at, claimed, retry_count, max_retries,
                       task_options, good_until)
    VALUES (:id, :name, :queue, :priority, :args, :kwargs, 'PENDING',
            NOW(), NOW(), NOW(), FALSE, 0, :max_retries, :task_options, :good_until)
""")

LINK_WORKFLOW_TASK_SQL = text("""
    UPDATE horsies_workflow_tasks SET task_id = :tid WHERE workflow_id = :wf_id AND task_index = :idx
""")


async def _enqueue_workflow_task(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
    all_dep_results: dict[int, 'TaskResult[Any, TaskError]'],
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
        try:
            options_data = loads_json(task_options_str)
            if isinstance(options_data, dict):
                retry_policy = options_data.get('retry_policy')
                if isinstance(retry_policy, dict):
                    max_retries = retry_policy.get('max_retries', 3)
                # Extract good_until if present
                good_until_raw = options_data.get('good_until')
                if good_until_raw is not None:
                    good_until_str = str(good_until_raw)
        except Exception:
            pass

    # Start with static kwargs
    raw_kwargs = loads_json(row[3])
    kwargs: dict[str, Any] = raw_kwargs if isinstance(raw_kwargs, dict) else {}

    # Inject args_from: map kwarg_name -> TaskResult from dependency
    if row[6]:  # args_from
        args_from_raw = row[6]
        # args_from is stored as JSONB, may come back as dict directly
        if isinstance(args_from_raw, str):
            args_from_map = loads_json(args_from_raw)
        else:
            args_from_map = args_from_raw

        if isinstance(args_from_map, dict):
            # Cast to proper type - args_from stores {kwarg_name: task_index}
            args_from_typed = cast(dict[str, int], args_from_map)
            for kwarg_name, dep_index in args_from_typed.items():
                dep_result = all_dep_results.get(dep_index)
                if dep_result is not None:
                    # Serialize TaskResult for transport
                    kwargs[kwarg_name] = {
                        '__horsies_taskresult__': True,
                        'data': dumps_json(dep_result),
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

    # Create actual task in tasks table
    task_id = str(uuid.uuid4())
    await session.execute(
        INSERT_TASK_FOR_WORKFLOW_SQL,
        {
            'id': task_id,
            'name': row[1],  # task_name
            'queue': row[4],  # queue_name
            'priority': row[5],  # priority
            'args': row[2],  # task_args (already JSON string)
            'kwargs': dumps_json(kwargs),
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


# -- SQL constants for _enqueue_subworkflow_task --

ENQUEUE_SUBWORKFLOW_TASK_SQL = text("""
    UPDATE horsies_workflow_tasks wt
    SET status = 'ENQUEUED', started_at = NOW()
    FROM horsies_workflows w
    WHERE wt.workflow_id = :wf_id
      AND wt.task_index = :idx
      AND wt.status = 'READY'
      AND wt.is_subworkflow = TRUE
      AND w.id = wt.workflow_id
      AND w.status = 'RUNNING'
    RETURNING wt.id, wt.sub_workflow_name, wt.task_args, wt.task_kwargs,
              wt.args_from, wt.node_id, wt.sub_workflow_module,
              wt.sub_workflow_qualname, wt.sub_workflow_retry_mode
""")

GET_WORKFLOW_NAME_SQL = text("""SELECT name FROM horsies_workflows WHERE id = :wf_id""")

MARK_WORKFLOW_TASK_FAILED_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'FAILED', result = :result, completed_at = NOW()
    WHERE workflow_id = :wf_id AND task_index = :idx
""")

INSERT_CHILD_WORKFLOW_SQL = text("""
    INSERT INTO horsies_workflows
    (id, name, status, on_error, output_task_index, success_policy,
     workflow_def_module, workflow_def_qualname,
     parent_workflow_id, parent_task_index, depth, root_workflow_id,
     created_at, started_at, updated_at)
    VALUES (:id, :name, 'RUNNING', :on_error, :output_idx, :success_policy,
            :wf_module, :wf_qualname,
            :parent_wf_id, :parent_idx, :depth, :root_wf_id,
            NOW(), NOW(), NOW())
""")

LINK_SUB_WORKFLOW_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET sub_workflow_id = :child_id, status = 'RUNNING'
    WHERE workflow_id = :wf_id AND task_index = :idx
""")


async def _enqueue_subworkflow_task(
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
        logger.error(f'Workflow {workflow_id} not found')
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
                'result': dumps_json(TaskResult(err=error)),
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
            await _check_workflow_completion(session, workflow_id, broker)

        return None

    # 3. Parse static kwargs and merge args_from
    raw_args = loads_json(task_args_json) if task_args_json else []
    task_args: tuple[Any, ...] = ()
    if isinstance(raw_args, list):
        task_args = tuple(raw_args)
    raw_kwargs = loads_json(task_kwargs_json) if task_kwargs_json else {}
    kwargs: dict[str, Any] = raw_kwargs if isinstance(raw_kwargs, dict) else {}

    if args_from_raw:
        # args_from is stored as JSONB, may come back as dict directly
        if isinstance(args_from_raw, str):
            args_from_map = loads_json(args_from_raw)
        else:
            args_from_map = args_from_raw

        if isinstance(args_from_map, dict):
            args_from_typed = cast(dict[str, int], args_from_map)
            for kwarg_name, dep_index in args_from_typed.items():
                dep_result = all_dep_results.get(dep_index)
                if dep_result is not None:
                    # Serialize TaskResult for transport
                    kwargs[kwarg_name] = {
                        '__horsies_taskresult__': True,
                        'data': dumps_json(dep_result),
                    }

    # 4. Build child WorkflowSpec (parameterized)
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
    child_spec = workflow_def.build_with(broker.app, *task_args, **kwargs)

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

    await session.execute(
        INSERT_CHILD_WORKFLOW_SQL,
        {
            'id': child_id,
            'name': child_spec.name,
            'on_error': child_spec.on_error.value,
            'output_idx': child_output_index,
            'success_policy': dumps_json(child_success_policy_json)
            if child_success_policy_json
            else None,
            'wf_module': child_spec.workflow_def_module,
            'wf_qualname': child_spec.workflow_def_qualname,
            'parent_wf_id': workflow_id,
            'parent_idx': task_index,
            'depth': parent_depth + 1,
            'root_wf_id': root_workflow_id,
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
            await session.execute(
                INSERT_WORKFLOW_TASK_SUBWORKFLOW_SQL,
                {
                    'id': child_wt_id,
                    'wf_id': child_id,
                    'idx': child_sub.index,
                    'node_id': child_sub.node_id,
                    'name': child_sub.name,
                    'args': dumps_json(child_sub.args),
                    'kwargs': dumps_json(child_sub.kwargs),
                    'queue': 'default',
                    'priority': 100,
                    'deps': child_dep_indices,
                    'args_from': dumps_json(child_args_from_indices)
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
            child_task_options_json: str | None = getattr(
                child_task.fn, 'task_options_json', None
            )
            if child_task.good_until is not None:
                child_base_options: dict[str, Any] = {}
                if child_task_options_json:
                    parsed = loads_json(child_task_options_json)
                    if isinstance(parsed, dict):
                        child_base_options = parsed
                child_base_options['good_until'] = child_task.good_until.isoformat()
                child_task_options_json = dumps_json(child_base_options)

            await session.execute(
                INSERT_WORKFLOW_TASK_SQL,
                {
                    'id': child_wt_id,
                    'wf_id': child_id,
                    'idx': child_task.index,
                    'node_id': child_task.node_id,
                    'name': child_task.name,
                    'args': dumps_json(child_task.args),
                    'kwargs': dumps_json(child_task.kwargs),
                    'queue': child_task.queue
                    or getattr(child_task.fn, 'task_queue_name', None)
                    or 'default',
                    'priority': child_task.priority
                    if child_task.priority is not None
                    else 100,
                    'deps': child_dep_indices,
                    'args_from': dumps_json(child_args_from_indices)
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
                await _enqueue_subworkflow_task(
                    session,
                    broker,
                    child_id,
                    child_root.index,
                    {},
                    parent_depth + 1,
                    root_workflow_id,
                )
            else:
                await _enqueue_workflow_task(session, child_id, child_root.index, {})

    logger.info(f'Started child workflow {child_id} for {workflow_name}:{task_index}')
    return child_id


# -- SQL constants for _build_workflow_context_data --

GET_SUBWORKFLOW_SUMMARIES_SQL = text("""
    SELECT node_id, sub_workflow_summary
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND node_id = ANY(:node_ids)
      AND is_subworkflow = TRUE
      AND sub_workflow_summary IS NOT NULL
""")


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
    dep_results = await _get_dependency_results_with_names(
        session, workflow_id, ctx_from_ids
    )

    results_by_id: dict[str, str] = {}
    for node_id, result in dep_results.by_id.items():
        results_by_id[node_id] = dumps_json(result)

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


# -- SQL constants for on_workflow_task_complete --

GET_WORKFLOW_TASK_BY_TASK_ID_SQL = text("""
    SELECT workflow_id, task_index
    FROM horsies_workflow_tasks
    WHERE task_id = :tid
""")

UPDATE_WORKFLOW_TASK_RESULT_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = :status, result = :result, completed_at = NOW()
    WHERE workflow_id = :wf_id AND task_index = :idx
""")

GET_WORKFLOW_STATUS_SQL = text(
    """SELECT status FROM horsies_workflows WHERE id = :wf_id"""
)


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

    # 2. Update workflow_task status and store result
    new_status = 'COMPLETED' if result.is_ok() else 'FAILED'
    await session.execute(
        UPDATE_WORKFLOW_TASK_RESULT_SQL,
        {
            'status': new_status,
            'result': dumps_json(result),
            'wf_id': workflow_id,
            'idx': task_index,
        },
    )

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
    await _check_workflow_completion(session, workflow_id, broker)


# -- SQL constants for _process_dependents --

GET_DEPENDENT_TASKS_SQL = text("""
    SELECT task_index FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND :completed_idx = ANY(dependencies)
      AND status = 'PENDING'
""")

GET_WORKFLOW_DEPTH_SQL = text(
    """SELECT depth, root_workflow_id FROM horsies_workflows WHERE id = :wf_id"""
)


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
        await _try_make_ready_and_enqueue(
            session, broker, workflow_id, row[0], depth, root_wf_id
        )


# -- SQL constants for _try_make_ready_and_enqueue --

GET_TASK_CONFIG_SQL = text("""
    SELECT wt.status, wt.dependencies, wt.allow_failed_deps,
           wt.join_type, wt.min_success, wt.workflow_ctx_from,
           wt.is_subworkflow,
           w.status as wf_status
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.workflow_id = :wf_id AND wt.task_index = :idx
""")

GET_DEP_STATUS_COUNTS_SQL = text("""
    SELECT status, COUNT(*) as cnt
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND task_index = ANY(:deps)
    GROUP BY status
""")

SKIP_WORKFLOW_TASK_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED'
    WHERE workflow_id = :wf_id AND task_index = :idx AND status = 'PENDING'
""")

COUNT_CTX_TERMINAL_DEPS_SQL = text("""
    SELECT COUNT(*) as cnt
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND node_id = ANY(:node_ids)
      AND status = ANY(:wf_task_terminal_states)
""")

MARK_TASK_READY_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'READY'
    WHERE workflow_id = :wf_id AND task_index = :idx AND status = 'PENDING'
    RETURNING task_index
""")

SKIP_READY_WORKFLOW_TASK_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED'
    WHERE workflow_id = :wf_id AND task_index = :idx AND status = 'READY'
""")


async def _try_make_ready_and_enqueue(
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
    ctx_from_ids = _as_str_list(raw_ctx_from)
    if ctx_from_ids:
        ctx_terminal_result = await session.execute(
            COUNT_CTX_TERMINAL_DEPS_SQL,
            {
                'wf_id': workflow_id,
                'node_ids': ctx_from_ids,
                'wf_task_terminal_states': _WF_TASK_TERMINAL_VALUES,
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
        should_skip_condition = await _evaluate_conditions(
            session, workflow_id, workflow_name, task_index, dependencies
        )
        if should_skip_condition:
            await session.execute(
                SKIP_READY_WORKFLOW_TASK_SQL,
                {'wf_id': workflow_id, 'idx': task_index},
            )
            await _process_dependents(session, workflow_id, task_index, broker)
            return

    # 7. Fetch dependency results and enqueue
    dep_results = await _get_dependency_results(session, workflow_id, dependencies)

    if is_subworkflow and broker is not None:
        # SubWorkflowNode: start child workflow
        actual_root = root_workflow_id or workflow_id
        await _enqueue_subworkflow_task(
            session, broker, workflow_id, task_index, dep_results, depth, actual_root
        )
    else:
        # Regular TaskNode: enqueue as task
        await _enqueue_workflow_task(session, workflow_id, task_index, dep_results)


# -- SQL constants for _evaluate_conditions --

GET_WORKFLOW_DEF_PATH_SQL = text("""
    SELECT workflow_def_module, workflow_def_qualname
    FROM horsies_workflows
    WHERE id = :wf_id
""")


async def _evaluate_conditions(
    session: AsyncSession,
    workflow_id: str,
    workflow_name: str,
    task_index: int,
    dependencies: list[int],
) -> bool:
    """
    Evaluate run_when/skip_when conditions for a task.

    Returns True if task should be SKIPPED, False to proceed with enqueue.

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
            return False

    node_any: AnyNode = node

    if node_any.skip_when is None and node_any.run_when is None:
        # No conditions to evaluate
        return False

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
    dep_results = await _get_dependency_results_with_names(
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
                try:
                    parsed = loads_json(summary_json)
                    if isinstance(parsed, dict):
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

    # Evaluate conditions
    try:
        if node_any.skip_when is not None and node_any.skip_when(ctx):
            return True  # Skip
        if node_any.run_when is not None and not node_any.run_when(ctx):
            return True  # Skip (run_when returned False)
    except Exception as e:
        # Condition evaluation failed - log and proceed with skip
        logger.warning(f'Condition evaluation failed for task {task_name}: {e}')
        return True  # Skip on error (safer default)

    return False  # Proceed with enqueue


class DependencyResults:
    """Container for dependency results with index, name, and node_id mappings."""

    def __init__(self) -> None:
        self.by_index: dict[int, 'TaskResult[Any, TaskError]'] = {}
        self.by_name: dict[str, 'TaskResult[Any, TaskError]'] = {}
        self.by_id: dict[str, 'TaskResult[Any, TaskError]'] = {}


# -- SQL constants for _get_dependency_results --

GET_DEPENDENCY_RESULTS_SQL = text("""
    SELECT task_index, status, result
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND task_index = ANY(:indices)
      AND status = ANY(:wf_task_terminal_states)
""")


async def _get_dependency_results(
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
            'wf_task_terminal_states': _WF_TASK_TERMINAL_VALUES,
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
            results[task_index] = task_result_from_json(loads_json(stored_result))

    return results


# -- SQL constants for _get_dependency_results_with_names --

GET_DEPENDENCY_RESULTS_WITH_NAMES_SQL = text("""
    SELECT task_index, task_name, node_id, status, result
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND node_id = ANY(:node_ids)
      AND status = ANY(:wf_task_terminal_states)
""")


async def _get_dependency_results_with_names(
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
            'wf_task_terminal_states': _WF_TASK_TERMINAL_VALUES,
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
            task_result = task_result_from_json(loads_json(stored_result))
        else:
            continue  # No result to include

        dep_results.by_index[task_index] = task_result
        if node_id is not None:
            dep_results.by_id[node_id] = task_result
        # Use unique key to avoid collisions when same task appears multiple times
        unique_key = f'{task_name}#{task_index}'
        dep_results.by_name[unique_key] = task_result

    return dep_results


# -- SQL constants for _check_workflow_completion --

GET_WORKFLOW_COMPLETION_STATUS_SQL = text("""
    SELECT
        w.status,
        w.completed_at,
        w.error,
        w.success_policy,
        w.name,
        COUNT(*) FILTER (WHERE NOT (wt.status = ANY(:wf_task_terminal_states))) as incomplete,
        COUNT(*) FILTER (WHERE wt.status = 'FAILED') as failed,
        COUNT(*) FILTER (WHERE wt.status = 'COMPLETED') as completed,
        COUNT(*) as total
    FROM horsies_workflows w
    LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
    WHERE w.id = :wf_id
    GROUP BY w.id, w.status, w.completed_at, w.error, w.success_policy, w.name
""")

MARK_WORKFLOW_COMPLETED_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'COMPLETED', result = :result, completed_at = NOW(), updated_at = NOW()
    WHERE id = :wf_id AND completed_at IS NULL
""")

MARK_WORKFLOW_FAILED_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'FAILED', result = :result,
        error = COALESCE(:error, error),
        completed_at = NOW(), updated_at = NOW()
    WHERE id = :wf_id AND completed_at IS NULL
""")

GET_PARENT_WORKFLOW_INFO_SQL = text("""
    SELECT parent_workflow_id, parent_task_index
    FROM horsies_workflows WHERE id = :wf_id
""")


async def _check_workflow_completion(
    session: AsyncSession,
    workflow_id: str,
    broker: 'PostgresBroker | None' = None,
) -> None:
    """
    Check if all workflow tasks are complete and update workflow status.
    Handles cases where workflow may already be FAILED but DAG is still resolving.

    If success_policy is set, evaluates success cases instead of "any failure → FAILED".
    """
    # Get current workflow status, error, success_policy, and task counts
    result = await session.execute(
        GET_WORKFLOW_COMPLETION_STATUS_SQL,
        {
            'wf_id': workflow_id,
            'wf_task_terminal_states': _WF_TASK_TERMINAL_VALUES,
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
    final_result = await _get_workflow_final_result(session, workflow_id)

    # Determine final status using success policy or default behavior
    workflow_succeeded = await _evaluate_workflow_success(
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
        error_json: str | None = await _get_workflow_failure_error(
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
        await _on_subworkflow_complete(session, workflow_id, broker)


# -- SQL constants for _on_subworkflow_complete --

GET_CHILD_WORKFLOW_INFO_SQL = text("""
    SELECT w.status, w.result, w.error, w.parent_workflow_id, w.parent_task_index,
           (SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = w.id) as total,
           (SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = w.id AND status = 'COMPLETED') as completed,
           (SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = w.id AND status = 'FAILED') as failed,
           (SELECT COUNT(*) FROM horsies_workflow_tasks WHERE workflow_id = w.id AND status = 'SKIPPED') as skipped
    FROM horsies_workflows w
    WHERE w.id = :child_id
""")

UPDATE_PARENT_NODE_RESULT_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = :status, result = :result, sub_workflow_summary = :summary, completed_at = NOW()
    WHERE workflow_id = :wf_id AND task_index = :idx
""")


async def _on_subworkflow_complete(
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
        try:
            parsed_result = task_result_from_json(loads_json(child_result_json))
            if parsed_result.is_ok():
                child_output = parsed_result.ok
        except Exception:
            pass

    error_summary: str | None = None
    if child_error:
        try:
            error_data = loads_json(child_error)
            if isinstance(error_data, dict):
                msg = error_data.get('message')
                if isinstance(msg, str):
                    error_summary = msg
        except Exception:
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
        parent_node_result = dumps_json(TaskResult(err=error))

    # 4. Update parent node
    await session.execute(
        UPDATE_PARENT_NODE_RESULT_SQL,
        {
            'status': parent_node_status,
            'result': parent_node_result,
            'summary': dumps_json(child_summary),
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
    await _check_workflow_completion(session, parent_wf_id, broker)


# -- SQL constants for _evaluate_workflow_success --

GET_TASK_STATUSES_SQL = text("""
    SELECT task_index, status
    FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
""")


async def _evaluate_workflow_success(
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
    policy: dict[str, Any]
    if isinstance(success_policy_data, str):
        policy = loads_json(success_policy_data)  # type: ignore[assignment]
    else:
        policy = success_policy_data

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


# -- SQL constants for _get_workflow_failure_error --

GET_FIRST_FAILED_TASK_RESULT_SQL = text("""
    SELECT result FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND status = 'FAILED'
    ORDER BY task_index ASC LIMIT 1
""")

GET_FIRST_FAILED_REQUIRED_TASK_SQL = text("""
    SELECT result FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id
      AND status = 'FAILED'
      AND task_index = ANY(:required)
    ORDER BY task_index ASC LIMIT 1
""")


async def _get_workflow_failure_error(
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
            task_result = task_result_from_json(loads_json(row[0]))
            if task_result.is_err() and task_result.err:
                return dumps_json(task_result.err)
        return None

    # Guard: JSONB may come back as string depending on driver
    policy: dict[str, Any]
    if isinstance(success_policy_data, str):
        policy = loads_json(success_policy_data)  # type: ignore[assignment]
    else:
        policy = success_policy_data

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
            task_result = task_result_from_json(loads_json(row[0]))
            if task_result.is_err() and task_result.err:
                return dumps_json(task_result.err)

    # No required task failed, but no case was satisfied (all SKIPPED?)
    return dumps_json(
        TaskError(
            error_code=LibraryErrorCode.WORKFLOW_SUCCESS_CASE_NOT_MET,
            message='No success case was satisfied',
        )
    )


# -- SQL constants for _get_workflow_final_result --

GET_WORKFLOW_OUTPUT_INDEX_SQL = text(
    """SELECT output_task_index FROM horsies_workflows WHERE id = :wf_id"""
)

GET_OUTPUT_TASK_RESULT_SQL = text("""
    SELECT result FROM horsies_workflow_tasks
    WHERE workflow_id = :wf_id AND task_index = :idx
""")

GET_TERMINAL_TASK_RESULTS_SQL = text("""
    SELECT wt.node_id, wt.task_index, wt.result
    FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = :wf_id
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks other
          WHERE other.workflow_id = wt.workflow_id
            AND wt.task_index = ANY(other.dependencies)
      )
""")


async def _get_workflow_final_result(
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
        return output_row[0] if output_row and output_row[0] else dumps_json(None)

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
            results_dict[unique_key] = task_result_from_json(loads_json(row[2]))
        else:
            results_dict[unique_key] = None

    # Wrap in TaskResult so WorkflowHandle._get_result() can parse it
    from horsies.core.models.tasks import TaskResult

    wrapped_result: TaskResult[dict[str, Any], Any] = TaskResult(ok=results_dict)
    return dumps_json(wrapped_result)


# -- SQL constants for _handle_workflow_task_failure --

GET_WORKFLOW_ON_ERROR_SQL = text(
    """SELECT on_error FROM horsies_workflows WHERE id = :wf_id"""
)

SET_WORKFLOW_ERROR_SQL = text("""
    UPDATE horsies_workflows
    SET error = :error, updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING'
""")

PAUSE_WORKFLOW_ON_ERROR_SQL = text("""
    UPDATE horsies_workflows
    SET status = 'PAUSED', error = :error, updated_at = NOW()
    WHERE id = :wf_id AND status = 'RUNNING'
""")


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
    error_payload = dumps_json(result.err) if result.is_err() and result.err else None

    if on_error == 'fail':
        # Store error but keep status RUNNING until DAG fully resolves
        # This allows allow_failed_deps tasks to run and produce meaningful final result
        # Status will be set to FAILED in _check_workflow_completion when all tasks are terminal
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
