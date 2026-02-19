"""Workflow lifecycle: start, pause, resume."""

from __future__ import annotations

import uuid
from collections import deque
from typing import TYPE_CHECKING, Any, TypeVar, cast

from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.codec.serde import dumps_json, loads_json
from horsies.core.logging import get_logger
from horsies.core.models.workflow import (
    WorkflowHandle,
    SubWorkflowNode,
    validate_workflow_generic_output_match,
)
from horsies.core.errors import WorkflowValidationError, ErrorCode
from horsies.core.workflows.sql import (
    CHECK_WORKFLOW_EXISTS_SQL,
    INSERT_WORKFLOW_SQL,
    INSERT_WORKFLOW_TASK_SUBWORKFLOW_SQL,
    INSERT_WORKFLOW_TASK_SQL,
    PAUSE_WORKFLOW_SQL,
    NOTIFY_WORKFLOW_DONE_SQL,
    GET_RUNNING_CHILD_WORKFLOWS_SQL,
    PAUSE_CHILD_WORKFLOW_SQL,
    RESUME_WORKFLOW_SQL,
    GET_PENDING_WORKFLOW_TASKS_SQL,
    GET_READY_WORKFLOW_TASKS_SQL,
    GET_PAUSED_CHILD_WORKFLOWS_SQL,
    RESUME_CHILD_WORKFLOW_SQL,
)

logger = get_logger('workflow.engine')

if TYPE_CHECKING:
    from horsies.core.models.workflow import WorkflowSpec
    from horsies.core.brokers.postgres import PostgresBroker

OutT = TypeVar('OutT')


def as_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    str_items: list[str] = []
    for item in cast(list[object], value):
        if not isinstance(item, str):
            return []
        str_items.append(item)
    return str_items


_LIFECYCLE_PRIVATE_EXPORTS = (as_str_list,)


def guard_no_positional_args(node_name: str, args: tuple[Any, ...]) -> None:
    """Raise if a workflow node still carries positional args.

    Under the kwargs-only contract, all runtime parameters must be
    passed via ``kwargs``.  Positional ``args`` should be empty for
    every new write.  This guard catches leftover callers that were
    not migrated.
    """
    if args:
        raise ValueError(
            f"Workflow node '{node_name}' has positional args={args!r}. "
            'Positional args are not supported for workflow nodes; '
            'use kwargs instead.',
        )




async def start_workflow_async(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    workflow_id: str | None = None,
) -> WorkflowHandle[OutT]:
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
    # Deferred import to avoid circular dependency with engine.py
    from horsies.core.workflows.engine import (
        enqueue_workflow_task,
        enqueue_subworkflow_task,
    )

    # Validate output type matches declared generic (E025)
    if spec.workflow_def_cls is not None:
        validate_workflow_generic_output_match(spec.workflow_def_cls, spec)

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

    from horsies.core.types.result import is_err

    init_result = await broker.ensure_schema_initialized()
    if is_err(init_result):
        err = init_result.err_value
        raise RuntimeError(f'Schema initialization failed: {err.message}') from err.exception

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
                return cast(
                    'WorkflowHandle[OutT]',
                    WorkflowHandle(workflow_id=wf_id, broker=broker),
                )

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
                guard_no_positional_args(node.name, node.args)
                await session.execute(
                    INSERT_WORKFLOW_TASK_SUBWORKFLOW_SQL,
                    {
                        'id': wt_id,
                        'wf_id': wf_id,
                        'idx': node.index,
                        'node_id': node.node_id,
                        'name': node.name,
                        'args': dumps_json(()),  # kwargs-only: positional args not persisted
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
                guard_no_positional_args(task.name, task.args)

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
                        'args': dumps_json(()),  # kwargs-only: positional args not persisted
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
                    await enqueue_subworkflow_task(
                        session, broker, wf_id, root_node.index, {}, 0, wf_id
                    )
                else:
                    # Enqueue regular task
                    await enqueue_workflow_task(session, wf_id, root_node.index, {})

        await session.commit()

    return cast(
        'WorkflowHandle[OutT]',
        WorkflowHandle(workflow_id=wf_id, broker=broker),
    )


def start_workflow(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    workflow_id: str | None = None,
) -> WorkflowHandle[OutT]:
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
    from horsies.core.utils.loop_runner import get_shared_runner

    return get_shared_runner().call(start_workflow_async, spec, broker, workflow_id)





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
        await cascade_pause_to_children(session, workflow_id)

        # Notify clients of pause (so get() returns immediately with WORKFLOW_PAUSED)
        await session.execute(
            NOTIFY_WORKFLOW_DONE_SQL,
            {'wf_id': workflow_id},
        )

        await session.commit()
        return True





async def cascade_pause_to_children(
    session: AsyncSession,
    workflow_id: str,
) -> None:
    """
    Iteratively pause all running child workflows using BFS.
    Avoids deep recursion for deeply nested workflows.
    """
    queue: deque[str] = deque([workflow_id])

    while queue:
        current_id = queue.popleft()

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
    from horsies.core.utils.loop_runner import get_shared_runner

    return get_shared_runner().call(pause_workflow, broker, workflow_id)






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
    # Deferred imports to avoid circular dependency with engine.py
    from horsies.core.workflows.engine import (
        try_make_ready_and_enqueue,
        get_dependency_results,
        enqueue_workflow_task,
        enqueue_subworkflow_task,
        check_workflow_completion,
    )

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
            await try_make_ready_and_enqueue(
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
            dep_results = await get_dependency_results(
                session, workflow_id, dep_indices
            )

            if is_subworkflow:
                await enqueue_subworkflow_task(
                    session,
                    broker,
                    workflow_id,
                    task_index,
                    dep_results,
                    depth,
                    root_wf_id,
                )
            else:
                await enqueue_workflow_task(
                    session, workflow_id, task_index, dep_results
                )

        # 4. Cascade resume to paused child workflows
        await cascade_resume_to_children(session, broker, workflow_id)

        # 5. Re-check completion after resume processing.
        # Resume may transition pending tasks directly to SKIPPED/terminal without
        # any subsequent task completion callback to trigger finalization.
        await check_workflow_completion(session, workflow_id, broker)

        await session.commit()
        return True





async def cascade_resume_to_children(
    session: AsyncSession,
    broker: 'PostgresBroker',
    workflow_id: str,
) -> None:
    """
    Iteratively resume all paused child workflows using BFS.
    Avoids deep recursion for deeply nested workflows.
    """
    # Deferred imports to avoid circular dependency with engine.py
    from horsies.core.workflows.engine import (
        try_make_ready_and_enqueue,
        get_dependency_results,
        enqueue_workflow_task,
        enqueue_subworkflow_task,
        check_workflow_completion,
    )

    queue: deque[str] = deque([workflow_id])

    while queue:
        current_id = queue.popleft()

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
                await try_make_ready_and_enqueue(
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
                dep_res = await get_dependency_results(session, child_id, dep_indices)

                if is_sub:
                    await enqueue_subworkflow_task(
                        session,
                        broker,
                        child_id,
                        task_idx,
                        dep_res,
                        child_depth,
                        child_root,
                    )
                else:
                    await enqueue_workflow_task(session, child_id, task_idx, dep_res)

            # Check completion: resume may transition all pending tasks to
            # SKIPPED/terminal without any subsequent callback to finalize.
            await check_workflow_completion(session, child_id, broker)

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
    from horsies.core.utils.loop_runner import get_shared_runner

    return get_shared_runner().call(resume_workflow, broker, workflow_id)
