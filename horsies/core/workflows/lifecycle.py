"""Workflow lifecycle: start, pause, resume."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from collections import deque
from typing import TYPE_CHECKING, Any, TypeVar, assert_never, cast

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from typing import Final

from pydantic import ValidationError as _PydanticValidationError

from horsies.core.codec.json_value import StrictJsonError
from horsies.core.lifecycle.commands import AbandonNodesOfPausedWorkflows
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.lifecycle.persistence import apply_batch_async
from horsies.core.codec.kwargs import (
    encode_kwargs,
    encode_subworkflow_kwargs,
    underlying_task_fn,
)
from horsies.core.codec.json_io import dumps_json, loads_json, SerializationError, SerdeResult
from horsies.core.types.result import Ok, Err, is_err as _is_err
from horsies.core.logging import get_logger
from horsies.core.models.workflow import (
    TaskNode,
    WorkflowHandle,
    SubWorkflowNode,
    validate_workflow_generic_output_match,
)
from horsies.core.models.workflow.handle import _OUTPUTLESS_TERMINALS
from horsies.core.models.workflow.typing_utils import _resolve_source_node_ok_type
from horsies.core.errors import WorkflowValidationError, MultipleValidationErrors, ErrorCode
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.models.workflow.handle_types import (
    HandleErrorCode,
    HandleOperationError,
    HandleResult,
)
from horsies.core.workflows.start_types import (
    WorkflowStartError,
    WorkflowStartErrorCode,
    WorkflowStartResult,
)
from horsies.core.utils.fingerprint import enqueue_fingerprint
from horsies.core.workflows.sql import (
    GET_WORKFLOW_STATUS_SQL,
    INSERT_TASK_FOR_WORKFLOW_SQL,
    INSERT_WORKFLOW_TASKS_BULK_SQL,
    REVERT_PRELINKED_FAST_ROOTS_SQL,
    CHECK_WORKFLOW_EXISTS_SQL,
    GET_WORKFLOW_NAME_SQL,
    INSERT_WORKFLOW_SQL,
    PAUSE_WORKFLOW_SQL,
    NOTIFY_WORKFLOW_DONE_SQL,
    GET_RUNNING_CHILD_WORKFLOWS_SQL,
    PAUSE_CHILD_WORKFLOW_SQL,
    RESET_ABANDONED_WORKFLOW_TASKS_SQL,
    RESUME_WORKFLOW_SQL,
    GET_PENDING_WORKFLOW_TASKS_SQL,
    GET_READY_WORKFLOW_TASKS_SQL,
    GET_PAUSED_CHILD_WORKFLOWS_SQL,
    RESUME_CHILD_WORKFLOW_SQL,
    workflow_task_enqueue_stamp,
)
from horsies.core.history.identity.uuid7 import mint_task_id

logger = get_logger('workflow.engine')

# Internal retry constants for resend_on_transient_err.
# 1 initial attempt + _START_RETRY_COUNT retries = 4 total attempts.
_START_RETRY_COUNT: Final[int] = 3
_START_RETRY_INITIAL_MS: Final[int] = 200
_START_RETRY_MAX_MS: Final[int] = 2000


def _ser_or_raise(result: SerdeResult[str], context: str) -> str:
    """Extract a serde Result or raise with context.

    Used inside the DB transaction zone of ``start_workflow_async``
    where ``SerializationError`` is caught and converted to
    ``Err(SERIALIZATION_FAILED)``.
    """
    if _is_err(result):
        raise SerializationError(f'Failed to serialize {context}: {result.err_value}')
    return result.ok_value


def _encode_node_kwargs_or_raise(task: Any) -> dict[str, Any]:
    """Encode a TaskNode's static kwargs through strict-serde phase 3.

    Routes through `encode_kwargs(task.fn, task.kwargs)` so each kwarg is
    encoded against its declared parameter type. Raises
    ``SerializationError`` on encode failure so the caller's existing
    transaction-zone handling converts it to ``SERIALIZATION_FAILED``.
    """
    underlying = underlying_task_fn(task.fn)
    try:
        return dict(encode_kwargs(underlying, task.kwargs))
    except (StrictJsonError, _PydanticValidationError) as exc:
        raise SerializationError(
            f'Failed to encode kwargs for node {task.name}: {exc}',
        ) from exc


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




def root_retry_and_deadline(
    task_options_json: str | None,
    node_name: str,
) -> tuple[int, str | None, datetime | None]:
    """Extract (max_retries, good_until_str, good_until_dt) for a fast-path root.

    Mirrors enqueue_workflow_task's task_options parsing. One deliberate
    difference: the promotion path folds corrupt options into a FAILED task
    (it re-reads rows written by another transaction); here the JSON was
    serialized moments ago in this same call, so corruption is a horsies
    bug — raise SerializationError and fail the whole start loudly
    (VALIDATION_FAILED, transaction rolled back).

    Raises:
        SerializationError: task_options_json does not parse to an object,
            or retry_policy.max_retries is not an int.
    """
    max_retries = 0
    good_until_str: str | None = None
    good_until_dt: datetime | None = None
    if not task_options_json:
        return max_retries, good_until_str, good_until_dt

    parsed_r = loads_json(task_options_json)
    if _is_err(parsed_r) or not isinstance(parsed_r.ok_value, dict):
        raise SerializationError(
            f'Corrupt task_options for root node {node_name}',
        )
    options = parsed_r.ok_value
    retry_policy = options.get('retry_policy')
    if isinstance(retry_policy, dict):
        # Default 3 mirrors RetryPolicy.max_retries (same as the engine).
        max_retries_raw = retry_policy.get('max_retries', 3)
        if isinstance(max_retries_raw, bool) or not isinstance(
            max_retries_raw, int
        ):
            raise SerializationError(
                f'Invalid max_retries in retry_policy for root node '
                f'{node_name}: {max_retries_raw!r}',
            )
        max_retries = max_retries_raw
    good_until_raw = options.get('good_until')
    if good_until_raw is not None:
        good_until_str = str(good_until_raw)
        try:
            good_until_dt = datetime.fromisoformat(good_until_str)
        except (ValueError, TypeError) as exc:
            # SHA will use None; good_until_str is still passed to the DB
            # column. Loud — fingerprint and stored deadline diverge.
            logger.warning(
                'Unparseable good_until for root node %s (%r): %s',
                node_name,
                good_until_raw,
                exc,
            )
    return max_retries, good_until_str, good_until_dt


async def start_workflow_async(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    workflow_id: str | None = None,
    sent_at: datetime | None = None,
    *,
    resend_on_transient_err: bool = False,
) -> WorkflowStartResult[WorkflowHandle[OutT]]:
    """Start a workflow asynchronously.

    Creates workflow and workflow_tasks records, then enqueues root tasks.

    Args:
        spec: The workflow specification.
        broker: PostgreSQL broker for database operations.
        workflow_id: Optional custom workflow ID.
        sent_at: Optional workflow call-site timestamp from Python.
        resend_on_transient_err: When ``True``, automatically retry up to
            ``_START_RETRY_COUNT`` times on transient infrastructure errors
            (schema init + DB transaction) with exponential backoff.

    Returns:
        ``Ok(WorkflowHandle)`` on success.
        ``Err(WorkflowStartError)`` on failure, with ``code`` and ``retryable``
        fields for programmatic handling.

    Notes:
        If workflow_id is provided and already exists, returns existing handle
        (best-effort idempotent by workflow_id, not payload-verified).
    """
    # Deferred import to avoid circular dependency with engine.py
    from horsies.core.workflows.engine import (
        drain_parent_propagations_in_session,
        enqueue_workflow_task,
        enqueue_subworkflow_task,
    )

    wf_id = workflow_id or str(uuid.uuid4())
    workflow_sent_at = sent_at or datetime.now(timezone.utc)

    # ── Zone 1: Prevalidation (never retried) ────────────────────────
    try:
        if workflow_id is not None:
            try:
                uuid.UUID(workflow_id)
            except ValueError as exc:
                raise WorkflowValidationError(
                    message='workflow_id is not a UUID',
                    code=ErrorCode.WORKFLOW_INVALID_ID,
                    notes=[f'received {workflow_id!r}'],
                    help_text=(
                        'workflow identity is a UUID; supply one (for '
                        'example uuid.uuid4()) or omit workflow_id to have '
                        'one minted'
                    ),
                ) from exc

        spec._require_definition_key()

        # Validate output type matches declared generic (HRS-025)
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
    except asyncio.CancelledError:
        raise
    except (WorkflowValidationError, MultipleValidationErrors, ValueError) as exc:
        return Err(WorkflowStartError(
            code=WorkflowStartErrorCode.VALIDATION_FAILED,
            message=str(exc),
            retryable=False,
            workflow_name=spec.name,
            workflow_id=wf_id,
            exception=exc,
        ))
    except SerializationError as exc:
        return Err(WorkflowStartError(
            code=WorkflowStartErrorCode.VALIDATION_FAILED,
            message=str(exc),
            retryable=False,
            workflow_name=spec.name,
            workflow_id=wf_id,
            exception=exc,
        ))

    # ── Zone 2: Schema init + DB transaction (retried when transient) ─
    from horsies.core.types.result import is_err

    max_attempts = 1 + (_START_RETRY_COUNT if resend_on_transient_err else 0)
    last_err: WorkflowStartError | None = None

    for attempt in range(max_attempts):
        if attempt > 0 and last_err is not None:
            delay_ms = min(
                _START_RETRY_INITIAL_MS * (2 ** (attempt - 1)),
                _START_RETRY_MAX_MS,
            )
            await asyncio.sleep(delay_ms / 1000.0)

        # ── Schema init ──────────────────────────────────────────────
        init_result = await broker.ensure_schema_initialized()
        if is_err(init_result):
            broker_err = init_result.err_value
            last_err = WorkflowStartError(
                code=WorkflowStartErrorCode.ENQUEUE_FAILED,
                message=f'Schema initialization failed: {broker_err.message}',
                retryable=broker_err.retryable,
                workflow_name=spec.name,
                workflow_id=wf_id,
                exception=broker_err.exception,
            )
            if last_err.retryable and attempt < max_attempts - 1:
                continue
            return Err(last_err)

        # ── DB transaction ───────────────────────────────────────────
        try:
            async with broker.session_factory() as session:
                # 1. Insert workflow record atomically (idempotent on workflow ID)
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

                insert_result = await session.execute(
                    INSERT_WORKFLOW_SQL,
                    {
                        'id': wf_id,
                        'name': spec.name,
                        'on_error': spec.on_error.value,
                        'output_idx': output_index,
                        'success_policy': _ser_or_raise(dumps_json(success_policy_json), 'success_policy')
                        if success_policy_json
                        else None,
                        'definition_key': spec.definition_key,
                        'sent_at': workflow_sent_at,
                    },
                )
                inserted_wf_id = insert_result.scalar_one_or_none()
                if inserted_wf_id is None:
                    existing_name_result = await session.execute(
                        GET_WORKFLOW_NAME_SQL,
                        {'wf_id': wf_id},
                    )
                    existing_name_row = existing_name_result.fetchone()
                    existing_name = existing_name_row.name if existing_name_row else None
                    if existing_name is not None and existing_name != spec.name:
                        return Err(WorkflowStartError(
                            code=WorkflowStartErrorCode.VALIDATION_FAILED,
                            message=(
                                f"workflow_id '{wf_id}' already exists for workflow "
                                f"name '{existing_name}', cannot start workflow "
                                f"name '{spec.name}' with the same id"
                            ),
                            retryable=False,
                            workflow_name=spec.name,
                            workflow_id=wf_id,
                        ))
                    logger.warning(
                        f'Workflow {wf_id} already exists, returning existing handle'
                    )
                    return Ok(cast(
                        'WorkflowHandle[OutT]',
                        WorkflowHandle(
                workflow_id=wf_id,
                broker=broker,
                out_type=(
                    _resolve_source_node_ok_type(spec.output)
                    if spec.output is not None
                    else _OUTPUTLESS_TERMINALS
                ),
            ),
                    ))

                # 2+3. Build every node row in memory, then bulk insert.
                # The previous shape executed one INSERT per node plus
                # three statements per root, sequentially — O(4N) round
                # trips in one transaction (~16s at N=119 against a
                # ~33ms-RTT remote database). The batched shape is a fixed
                # handful of pipelined statements.
                #
                # Fast-path roots (plain TaskNodes without args_from /
                # workflow_ctx_from) are inserted directly as ENQUEUED with
                # task_id pre-linked and their horsies_tasks row created in
                # the same batch. The READY->ENQUEUED CAS that the
                # promotion path performs is vacuous here: these rows are
                # created in this same uncommitted transaction (see
                # INSERT_WORKFLOW_TASKS_BULK_SQL). Subworkflow roots and
                # args_from/ctx_from roots keep the per-row path below —
                # they need recursive child starts or engine-owned
                # context assembly and conflict checks.
                #
                # NOTE: serialization failures (_ser_or_raise) raise while
                # building params, before any INSERT executes — the
                # transaction rolls back with no partial state, same
                # guarantee the old mid-loop raise had.
                node_params: list[dict[str, Any]] = []
                fast_root_task_params: list[dict[str, Any]] = []
                fast_root_indexes: list[int] = []
                slow_root_nodes: list[TaskNode[Any] | SubWorkflowNode[Any]] = []

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
                    args_from_json = (
                        _ser_or_raise(dumps_json(args_from_indices), 'args_from')
                        if args_from_indices
                        else None
                    )
                    is_root = not node.waits_for

                    wt_id = str(uuid.uuid4())

                    if isinstance(node, SubWorkflowNode):
                        # SubWorkflowNode: no fn, queue, priority, good_until
                        guard_no_positional_args(node.name, node.args)
                        if is_root:
                            slow_root_nodes.append(node)
                        node_params.append({
                            'id': wt_id,
                            'wf_id': wf_id,
                            'idx': node.index,
                            'node_id': node.node_id,
                            'name': node.name,
                            'args': _ser_or_raise(dumps_json([]), 'positional args'),  # kwargs-only: positional args not persisted
                            'kwargs': _ser_or_raise(
                                dumps_json(
                                    encode_subworkflow_kwargs(
                                        node.workflow_def, node.kwargs,
                                    ),
                                ),
                                f'kwargs for node {node.name}',
                            ),
                            'queue': 'default',  # SubWorkflowNode doesn't have queue
                            'priority': 100,  # SubWorkflowNode doesn't have priority
                            'deps': dep_indices,
                            'args_from': args_from_json,
                            'ctx_from': ctx_from_ids,
                            'allow_failed': node.allow_failed_deps,
                            'join_type': node.join,
                            'min_success': node.min_success,
                            'task_options': None,
                            'status': 'PENDING' if dep_indices else 'READY',
                            'is_subworkflow': True,
                            'sub_wf_name': node.workflow_def.name,
                            'sub_def_key': node.workflow_def._require_definition_key(),
                            'task_id': None,
                            'is_enqueued': False,
                        })
                        continue

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
                            parsed_r = loads_json(task_options_json)
                            if _is_err(parsed_r):
                                raise SerializationError(
                                    f'Corrupt task_options_json for node {task.name}: {parsed_r.err_value}',
                                )
                            if isinstance(parsed_r.ok_value, dict):
                                base_options = parsed_r.ok_value
                        base_options['good_until'] = task.good_until.isoformat()
                        task_options_json = _ser_or_raise(
                            dumps_json(base_options),
                            f'task_options for node {task.name}',
                        )

                    encoded_kwargs = _encode_node_kwargs_or_raise(task)
                    # Queue: use override, else task's declared queue, else "default"
                    resolved_queue = (
                        task.queue
                        or getattr(task.fn, 'task_queue_name', None)
                        or 'default'
                    )
                    # Priority is bound before persist: app.workflow() binds it,
                    # and start validation rejects unbound top-level specs (the
                    # WORKFLOW_UNRESOLVED_PRIORITY guard earlier in this
                    # function). Treat a residual None as an invariant violation
                    # — fail closed, never guess a literal default.
                    if task.priority is None:
                        raise WorkflowValidationError(
                            message='TaskNode reached enqueue with unresolved priority',
                            code=ErrorCode.WORKFLOW_UNRESOLVED_PRIORITY,
                            notes=[f"TaskNode '{task.name}' has priority=None"],
                            help_text='build the workflow via app.workflow() so queue/priority bind before start',
                        )
                    resolved_priority = task.priority
                    args_json = _ser_or_raise(dumps_json([]), 'positional args')  # kwargs-only: positional args not persisted

                    is_fast_root = (
                        is_root and not args_from_indices and not ctx_from_ids
                    )
                    task_id: str | None = None
                    if is_fast_root:
                        task_id = mint_task_id()
                        max_retries, good_until_str, good_until_dt = (
                            root_retry_and_deadline(task_options_json, task.name)
                        )
                        # Task-row kwargs = node kwargs + workflow_meta
                        # (mirrors enqueue_workflow_task; args_from/ctx
                        # injection is structurally absent on fast roots).
                        task_kwargs: dict[str, Any] = dict(encoded_kwargs)
                        task_kwargs['__h_workflow_meta__'] = {
                            'workflow_id': wf_id,
                            'task_index': task.index,
                            'task_name': task.name,
                        }
                        task_kwargs_json = _ser_or_raise(
                            dumps_json(task_kwargs),
                            f'task kwargs for node {task.name}',
                        )
                        task_sent_at = datetime.now(timezone.utc)
                        sha = enqueue_fingerprint(
                            task_name=task.name,
                            queue_name=resolved_queue,
                            priority=resolved_priority,
                            args_json=args_json,
                            kwargs_json=task_kwargs_json,
                            sent_at=task_sent_at,
                            good_until=good_until_dt,
                            enqueue_delay_seconds=None,
                            task_options=task_options_json,
                        )
                        fast_root_indexes.append(task.index)
                        fast_root_task_params.append({
                            'id': task_id,
                            'name': task.name,
                            'queue': resolved_queue,
                            'priority': resolved_priority,
                            'args': args_json,
                            'kwargs': task_kwargs_json,
                            'sent_at': task_sent_at,
                            'max_retries': max_retries,
                            'task_options': task_options_json,
                            'good_until': good_until_str,
                            'enqueue_sha': sha,
                            **workflow_task_enqueue_stamp(
                                task_name=task.name,
                                queue_name=resolved_queue,
                                priority=resolved_priority,
                                args_json=args_json,
                                kwargs_json=task_kwargs_json,
                                good_until=good_until_dt,
                                task_options_json=task_options_json,
                            ),
                        })
                    elif is_root:
                        slow_root_nodes.append(task)

                    node_params.append({
                        'id': wt_id,
                        'wf_id': wf_id,
                        'idx': task.index,
                        'node_id': task.node_id,
                        'name': task.name,
                        'args': args_json,
                        'kwargs': _ser_or_raise(
                            dumps_json(encoded_kwargs),
                            f'kwargs for node {task.name}',
                        ),
                        'queue': resolved_queue,
                        'priority': resolved_priority,
                        'deps': dep_indices,
                        'args_from': args_from_json,
                        'ctx_from': ctx_from_ids,
                        'allow_failed': task.allow_failed_deps,
                        'join_type': task.join,
                        'min_success': task.min_success,
                        'task_options': task_options_json,
                        'status': 'ENQUEUED'
                        if is_fast_root
                        else ('PENDING' if dep_indices else 'READY'),
                        'is_subworkflow': False,
                        'sub_wf_name': None,
                        'sub_def_key': None,
                        'task_id': task_id,
                        'is_enqueued': task_id is not None,
                    })

                if node_params:
                    await session.execute(
                        INSERT_WORKFLOW_TASKS_BULK_SQL, node_params,
                    )
                # Slow-path roots first: subworkflows start child
                # workflows recursively; args_from/ctx_from roots go
                # through the engine's enqueue (context assembly,
                # conflict checks). A slow root's failure may pause THIS
                # workflow (its on_error policy), which must gate the
                # fast-root task rows below.
                for root_node in slow_root_nodes:
                    if root_node.index is not None:
                        if isinstance(root_node, SubWorkflowNode):
                            # Start child workflow
                            await enqueue_subworkflow_task(
                                session,
                                broker,
                                wf_id,
                                root_node.index,
                                all_dep_results={},
                                all_dep_task_names={},
                                parent_depth=0,
                                root_workflow_id=wf_id,
                            )
                        else:
                            # Enqueue regular task
                            await enqueue_workflow_task(
                                session,
                                wf_id,
                                root_node.index,
                                all_dep_results={},
                                all_dep_task_names={},
                                broker=broker,
                            )

                # A synchronously terminal child workflow can queue parent
                # propagation while slow roots run. Drain before the fast-root
                # gate so a child failure can pause THIS workflow before any
                # runnable fast-root task rows land.
                if slow_root_nodes:
                    await drain_parent_propagations_in_session(session, broker)

                # Fast-root task rows land only if the workflow is still
                # runnable: a slow root's failure, or drained child->parent
                # propagation from a synchronously failed child, may have
                # paused it. On pause, the fast roots revert to READY — the
                # paused workflow gains no runnable task rows, and resume
                # re-enqueues READY nodes through the normal path.
                if fast_root_task_params:
                    runnable = True
                    if slow_root_nodes:
                        status_check = await session.execute(
                            GET_WORKFLOW_STATUS_SQL, {'wf_id': wf_id},
                        )
                        status_row = status_check.fetchone()
                        runnable = (
                            status_row is not None
                            and status_row.status == 'RUNNING'
                        )
                    if runnable:
                        await session.execute(
                            INSERT_TASK_FOR_WORKFLOW_SQL, fast_root_task_params,
                        )
                    else:
                        await session.execute(
                            REVERT_PRELINKED_FAST_ROOTS_SQL,
                            {'wf_id': wf_id, 'idxs': fast_root_indexes},
                        )

                await session.commit()

        except asyncio.CancelledError:
            raise
        except SerializationError as exc:
            # Deterministic — never retry.
            return Err(WorkflowStartError(
                code=WorkflowStartErrorCode.VALIDATION_FAILED,
                message=str(exc),
                retryable=False,
                workflow_name=spec.name,
                workflow_id=wf_id,
                exception=exc,
            ))
        except ValueError as exc:
            # guard_no_positional_args — deterministic, never retry.
            return Err(WorkflowStartError(
                code=WorkflowStartErrorCode.VALIDATION_FAILED,
                message=str(exc),
                retryable=False,
                workflow_name=spec.name,
                workflow_id=wf_id,
                exception=exc,
            ))
        except Exception as exc:
            last_err = WorkflowStartError(
                code=WorkflowStartErrorCode.ENQUEUE_FAILED,
                message=f'{type(exc).__name__}: {exc}',
                retryable=is_retryable_connection_error(exc),
                workflow_name=spec.name,
                workflow_id=wf_id,
                exception=exc,
            )
            if last_err.retryable and attempt < max_attempts - 1:
                continue
            return Err(last_err)

        # Success — exit retry loop.
        return Ok(cast(
            'WorkflowHandle[OutT]',
            WorkflowHandle(
                workflow_id=wf_id,
                broker=broker,
                out_type=(
                    _resolve_source_node_ok_type(spec.output)
                    if spec.output is not None
                    else _OUTPUTLESS_TERMINALS
                ),
            ),
        ))

    # Exhausted all attempts (should not reach here, but satisfy type checker).
    if last_err is not None:
        return Err(last_err)
    return Err(WorkflowStartError(
        code=WorkflowStartErrorCode.ENQUEUE_FAILED,
        message='Exhausted retry attempts',
        retryable=False,
        workflow_name=spec.name,
        workflow_id=wf_id,
    ))


def start_workflow(
    spec: 'WorkflowSpec[OutT]',
    broker: 'PostgresBroker',
    workflow_id: str | None = None,
    sent_at: datetime | None = None,
    *,
    resend_on_transient_err: bool = False,
) -> WorkflowStartResult[WorkflowHandle[OutT]]:
    """Start a workflow synchronously.

    Sync wrapper around ``start_workflow_async()``.

    Inner async ``Err`` passes through unchanged.
    Bridge infrastructure failures are returned as ``INTERNAL_FAILED``.
    Unexpected sync-path exceptions are returned as ``INTERNAL_FAILED``.

    Args:
        spec: The workflow specification.
        broker: PostgreSQL broker for database operations.
        workflow_id: Optional custom workflow ID.
        sent_at: Optional workflow call-site timestamp from Python.
        resend_on_transient_err: Forwarded to ``start_workflow_async()``.

    Returns:
        ``Ok(WorkflowHandle)`` on success, ``Err(WorkflowStartError)`` on failure.
    """
    from horsies.core.utils.loop_runner import get_shared_runner, LoopRunnerError

    wf_id = workflow_id or str(uuid.uuid4())
    workflow_sent_at = sent_at or datetime.now(timezone.utc)
    try:
        return get_shared_runner().call(
            start_workflow_async,
            spec,
            broker,
            wf_id,
            workflow_sent_at,
            resend_on_transient_err=resend_on_transient_err,
        )
    except LoopRunnerError as exc:
        return Err(WorkflowStartError(
            code=WorkflowStartErrorCode.INTERNAL_FAILED,
            message=f'Sync bridge failed: {type(exc).__name__}: {exc}',
            retryable=False,
            workflow_name=spec.name,
            workflow_id=wf_id,
            exception=exc,
        ))
    except Exception as exc:
        return Err(WorkflowStartError(
            code=WorkflowStartErrorCode.INTERNAL_FAILED,
            message=f'Internal start failure: {type(exc).__name__}: {exc}',
            retryable=False,
            workflow_name=spec.name,
            workflow_id=wf_id,
            exception=exc,
        ))





async def pause_workflow(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> HandleResult[bool]:
    """Pause a running workflow.

    Transitions workflow from RUNNING to PAUSED state. Already-running tasks
    will continue to completion, but:
    - No new PENDING tasks will become READY
    - No READY tasks will be enqueued
    - Workflow completion check is skipped while PAUSED

    Also cascades pause to all running child workflows (iteratively, not recursively).

    Use resume_workflow() to continue execution.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to pause.

    Returns:
        ``Ok(True)`` if workflow was paused.
        ``Ok(False)`` if workflow exists but is not RUNNING (no-op).
        ``Err(WORKFLOW_NOT_FOUND)`` if workflow does not exist.
        ``Err(DB_OPERATION_FAILED)`` on infrastructure failure.
    """
    try:
        async with broker.session_factory() as session:
            result = await session.execute(
                PAUSE_WORKFLOW_SQL,
                {'wf_id': workflow_id},
            )
            row = result.fetchone()
            if row is None:
                exists = await session.execute(
                    CHECK_WORKFLOW_EXISTS_SQL,
                    {'wf_id': workflow_id},
                )
                if exists.fetchone() is None:
                    return Err(HandleOperationError(
                        code=HandleErrorCode.WORKFLOW_NOT_FOUND,
                        message=f'Workflow {workflow_id} not found',
                        retryable=False,
                        workflow_id=workflow_id,
                    ))
                return Ok(False)

            # Cascade pause to running child workflows (iterative BFS)
            paused_workflow_ids = [workflow_id]
            paused_workflow_ids.extend(
                await cascade_pause_to_children(session, workflow_id)
            )

            # Claimed-but-not-started task rows cannot safely remain claimable
            # after their workflow_task is reset to READY for resume. Cancel the
            # abandoned internal task rows and let resume enqueue fresh rows.
            abandon_outcomes = await apply_batch_async(
                await session.connection(),
                AbandonNodesOfPausedWorkflows(
                    workflow_ids=tuple(paused_workflow_ids),
                ),
            )
            abandoned_task_ids: list[str] = []
            for outcome in abandon_outcomes:
                match outcome:
                    case Applied(task_id=task_id):
                        abandoned_task_ids.append(task_id)
                    case (
                        AlreadyApplied()
                        | LostClaim()
                        | SourceStateConflict()
                        | TaskAbsent()
                    ):
                        raise RuntimeError(
                            'paused-workflow abandonment returned '
                            f'{type(outcome).__name__}; workflow-scoped '
                            'batches report transitioned rows only'
                        )
                    case _ as unreachable:
                        assert_never(unreachable)
            if abandoned_task_ids:
                await session.execute(
                    RESET_ABANDONED_WORKFLOW_TASKS_SQL,
                    {'task_ids': abandoned_task_ids},
                )

            # Notify clients of pause (so get() returns immediately with WORKFLOW_PAUSED)
            await session.execute(
                NOTIFY_WORKFLOW_DONE_SQL,
                {'wf_id': workflow_id},
            )

            await session.commit()
            return Ok(True)
    except SQLAlchemyError as exc:
        return Err(HandleOperationError(
            code=HandleErrorCode.DB_OPERATION_FAILED,
            message=f'Pause failed for workflow {workflow_id}: {type(exc).__name__}: {exc}',
            retryable=is_retryable_connection_error(exc),
            workflow_id=workflow_id,
            exception=exc,
        ))
    except Exception as exc:
        return Err(HandleOperationError(
            code=HandleErrorCode.INTERNAL_FAILED,
            message=f'Pause failed for workflow {workflow_id}: {type(exc).__name__}: {exc}',
            retryable=False,
            workflow_id=workflow_id,
            exception=exc,
        ))





async def cascade_pause_to_children(
    session: AsyncSession,
    workflow_id: str,
) -> list[str]:
    """
    Iteratively pause all running child workflows using BFS.
    Avoids deep recursion for deeply nested workflows.
    """
    queue: deque[str] = deque([workflow_id])
    paused_child_ids: list[str] = []

    while queue:
        current_id = queue.popleft()

        # Find running child workflows
        children = await session.execute(
            GET_RUNNING_CHILD_WORKFLOWS_SQL,
            {'wf_id': current_id},
        )

        for child_row in children.fetchall():
            child_id = child_row.id
            paused_child_ids.append(child_id)

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

    return paused_child_ids


def pause_workflow_sync(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> HandleResult[bool]:
    """Pause a running workflow synchronously.

    Sync wrapper around ``pause_workflow()``.

    Inner async ``Err`` passes through unchanged.
    Bridge infrastructure failures are returned as ``LOOP_RUNNER_FAILED``.
    Unexpected sync-path exceptions are returned as ``INTERNAL_FAILED``.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to pause.

    Returns:
        ``Ok(True)`` if workflow was paused.
        ``Ok(False)`` if workflow exists but is not RUNNING (no-op).
        ``Err(...)`` on failure — see ``HandleErrorCode``.
    """
    from horsies.core.utils.loop_runner import get_shared_runner, LoopRunnerError

    try:
        return get_shared_runner().call(pause_workflow, broker, workflow_id)
    except LoopRunnerError as exc:
        return Err(HandleOperationError(
            code=HandleErrorCode.LOOP_RUNNER_FAILED,
            message=f'Sync bridge failed: {type(exc).__name__}: {exc}',
            retryable=False,
            workflow_id=workflow_id,
            exception=exc,
        ))
    except Exception as exc:
        return Err(HandleOperationError(
            code=HandleErrorCode.INTERNAL_FAILED,
            message=f'Internal pause failure: {type(exc).__name__}: {exc}',
            retryable=False,
            workflow_id=workflow_id,
            exception=exc,
        ))






async def resume_workflow(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> HandleResult[bool]:
    """Resume a paused workflow.

    Re-evaluates all PENDING tasks (marks READY if deps are terminal) and
    enqueues all READY tasks. Only works if workflow is currently PAUSED.

    Also cascades resume to all paused child workflows (iteratively, not recursively).

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to resume.

    Returns:
        ``Ok(True)`` if workflow was resumed.
        ``Ok(False)`` if workflow exists but is not PAUSED (no-op).
        ``Err(WORKFLOW_NOT_FOUND)`` if workflow does not exist.
        ``Err(DB_OPERATION_FAILED)`` on infrastructure failure.
    """
    # Deferred imports to avoid circular dependency with engine.py
    from horsies.core.workflows.engine import (
        try_make_ready_and_enqueue,
        get_dependency_results,
        enqueue_workflow_task,
        enqueue_subworkflow_task,
        check_workflow_completion,
        drain_parent_propagations_in_session,
    )

    try:
        async with broker.session_factory() as session:
            # 1. Transition PAUSED → RUNNING (only if currently PAUSED)
            result = await session.execute(
                RESUME_WORKFLOW_SQL,
                {'wf_id': workflow_id},
            )
            row = result.fetchone()
            if row is None:
                exists = await session.execute(
                    CHECK_WORKFLOW_EXISTS_SQL,
                    {'wf_id': workflow_id},
                )
                if exists.fetchone() is None:
                    return Err(HandleOperationError(
                        code=HandleErrorCode.WORKFLOW_NOT_FOUND,
                        message=f'Workflow {workflow_id} not found',
                        retryable=False,
                        workflow_id=workflow_id,
                    ))
                return Ok(False)

            depth = row.depth or 0
            root_wf_id = row.root_workflow_id or workflow_id

            # 2. Find all PENDING tasks and try to make them READY
            pending_result = await session.execute(
                GET_PENDING_WORKFLOW_TASKS_SQL,
                {'wf_id': workflow_id},
            )
            pending_indices = [r.task_index for r in pending_result.fetchall()]

            for task_index in pending_indices:
                await try_make_ready_and_enqueue(
                    session, broker, workflow_id, task_index, depth, root_wf_id,
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
                dep_results, dep_task_names, dep_definition_keys = await get_dependency_results(
                    session, workflow_id, dep_indices, app=broker.app,
                )

                if is_subworkflow:
                    await enqueue_subworkflow_task(
                        session,
                        broker,
                        workflow_id,
                        task_index,
                        all_dep_results=dep_results,
                        all_dep_task_names=dep_task_names,
                        parent_depth=depth,
                        root_workflow_id=root_wf_id,
                    )
                else:
                    await enqueue_workflow_task(
                        session,
                        workflow_id,
                        task_index,
                        all_dep_results=dep_results,
                        all_dep_task_names=dep_task_names,
                        broker=broker,
                        all_dep_definition_keys=dep_definition_keys,
                    )

            # 4. Cascade resume to paused child workflows
            await cascade_resume_to_children(session, broker, workflow_id)

            # 5. Re-check completion after resume processing.
            # Resume may transition pending tasks directly to SKIPPED/terminal without
            # any subsequent task completion callback to trigger finalization.
            await check_workflow_completion(session, workflow_id, broker)

            # Drain parent propagations queued by the completion checks above
            # (incl. the cascade's) — in-session on this cold path, matching
            # the pre-denest in-transaction behavior.
            await drain_parent_propagations_in_session(session, broker)

            await session.commit()

        # A task can finish while this transaction has resumed a child workflow
        # but before that status change is visible to the task-completion
        # callback. The callback observes PAUSED and correctly avoids
        # propagation; once resume commits, run the recovery completion pass to
        # close that race through the same canonical engine paths. Scope it to
        # the resumed workflow's tree (self + descendants) so resume does not
        # trigger a full-DB recovery sweep over unrelated workflows.
        async with broker.session_factory() as recovery_session:
            from horsies.core.workflows.recovery import (
                GET_WORKFLOW_TREE_IDS_SQL,
                recover_stuck_workflows,
            )

            tree_result = await recovery_session.execute(
                GET_WORKFLOW_TREE_IDS_SQL,
                {'wf_id': workflow_id},
            )
            tree_ids = [r.id for r in tree_result.fetchall()]
            await recover_stuck_workflows(
                recovery_session, broker, scope_workflow_ids=tree_ids,
            )
            await recovery_session.commit()

        return Ok(True)
    except SQLAlchemyError as exc:
        return Err(HandleOperationError(
            code=HandleErrorCode.DB_OPERATION_FAILED,
            message=f'Resume failed for workflow {workflow_id}: {type(exc).__name__}: {exc}',
            retryable=is_retryable_connection_error(exc),
            workflow_id=workflow_id,
            exception=exc,
        ))
    except Exception as exc:
        return Err(HandleOperationError(
            code=HandleErrorCode.INTERNAL_FAILED,
            message=f'Resume failed for workflow {workflow_id}: {type(exc).__name__}: {exc}',
            retryable=False,
            workflow_id=workflow_id,
            exception=exc,
        ))





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
            child_id = child_row.id
            child_depth = child_row.depth or 0
            child_root = child_row.root_workflow_id or child_id

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
                    session, broker, child_id, pending_row.task_index, child_depth, child_root,
                )

            child_ready = await session.execute(
                GET_READY_WORKFLOW_TASKS_SQL,
                {'wf_id': child_id},
            )
            for ready_row in child_ready.fetchall():
                task_idx = ready_row.task_index
                deps = ready_row.dependencies
                is_sub = ready_row.is_subworkflow
                dep_indices: list[int] = (
                    cast(list[int], deps) if isinstance(deps, list) else []
                )
                dep_res, dep_names, dep_def_keys = await get_dependency_results(
                    session, child_id, dep_indices, app=broker.app,
                )

                if is_sub:
                    await enqueue_subworkflow_task(
                        session,
                        broker,
                        child_id,
                        task_idx,
                        all_dep_results=dep_res,
                        all_dep_task_names=dep_names,
                        parent_depth=child_depth,
                        root_workflow_id=child_root,
                    )
                else:
                    await enqueue_workflow_task(
                        session,
                        child_id,
                        task_idx,
                        all_dep_results=dep_res,
                        all_dep_task_names=dep_names,
                        broker=broker,
                        all_dep_definition_keys=dep_def_keys,
                    )

            # Check completion: resume may transition all pending tasks to
            # SKIPPED/terminal without any subsequent callback to finalize.
            await check_workflow_completion(session, child_id, broker)

            # Add to queue to resume its children
            queue.append(child_id)


def resume_workflow_sync(
    broker: 'PostgresBroker',
    workflow_id: str,
) -> HandleResult[bool]:
    """Resume a paused workflow synchronously.

    Sync wrapper around ``resume_workflow()``.

    Inner async ``Err`` passes through unchanged.
    Bridge infrastructure failures are returned as ``LOOP_RUNNER_FAILED``.
    Unexpected sync-path exceptions are returned as ``INTERNAL_FAILED``.

    Args:
        broker: PostgreSQL broker for database operations.
        workflow_id: The workflow ID to resume.

    Returns:
        ``Ok(True)`` if workflow was resumed.
        ``Ok(False)`` if workflow exists but is not PAUSED (no-op).
        ``Err(...)`` on failure — see ``HandleErrorCode``.
    """
    from horsies.core.utils.loop_runner import get_shared_runner, LoopRunnerError

    try:
        return get_shared_runner().call(resume_workflow, broker, workflow_id)
    except LoopRunnerError as exc:
        return Err(HandleOperationError(
            code=HandleErrorCode.LOOP_RUNNER_FAILED,
            message=f'Sync bridge failed: {type(exc).__name__}: {exc}',
            retryable=False,
            workflow_id=workflow_id,
            exception=exc,
        ))
    except Exception as exc:
        return Err(HandleOperationError(
            code=HandleErrorCode.INTERNAL_FAILED,
            message=f'Internal resume failure: {type(exc).__name__}: {exc}',
            retryable=False,
            workflow_id=workflow_id,
            exception=exc,
        ))


async def expire_paused_workflows(
    session_factory: 'async_sessionmaker[AsyncSession]',
    *,
    older_than: timedelta,
    batch_size: int = 50,
) -> int:
    """Expire PAUSED workflows older than the declared age policy.

    The writer is the cancel path's locked sequence with EXPIRED as the
    terminal status: workflow row locked first (the cancel lock-order
    invariant), backing tasks and node rows locked, the status CAS
    applied (PAUSED only — a concurrent resume wins), the shared batch
    terminalization moving still-live backing rows to history under the
    workflow-cancel kind, and the node skips driven by the batch's
    reported ids. No parent-to-child cascade: pause already cascaded,
    so every paused child carries its own age and expires on it.

    Returns the number of workflows expired. One transaction per
    workflow keeps the pass bounded; a failure on one workflow leaves
    the remainder for the next tick.
    """
    from horsies.core.models.workflow.handle import (
        LOCK_WORKFLOW_BACKING_TASKS_FOR_CANCEL_SQL,
        LOCK_WORKFLOW_ROW_FOR_CANCEL_SQL,
        LOCK_WORKFLOW_TASKS_FOR_CANCEL_SQL,
        SKIP_CANCELLED_ENQUEUED_WORKFLOW_TASKS_SQL,
        SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL,
        WF_TASK_TERMINAL_VALUES,
    )
    from horsies.core.lifecycle.commands import (
        CancelNodesOfCancelledWorkflow,
    )

    from horsies.core.types.status import TASK_TERMINAL_STATES

    task_terminal_values = [
        status.value for status in TASK_TERMINAL_STATES
    ]
    error_text = f'paused_workflow_auto_cancel_after: {older_than}'
    expired_count = 0
    async with session_factory() as candidate_session:
        candidate_rows = (
            await candidate_session.execute(
                text("""
                    SELECT id FROM horsies_workflows
                    WHERE status = 'PAUSED'
                      AND updated_at < NOW() - CAST(:age AS interval)
                    ORDER BY updated_at
                    LIMIT :batch_size
                """),
                {'age': older_than, 'batch_size': batch_size},
            )
        ).all()
    candidate_ids = [row.id for row in candidate_rows]

    for workflow_id in candidate_ids:
        async with session_factory() as session:
            lock_params = {
                'wf_id': workflow_id,
                'wf_task_terminal_states': WF_TASK_TERMINAL_VALUES,
                'task_terminal_states': task_terminal_values,
            }
            await session.execute(
                LOCK_WORKFLOW_ROW_FOR_CANCEL_SQL, {'wf_id': workflow_id}
            )
            await session.execute(
                LOCK_WORKFLOW_BACKING_TASKS_FOR_CANCEL_SQL, lock_params
            )
            await session.execute(
                LOCK_WORKFLOW_TASKS_FOR_CANCEL_SQL, lock_params
            )
            await session.execute(
                LOCK_WORKFLOW_BACKING_TASKS_FOR_CANCEL_SQL, lock_params
            )
            expired_row = (
                await session.execute(
                    text("""
                        UPDATE horsies_workflows
                        SET status = 'EXPIRED',
                            error = :error,
                            completed_at = NOW(),
                            updated_at = NOW()
                        WHERE id = :wf_id
                          AND status = 'PAUSED'
                          AND updated_at < NOW() - CAST(:age AS interval)
                        RETURNING id
                    """),
                    {
                        'wf_id': workflow_id,
                        'error': error_text,
                        'age': older_than,
                    },
                )
            ).first()
            if expired_row is None:
                await session.rollback()
                continue
            outcomes = await apply_batch_async(
                await session.connection(),
                CancelNodesOfCancelledWorkflow(
                    workflow_ids=(workflow_id,),
                ),
            )
            await session.execute(
                SKIP_WORKFLOW_TASKS_ON_CANCEL_SQL, {'wf_id': workflow_id}
            )
            await session.execute(
                SKIP_CANCELLED_ENQUEUED_WORKFLOW_TASKS_SQL,
                {
                    'wf_id': workflow_id,
                    'cancelled_task_ids': [
                        outcome.task_id for outcome in outcomes
                    ],
                },
            )
            await session.commit()
            expired_count += 1
    return expired_count
