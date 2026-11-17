"""The engine's session phase 2, before ``horsies_workflow_node_finish``: a copy.

``on_workflow_task_complete`` wrote the node of a finished workflow task
with one statement (``COMPLETE_WORKFLOW_TASK_SQL``: the capacity wake, the
workflow row lock, the node write, the delete of the phase-2 record), then
ran ``apply_node_progression`` in the same transaction: the failure policy,
the paused guard, the dependents and the completion check. Phase-2 recovery
ran ``apply_node_progression`` after ``horsies_phase2_consume``.

The worker's phase 2 runs ``horsies_workflow_node_finish`` and phase-2
recovery runs ``horsies_workflow_phase2_recover``, so the package no longer
holds the statement or these two functions; the dual-run references
(``workflow_node_finish.py``, ``workflow_phase2_recover.py``) use this copy.
The engine functions it calls stay in the package for workflow recovery:
``_handle_workflow_task_failure``, ``_process_dependents`` and
``check_workflow_completion``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.json_value import escape_lone_surrogates
from horsies.core.codec.typed import Json, encode_task_result
from horsies.core.history.names import WORKFLOW_PHASE2_PENDING, WORKFLOW_PHASE2_QUARANTINE
from horsies.core.logging import get_logger
from horsies.core.models.tasks import OperationalErrorCode, TaskError, TaskResult
from horsies.core.models.workflow import WF_TASK_TERMINAL_VALUES, WorkflowStatus
from horsies.core.workflows.engine import (
    _handle_workflow_task_failure,  # pyright: ignore[reportPrivateUsage]
    _process_dependents,  # pyright: ignore[reportPrivateUsage]
    _resolve_source_ok_type,  # pyright: ignore[reportPrivateUsage]
    _ser_or_raise,  # pyright: ignore[reportPrivateUsage]
    check_workflow_completion,
)

logger = get_logger('workflow.engine')

# Single round trip replacing the old locate -> lock -> CAS-update triple
# (each statement cost one round trip under the workflow lock). One
# statement does all three:
#   - `found` locates the workflow task by its backing task id and takes
#     the workflow row's FOR UPDATE lock (serializes completion handling
#     per workflow — same lock discipline as before).
#   - `upd` CAS-updates the node to its terminal status (no-op if a
#     concurrent path already made it terminal).
#   - The outer SELECT returns the progression context (wf_status, depth,
#     root_workflow_id, on_error) so no follow-up reads are needed: the
#     held lock freezes the workflow row for the rest of the transaction
#     (pause/cancel are UPDATEs on the same row and block on it), and
#     data-modifying CTEs always execute exactly once, so the CAS runs
#     even though the outer query only reads `found`.
# The statement returns one row. workflow_id NULL = not a workflow task, or
# its workflow row is missing; the caller treats both as a no-op (matches
# the old early returns).
#
# `woken` sends the capacity wake of the finished task's queue when
# :wake_channel is not NULL, whether or not a node is found. A NOTIFY is
# delivered at COMMIT, so its place in the transaction changes nothing for a
# listener; when the transaction rolls back, no wake is sent. A wake that the
# server refuses fails the statement, and the transaction with it. The outer
# query reads `woken` through an aggregate, so it runs exactly once: a SELECT
# CTE that no query reads does not run.
#
# `consumed` deletes the phase-2 pending row that the task's terminal move
# wrote, and `released` the quarantine row a pending row can point at, as
# the consumption function does on a durable disposition. The progression
# runs in this transaction, so the delete commits with it or not at all, and
# a later recovery pass has nothing left to consume. Both read `found`, so
# they run after the workflow row lock: the pending row is locked after the
# workflow row, the order of horsies_phase2_consume.
#
# `has_dependents` is whether a node of the same workflow lists the node in
# its dependencies, with the predicate of the dependents read
# (GET_DEPENDENTS_EVAL_SQL). The dependencies of a workflow are written with
# its nodes, in the transaction that starts the workflow
# (INSERT_WORKFLOW_TASKS_BULK_SQL), and no statement changes them, so the
# value does not depend on the snapshot. When it is false, the dependents
# read of the node returns no row, and the progression does not send it.
#
# Lock-order invariant: this statement locks the workflow row (`found`'s
# FOR UPDATE OF w) before the workflow_task row (`upd`'s UPDATE). Every
# transaction that locks both horsies_workflows and horsies_workflow_tasks
# rows must take them in that order — workflows before workflow_tasks — or it
# can deadlock against this path (Postgres aborts one side, SQLSTATE 40P01).
# The cancel (horsies_workflow_cancel) and the paused expiry
# (LOCK_WORKFLOW_ROW_FOR_CANCEL_SQL in models/workflow/handle.py) are held to
# the same order.
COMPLETE_WORKFLOW_TASK_SQL = text(f"""
    WITH woken AS (
        SELECT pg_notify(
            CAST(:wake_channel AS text), CAST(:wake_payload AS text)
        )
        WHERE CAST(:wake_channel AS text) IS NOT NULL
    ),
    found AS (
        SELECT wt.workflow_id, wt.task_index, wt.task_id,
               w.status AS wf_status, w.depth, w.root_workflow_id, w.on_error
        FROM horsies_workflow_tasks wt
        JOIN horsies_workflows w ON w.id = wt.workflow_id
        WHERE wt.task_id = :tid
        FOR UPDATE OF w
    ),
    upd AS (
        UPDATE horsies_workflow_tasks wt
        SET status = :status, result = :result, completed_at = NOW()
        FROM found
        WHERE wt.workflow_id = found.workflow_id
          AND wt.task_index = found.task_index
          AND NOT (wt.status = ANY(:terminal_states))
        RETURNING wt.task_index
    ),
    consumed AS (
        DELETE FROM {WORKFLOW_PHASE2_PENDING} pending
        USING found
        WHERE pending.task_id = found.task_id
          AND pending.workflow_id = found.workflow_id
        RETURNING pending.recovery_source, pending.quarantine_task_id
    ),
    released AS (
        DELETE FROM {WORKFLOW_PHASE2_QUARANTINE} quarantined
        USING consumed
        WHERE consumed.recovery_source = 'QUARANTINE'
          AND quarantined.task_id = consumed.quarantine_task_id
    )
    SELECT found.workflow_id, found.task_index, found.wf_status,
           found.depth, found.root_workflow_id, found.on_error,
           EXISTS (SELECT 1 FROM upd) AS cas_won,
           EXISTS (
               SELECT 1 FROM horsies_workflow_tasks dependent
               WHERE dependent.workflow_id = found.workflow_id
                 AND dependent.dependencies && ARRAY[found.task_index]
           ) AS has_dependents
    FROM (SELECT count(*) AS sent FROM woken) AS wake
    LEFT JOIN found ON TRUE
""")


def pause_halts_progression(workflow_status: str | None) -> bool:
    """Whether a workflow's status stops progression after a node write.

    Read at lock time: pause and cancel are UPDATEs on the locked
    workflow row, so the status cannot change for the rest of the
    transaction that observed it. Both progression paths — the
    in-process completion and the outbox-driven recovery — ask this one
    question rather than spelling the comparison twice.
    """
    return workflow_status == WorkflowStatus.PAUSED.value


@dataclass(frozen=True, slots=True)
class QueueWake:
    """The capacity wake of a finished task's queue: ``pg_notify(channel,
    payload)``, sent in the transaction that advances the node."""

    channel: str
    payload: str


@dataclass(frozen=True, slots=True)
class NoQueueWake:
    """The caller sends no capacity wake."""


type NodeCompletionWake = QueueWake | NoQueueWake


@dataclass(frozen=True, slots=True)
class NodeHasNoDependents:
    """No node of the workflow lists the node in its dependencies.

    The dependencies of a workflow are written with its nodes, in the
    transaction that starts it, and never change, so this holds for the
    life of the workflow. The progression does not read the dependents."""


@dataclass(frozen=True, slots=True)
class NodeMayHaveDependents:
    """A node of the workflow may list the node in its dependencies: the
    progression reads the dependents."""


type NodeDependents = NodeHasNoDependents | NodeMayHaveDependents


def _node_dependents(has_dependents: bool) -> NodeDependents:
    """The dependents of a node, from ``has_dependents`` of
    ``COMPLETE_WORKFLOW_TASK_SQL``. Pure."""
    if has_dependents:
        return NodeMayHaveDependents()
    return NodeHasNoDependents()


def _wake_parameters(wake: NodeCompletionWake) -> dict[str, str | None]:
    """The wake parameters of ``COMPLETE_WORKFLOW_TASK_SQL``; NULL sends
    none. Pure."""
    match wake:
        case QueueWake(channel=channel, payload=payload):
            return {'wake_channel': channel, 'wake_payload': payload}
        case NoQueueWake():
            return {'wake_channel': None, 'wake_payload': None}
        case _ as unreachable:
            assert_never(unreachable)


async def on_workflow_task_complete(
    session: AsyncSession,
    task_id: str,
    result: TaskResult[Any, TaskError],
    broker: PostgresBroker | None,
    *,
    task_name: str,
    wake: NodeCompletionWake = NoQueueWake(),
) -> None:
    """
    The node write and progression of a finished workflow task, with the
    session statements.

    Raises:
        SerializationError: a value that the engine built does not
            serialize, for example the encoded result envelope. This is a
            defect; the caller's transaction rolls back.
        Exception: DB/engine errors propagate to the typed seams that own
            recovery: the worker folds them into a phase2 finalize error
            with bounded retries, pause and resume fold a database error
            into a HandleOperationError variant, and the reaper contains
            each recovery pass as one unit.

    ``task_name`` is the completed task's registered name, read by the
    caller from the task row it already holds (worker dispatch/finalize,
    recovery case 1.7). It equals the workflow node's ``task_name`` by
    construction (the node enqueue writes the task row from the node), and
    is needed BEFORE any statement: the single locate+lock+CAS statement
    binds the encoded result, and encoding resolves the source task's
    ``task_ok_type`` from this name.

    ``wake`` is sent by the first statement, whether or not the task has a
    node: the worker's phase 2 passes the capacity wake of the task's queue,
    so the wake costs no round trip of its own. It is delivered at COMMIT.

    ``broker`` is required (explicit `None` allowed for
    unit-test branches that return before dependency propagation). Removing
    the implicit default surfaces missing-broker bugs at the call site instead
    of degrading to a `WORKER_SERIALIZATION_ERROR` during args_from envelope
    encoding when `broker.app` is needed to resolve the source task's
    `task_ok_type`.
    """
    # 1. Decide terminal status and encode the result envelope before any
    # statement.
    encoded = _encode_node_result(
        result,
        broker,
        task_id=task_id,
        task_name=task_name,
    )

    # 2. One statement: locate the workflow task, take the workflow row's
    # FOR UPDATE lock (serializes completion handling per workflow so
    # concurrent completions don't race dependency promotion), CAS-update
    # the node to its terminal status, and return the progression context.
    row = (
        await session.execute(
            COMPLETE_WORKFLOW_TASK_SQL,
            {
                'tid': task_id,
                'status': encoded.status,
                'result': _ser_or_raise(
                    dumps_json(encoded.envelope),
                    f'completion result for task {task_id}',
                ),
                'terminal_states': WF_TASK_TERMINAL_VALUES,
                **_wake_parameters(wake),
            },
        )
    ).one()
    if row.workflow_id is None:
        return  # Not a workflow task (or its workflow row is missing)

    workflow_id = row.workflow_id
    task_index = row.task_index
    if not row.cas_won:
        logger.debug(
            'Workflow task already terminal, skipping progression: '
            'workflow=%s task_index=%s task_id=%s',
            workflow_id, task_index, task_id,
        )
        return

    # 3-6. Everything after the node write, shared with the outbox-driven
    # recovery path so a node that reaches its terminal status by either
    # route progresses identically.
    await apply_node_progression(
        session,
        broker,
        workflow_id=workflow_id,
        task_index=task_index,
        failure=encoded.result if encoded.result.is_err() else None,
        on_error=row.on_error,
        workflow_status=row.wf_status,
        depth=row.depth,
        root_workflow_id=row.root_workflow_id,
        dependents=_node_dependents(row.has_dependents),
    )


@dataclass(frozen=True, slots=True)
class _EncodedNodeResult:
    """The terminal write of a node and the result its progression sees."""

    status: Literal['COMPLETED', 'FAILED']
    envelope: dict[str, Json]
    """The wire envelope of ``result``, which the node stores."""
    result: TaskResult[Any, TaskError]
    """The task's result, or the serialization error that replaced it."""


def _encode_node_result(
    result: TaskResult[Any, TaskError],
    broker: PostgresBroker | None,
    *,
    task_id: str,
    task_name: str,
) -> _EncodedNodeResult:
    """Encode the result of a node's task into the wire envelope.

    The envelope carries the ``__h_task_result__`` marker that the readers of
    a stored node result (``_decode_stored_task_result``,
    ``_decode_err_only``, the child to parent pass-through of
    ``on_subworkflow_complete``) expect. The source task's declared
    ``task_ok_type`` validates the value when the broker has an app; ``Any``
    is used otherwise (test and recovery paths), so a missing type never
    fails the completion.

    A result that does not encode is logged and replaced by a FAILED result
    with ``WORKER_SERIALIZATION_ERROR``: a deterministic encode failure
    would otherwise repeat on every phase-2 retry. The encode of that error
    is fixed-schema and total. No I/O besides the log.
    """
    ok_type_for_encode: Any = Any
    if broker is not None and broker.app is not None:
        resolved = _resolve_source_ok_type(broker.app, task_name)
        if resolved is not None:
            ok_type_for_encode = resolved
    try:
        return _EncodedNodeResult(
            status='COMPLETED' if result.is_ok() else 'FAILED',
            envelope=encode_task_result(result, ok_type_for_encode),
            result=result,
        )
    except Exception as exc:
        # The text of ``exc`` can come from user code, for example a
        # validator, so a lone surrogate in it is escaped: the encode of
        # the error refuses one.
        logger.error(
            'Failed to encode completion result for task=%s '
            'task_name=%s: %s; storing a serialization-error envelope',
            task_id,
            task_name,
            exc,
        )
        failure: TaskResult[Any, TaskError] = TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
                message=(
                    'Failed to encode completion result: '
                    f'{escape_lone_surrogates(str(exc))}'
                ),
                data={'task_id': task_id, 'task_name': task_name},
            ),
        )
        return _EncodedNodeResult(
            status='FAILED',
            envelope=encode_task_result(failure, type(None)),
            result=failure,
        )


async def apply_node_progression(
    session: AsyncSession,
    broker: PostgresBroker | None,
    *,
    workflow_id: str,
    task_index: int,
    failure: TaskResult[Any, TaskError] | None,
    on_error: str | None,
    workflow_status: str | None,
    depth: int | None,
    root_workflow_id: str | None,
    dependents: NodeDependents,
) -> None:
    """Everything a workflow does after one of its nodes goes terminal.

    Failure policy, the paused guard, dependent promotion, and the
    workflow-completion check — in that order, inside the caller's
    transaction, which must already hold the workflow row's lock.

    Both progression paths call this: the in-process completion, which
    has just CAS-written the node itself, and the outbox-driven recovery,
    where the database-owned consumption function wrote the node and
    returned this same context. ``failure`` is the task's error when the
    node went terminal unsuccessfully and ``None`` otherwise — the
    policy step is the only one that needs it. ``dependents`` is
    ``NodeHasNoDependents`` when no node of the workflow waits for the
    node: the dependents read is not sent, as it would return no row.
    """
    # 3. Handle failure based on on_error policy. The locked row's
    # on_error is authoritative: the held lock freezes the workflow row.
    if failure is not None:
        should_continue = await _handle_workflow_task_failure(
            session, workflow_id, failure,
            known_on_error=on_error,
        )
        if not should_continue:
            # PAUSE mode - stop processing, don't propagate to dependents
            return

    # 4. PAUSED guard.
    if pause_halts_progression(workflow_status):
        return  # Don't propagate - workflow is paused

    # 5. Find and potentially enqueue dependent tasks
    match dependents:
        case NodeHasNoDependents():
            pass
        case NodeMayHaveDependents():
            await _process_dependents(
                session,
                workflow_id,
                task_index,
                broker,
                depth=depth if depth is not None else 0,
                root_workflow_id=root_workflow_id or workflow_id,
            )
        case _ as unreachable:
            assert_never(unreachable)

    # 6. Check if workflow is complete (this transaction already holds the
    # workflow lock taken in step 2)
    await check_workflow_completion(session, workflow_id, broker, lock_held=True)
