"""Executing a terminalization command, from either driver, one way.

Both adapters build the same call from the same command and hand the row to
the same decoder. Neither interprets a column: a driver that decides what a
returned row means is a driver that can decide differently from the other one,
which is the failure this boundary exists to prevent.

The accepted type widens as operations land. That is deliberate — a dispatch
that accepts a command it cannot execute would need a fallthrough case, and a
fallthrough is what lets the sixteenth writer arrive unnoticed. Until an
operation exists in the database, its command is not accepted here, and the
type checker says so at the call site rather than at run time.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..logging import get_logger
from .commands import (
    AbandonNodesOfPausedWorkflows,
    AbandonOwnedNode,
    AbandonOwnedNodes,
    CancelLockedTask,
    CancelNodesOfCancelledWorkflow,
    CancelOrphanedTasks,
    CancelOwnedNode,
    CancelOwnedNodes,
    CancelOwnedOrphan,
    CompleteLockedTask,
    CompleteTaskFused,
    ExpireOwnedClaim,
    ExpirePendingTasks,
    FailLockedTask,
    FailStaleTask,
    TerminalizationCommand,
    fence_of,
)
from .fences import (
    CallerHoldsRowLock,
    OwnedClaim,
    OwnedClaimBatch,
    PriorLockedRead,
    TerminalFence,
    WorkerOwned,
)
from .operations import equivalence_class_of, function_name_of, kind_of
from .outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    SourceStateConflict,
    TaskAbsent,
    TerminalizationOutcome,
    decode_outcome_row,
)

logger = get_logger('lifecycle')

type ExecutableCommand = (
    CompleteLockedTask
    | CompleteTaskFused
    | FailLockedTask
    | FailStaleTask
    | ExpireOwnedClaim
    | CancelLockedTask
    | CancelOwnedOrphan
    | AbandonOwnedNode
    | CancelOwnedNode
)

# Batch operations return zero or more rows, so they cannot share the
# single-task adapters: exactly-one-row is a contract worth keeping honest
# for the operations it is true of.
type ExecutableBatchCommand = (
    ExpirePendingTasks
    | CancelOrphanedTasks
    | AbandonOwnedNodes
    | AbandonNodesOfPausedWorkflows
    | CancelOwnedNodes
    | CancelNodesOfCancelledWorkflow
)

# Identity parameters bind uuid-era casts: a running fleet only ever
# faces post-cutover identity columns, and the varchar-signature
# in-place program exists solely at drained moments (historical chain
# positions and the offline cutover), never under running callers.
_COMPLETE_LOCKED_TASK_SQL = text("""
    SELECT * FROM horsies_complete_locked_task(
        CAST(:task_id AS uuid), :worker_id, :result
    )
""")

_COMPLETE_TASK_FUSED_SQL = text("""
    SELECT * FROM horsies_complete_task_fused(
        CAST(:task_id AS uuid), :worker_id,
        CAST(:claimed_at AS TIMESTAMPTZ), :result,
        :notify_channel, :notify_payload
    )
""")

_FAIL_LOCKED_TASK_SQL = text("""
    SELECT * FROM horsies_fail_locked_task(
        CAST(:task_id AS uuid), :worker_id,
        :result, :error_code, :failed_reason
    )
""")

_FAIL_STALE_TASK_SQL = text("""
    SELECT * FROM horsies_fail_stale_task(
        CAST(:task_id AS uuid),
        CAST(:stale_after_ms AS INTEGER),
        CAST(:finalizing_stale_after_ms AS INTEGER),
        :result, :error_code, :failed_reason
    )
""")

_EXPIRE_OWNED_CLAIM_SQL = text("""
    SELECT * FROM horsies_expire_owned_claim(
        CAST(:task_id AS uuid), :worker_id, :result, :error_code
    )
""")

_EXPIRE_PENDING_TASKS_SQL = text("""
    SELECT * FROM horsies_expire_pending_tasks(
        CAST(:batch_size AS INTEGER), :result, :error_code
    )
""")

_CANCEL_LOCKED_TASK_SQL = text("""
    SELECT * FROM horsies_cancel_locked_task(
        CAST(:task_id AS uuid),
        CAST(:permitted_source_statuses AS TEXT[])
    )
""")

_CANCEL_OWNED_ORPHAN_SQL = text("""
    SELECT * FROM horsies_cancel_owned_orphan(
        CAST(:task_id AS uuid), :worker_id,
        CAST(:claimed_at AS TIMESTAMPTZ)
    )
""")

_CANCEL_ORPHANED_TASKS_SQL = text("""
    SELECT * FROM horsies_cancel_orphaned_tasks(
        CAST(:batch_size AS INTEGER)
    )
""")

_ABANDON_OWNED_NODE_SQL = text("""
    SELECT * FROM horsies_abandon_owned_node(
        CAST(:task_id AS uuid), :worker_id,
        CAST(:claimed_at AS TIMESTAMPTZ)
    )
""")

_ABANDON_OWNED_NODES_SQL = text("""
    SELECT * FROM horsies_abandon_owned_nodes(
        CAST(:ids AS uuid[]), CAST(:claimed_ats AS TIMESTAMPTZ[]),
        :worker_id
    )
""")

_ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL = text("""
    SELECT * FROM horsies_abandon_nodes_of_paused_workflows(
        CAST(:workflow_ids AS uuid[])
    )
""")

_CANCEL_OWNED_NODE_SQL = text("""
    SELECT * FROM horsies_cancel_owned_node(
        CAST(:task_id AS uuid), :worker_id,
        CAST(:claimed_at AS TIMESTAMPTZ),
        CAST(:accepts_requeued_pending AS BOOLEAN)
    )
""")

_CANCEL_OWNED_NODES_SQL = text("""
    SELECT * FROM horsies_cancel_owned_nodes(
        CAST(:ids AS uuid[]), CAST(:claimed_ats AS TIMESTAMPTZ[]),
        :worker_id
    )
""")

_CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL = text("""
    SELECT * FROM horsies_cancel_nodes_of_cancelled_workflow(
        CAST(:workflow_ids AS uuid[])
    )
""")

_LOCKED_READ_MISS_SQL = text("""
    SELECT * FROM horsies_terminalization_miss(
        CAST(:task_id AS uuid), CAST(:equivalent_kinds AS TEXT[]),
        :worker_id, CAST(:claimed_at AS TIMESTAMPTZ)
    )
""")

type LockedReadCommand = CompleteLockedTask | FailLockedTask


def call_for(command: ExecutableCommand) -> tuple[Any, dict[str, Any]]:
    """The statement and parameters that execute this command.

    Shared by both drivers so the two cannot diverge in what they send, only
    in how they await it.
    """
    match command:
        case CompleteLockedTask(task_id=task_id, fence=fence, result_json=result):
            return _COMPLETE_LOCKED_TASK_SQL, {
                'task_id': task_id,
                'worker_id': fence.worker_id,
                'result': result,
            }
        case CompleteTaskFused(
            task_id=task_id,
            fence=fence,
            result_json=result,
            notify_channel=channel,
            notify_payload=payload,
        ):
            return _COMPLETE_TASK_FUSED_SQL, {
                'task_id': task_id,
                'worker_id': fence.worker_id,
                'claimed_at': fence.claimed_at,
                'result': result,
                'notify_channel': channel,
                'notify_payload': payload,
            }
        case FailLockedTask(
            task_id=task_id,
            fence=fence,
            result_json=result,
            error_code=error_code,
            failed_reason=failed_reason,
        ):
            return _FAIL_LOCKED_TASK_SQL, {
                'task_id': task_id,
                'worker_id': fence.worker_id,
                'result': result,
                'error_code': error_code,
                'failed_reason': failed_reason,
            }
        case FailStaleTask(
            task_id=task_id,
            stale_after_ms=stale_after_ms,
            finalizing_stale_after_ms=finalizing_stale_after_ms,
            result_json=result,
            error_code=error_code,
            failed_reason=failed_reason,
        ):
            return _FAIL_STALE_TASK_SQL, {
                'task_id': task_id,
                'stale_after_ms': stale_after_ms,
                'finalizing_stale_after_ms': finalizing_stale_after_ms,
                'result': result,
                'error_code': error_code,
                'failed_reason': failed_reason,
            }
        case ExpireOwnedClaim(
            task_id=task_id,
            fence=fence,
            result_json=result,
            error_code=error_code,
        ):
            return _EXPIRE_OWNED_CLAIM_SQL, {
                'task_id': task_id,
                'worker_id': fence.worker_id,
                'result': result,
                'error_code': error_code,
            }
        case CancelLockedTask(
            task_id=task_id,
            permitted_source_statuses=permitted_source_statuses,
        ):
            return _CANCEL_LOCKED_TASK_SQL, {
                'task_id': task_id,
                'permitted_source_statuses': [
                    status.value for status in permitted_source_statuses
                ],
            }
        case CancelOwnedOrphan(task_id=task_id, fence=fence):
            return _CANCEL_OWNED_ORPHAN_SQL, {
                'task_id': task_id,
                'worker_id': fence.worker_id,
                'claimed_at': fence.claimed_at,
            }
        case AbandonOwnedNode(task_id=task_id, fence=fence):
            return _ABANDON_OWNED_NODE_SQL, {
                'task_id': task_id,
                'worker_id': fence.worker_id,
                'claimed_at': fence.claimed_at,
            }
        case CancelOwnedNode(
            task_id=task_id,
            fence=fence,
            accepts_requeued_pending=accepts_requeued_pending,
        ):
            return _CANCEL_OWNED_NODE_SQL, {
                'task_id': task_id,
                'worker_id': fence.worker_id,
                'claimed_at': fence.claimed_at,
                'accepts_requeued_pending': accepts_requeued_pending,
            }
        case _ as unreachable:
            assert_never(unreachable)


def batch_call_for(
    command: ExecutableBatchCommand,
) -> tuple[Any, dict[str, Any]]:
    """The statement and parameters that execute this batch command."""
    match command:
        case ExpirePendingTasks(
            batch_size=batch_size,
            result_json=result,
            error_code=error_code,
        ):
            return _EXPIRE_PENDING_TASKS_SQL, {
                'batch_size': batch_size,
                'result': result,
                'error_code': error_code,
            }
        case CancelOrphanedTasks(batch_size=batch_size):
            return _CANCEL_ORPHANED_TASKS_SQL, {
                'batch_size': batch_size,
            }
        case AbandonOwnedNodes(fence=fence):
            return _ABANDON_OWNED_NODES_SQL, {
                'ids': list(fence.task_ids()),
                'claimed_ats': list(fence.generations()),
                'worker_id': fence.worker_id,
            }
        case AbandonNodesOfPausedWorkflows(workflow_ids=workflow_ids):
            return _ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL, {
                'workflow_ids': list(workflow_ids),
            }
        case CancelOwnedNodes(fence=fence):
            return _CANCEL_OWNED_NODES_SQL, {
                'ids': list(fence.task_ids()),
                'claimed_ats': list(fence.generations()),
                'worker_id': fence.worker_id,
            }
        case CancelNodesOfCancelledWorkflow(workflow_ids=workflow_ids):
            return _CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL, {
                'workflow_ids': list(workflow_ids),
            }
        case _ as unreachable:
            assert_never(unreachable)


async def apply_batch_async(
    connection: AsyncConnection,
    command: ExecutableBatchCommand,
) -> list[TerminalizationOutcome]:
    """Execute a batch command and decode every row it reports.

    Zero rows is a valid answer — a discovery batch that found nothing
    eligible has nothing to report, and inventing an outcome for work that
    did not happen is what the row-per-transition contract exists to prevent.
    """
    statement, parameters = batch_call_for(command)
    result = await connection.execute(statement, parameters)
    outcomes = [
        decode_outcome_row({str(key): value for key, value in row.items()})
        for row in result.mappings().all()
    ]
    match command:
        case AbandonOwnedNodes(fence=fence) | CancelOwnedNodes(fence=fence):
            ordered_outcomes = _reconstruct_id_keyed_batch(
                outcomes,
                expected_count=len(fence.claim_generations),
                command=command,
            )
        case (
            ExpirePendingTasks()
            | CancelOrphanedTasks()
            | AbandonNodesOfPausedWorkflows()
            | CancelNodesOfCancelledWorkflow()
        ):
            ordered_outcomes = outcomes
        case _ as unreachable:
            assert_never(unreachable)
    for outcome in ordered_outcomes:
        _log_outcome(command, outcome)
    return ordered_outcomes


def _reconstruct_id_keyed_batch(
    outcomes: Sequence[TerminalizationOutcome],
    *,
    expected_count: int,
    command: AbandonOwnedNodes | CancelOwnedNodes,
) -> list[TerminalizationOutcome]:
    """Verify the ordinal contract and restore caller input order.

    SQL result order is never trusted, even though the functions order their
    rows. The exact ordinal set proves there is one answer per input and no
    duplicate answer before the adapter gives the outcomes to its caller.
    """
    by_ordinal: dict[int, TerminalizationOutcome] = {}
    for outcome in outcomes:
        if outcome.ordinality is None:
            raise RuntimeError(
                f'{type(command).__name__} returned a row without ordinality'
            )
        if outcome.ordinality in by_ordinal:
            raise RuntimeError(
                f'{type(command).__name__} returned duplicate ordinality '
                f'{outcome.ordinality}'
            )
        by_ordinal[outcome.ordinality] = outcome

    expected = set(range(1, expected_count + 1))
    actual = set(by_ordinal)
    if actual != expected:
        raise RuntimeError(
            f'{type(command).__name__} ordinal set does not match its input: '
            f'expected={sorted(expected)} actual={sorted(actual)}'
        )
    return [by_ordinal[ordinal] for ordinal in range(1, expected_count + 1)]


async def apply_async(
    connection: AsyncConnection,
    command: ExecutableCommand,
) -> TerminalizationOutcome:
    """Execute a command on the async driver and decode what it reports.

    The transaction stays the caller's: these functions never commit, so a
    coupled write belongs in the same transaction as the transition it proves.
    """
    statement, parameters = call_for(command)
    result = await connection.execute(statement, parameters)
    outcome = decode_outcome_row(_single_row(result.mappings().all(), command))
    _log_outcome(command, outcome)
    return outcome


async def classify_locked_read_miss_async(
    connection: AsyncConnection,
    command: LockedReadCommand,
    *,
    claimed_at: datetime | None,
) -> TerminalizationOutcome:
    """Classify a failed generation-fenced locking read without mutating.

    ``CompleteLockedTask`` and ``FailLockedTask`` intentionally carry only the
    worker half of their fence: their generation was already checked by the
    caller's ``SELECT ... FOR UPDATE``. If that read matched nothing, invoking
    the operation function would be unsafe — the same worker may already own a
    newer generation. The database's shared miss classifier accepts the full
    dispatched generation and distinguishes an idempotent replay from that
    lost claim while keeping terminal-before-fence ordering identical to every
    operation function.
    """
    requested_kind = kind_of(command)
    parameters = {
        'task_id': command.task_id,
        'equivalent_kinds': [
            kind.value
            for kind in sorted(
                equivalence_class_of(requested_kind),
                key=lambda member: member.value,
            )
        ],
        'worker_id': command.fence.worker_id,
        'claimed_at': claimed_at,
    }
    result = await connection.execute(_LOCKED_READ_MISS_SQL, parameters)
    outcome = decode_outcome_row(_single_row(result.mappings().all(), command))
    _log_outcome(
        command,
        outcome,
        expected_fence=OwnedClaim(
            worker_id=command.fence.worker_id,
            claimed_at=claimed_at,
        ),
    )
    return outcome


def apply_sync(
    cursor: Any,
    command: ExecutableCommand,
) -> TerminalizationOutcome:
    """Execute a command on the synchronous child-process driver.

    Takes a psycopg cursor rather than a connection because the child paths
    hold one open across their whole pre-start sequence, and the transaction
    they commit is the one this transition has to land in.
    """
    statement, parameters = call_for(command)
    cursor.execute(_as_psycopg(statement), parameters)
    columns = [description.name for description in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    outcome = decode_outcome_row(_single_row(rows, command))
    _log_outcome(command, outcome)
    return outcome


def _log_outcome(
    command: TerminalizationCommand,
    outcome: TerminalizationOutcome,
    *,
    expected_fence: TerminalFence | None = None,
) -> None:
    """Emit the operation, expected fence, and locked observation together.

    Applied transitions are debug-level steady-state traffic. Every refusal or
    replay is warning-level because its evidence is the only race diagnosis
    that cannot be reconstructed by reading the row later.
    """
    level = logging.DEBUG if isinstance(outcome, Applied) else logging.WARNING
    if not logger.isEnabledFor(level):
        return
    match outcome:
        case (
            Applied(
                observed=observed,
                terminal_at=terminal_at,
                kind=committed_kind,
            )
            | AlreadyApplied(
                observed=observed,
                terminal_at=terminal_at,
                kind=committed_kind,
            )
        ):
            evidence: object | None = None
        case LostClaim(observed=observed):
            terminal_at = None
            committed_kind = None
            evidence = None
        case SourceStateConflict(observed=observed, evidence=guard_evidence):
            terminal_at = None
            committed_kind = None
            evidence = guard_evidence
        case TaskAbsent():
            observed = None
            terminal_at = None
            committed_kind = None
            evidence = None
        case _ as unreachable:
            assert_never(unreachable)
    command_fence = fence_of(command)
    match expected_fence:
        case (
            CallerHoldsRowLock()
            | PriorLockedRead()
            | WorkerOwned()
            | OwnedClaim()
            | OwnedClaimBatch()
        ):
            fence_type = type(expected_fence).__name__
            logged_expected_fence = expected_fence
        case None:
            match command_fence:
                case OwnedClaimBatch(
                    worker_id=worker_id,
                    claim_generations=claims,
                ):
                    generation_by_task = dict(claims)
                    fence_type = OwnedClaimBatch.__name__
                    logged_expected_fence: object | None = {
                        'worker_id': worker_id,
                        'claimed_at': generation_by_task.get(outcome.task_id),
                    }
                case None:
                    fence_type = None
                    logged_expected_fence = None
                case (
                    CallerHoldsRowLock()
                    | PriorLockedRead()
                    | WorkerOwned()
                    | OwnedClaim()
                ):
                    fence_type = type(command_fence).__name__
                    logged_expected_fence = command_fence
    logger.log(
        level,
        'terminalization operation=%s function=%s outcome=%s task_id=%s '
        'terminal_at=%r committed_kind=%s fence_type=%s expected_fence=%r '
        'observed=%r evidence=%r',
        type(command).__name__,
        function_name_of(command),
        type(outcome).__name__,
        outcome.task_id,
        terminal_at,
        committed_kind.value if committed_kind is not None else None,
        fence_type,
        logged_expected_fence,
        observed,
        evidence,
    )


def _single_row(
    rows: Sequence[Any],
    command: ExecutableCommand,
) -> Mapping[str, Any]:
    """A single-task operation reports exactly one row, always.

    Even a task that does not exist produces one — absence is an outcome, not
    an empty result. No rows means the function did not honour its contract,
    which is not something to paper over with a default.
    """
    if len(rows) != 1:
        raise RuntimeError(
            f'{type(command).__name__} returned {len(rows)} rows; every '
            f'single-task operation reports exactly one outcome'
        )
    return rows[0]


# Named-parameter syntax differs between the drivers; the call does not.
# The lookbehind leaves `::` casts alone, and matching every `:name` at once
# avoids the trap a hand-kept name list carries: two parameters where one is
# a prefix of the other would corrupt the longer one if replaced first.
_NAMED_PARAMETER_RE = re.compile(r'(?<!:):(\w+)')


def _as_psycopg(statement: Any) -> str:
    return _NAMED_PARAMETER_RE.sub(r'%(\1)s', str(statement))
