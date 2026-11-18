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

import json
import logging
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Final, LiteralString, assert_never, cast

from psycopg import AsyncCursor
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

import horsies._core as core

from ..history.terminalization.move import TASK_LOCK_SEED
from ..logging import get_logger
from ..types.result import Err, Ok, Result
from ..utils.db import (
    AutocommitFailure,
    NotSent,
    OutcomeUnknown,
    SingleStatement,
    StatementRefused,
    StatementRunner,
    is_retryable_connection_error,
    run_in_autocommit,
)
from .commands import (
    AbandonNodesOfPausedWorkflows,
    AbandonOwnedNode,
    AbandonOwnedNodes,
    CancelNodesOfCancelledWorkflow,
    CancelOrphanedTasks,
    CancelOwnedNode,
    CancelOwnedNodes,
    CancelOwnedOrphan,
    CompleteTaskFused,
    ExpireOwnedClaim,
    ExpirePendingTasks,
    TerminalizationCommand,
    fence_of,
)
from .fences import (
    OwnedClaim,
    OwnedClaimBatch,
    TerminalFence,
    WorkerOwned,
)
from .core_records import (
    already_applied_of,
    lost_claim_of,
    observed_of,
    source_state_conflict_of,
    task_absent_of,
)
from .operations import TerminalizationKind, function_name_of
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
    CompleteTaskFused
    | ExpireOwnedClaim
    | CancelOwnedOrphan
    | AbandonOwnedNode
    | CancelOwnedNode
)

# The commands that are complete in one statement: the function checks the
# full claim generation itself, writes the coupled rows (attempt, history,
# capacity NOTIFY) in the same call, and writes nothing when it refuses. Only
# these may run in autocommit.
type AutocommitCommand = CompleteTaskFused | CancelOwnedOrphan

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
#
# The last argument (wake) is TRUE: the function sends the capacity wake of
# the locked row's queue.
_COMPLETE_TASK_FUSED_SQL = text("""
    SELECT * FROM horsies_complete_task_fused(
        CAST(:task_id AS uuid), :worker_id,
        CAST(:claimed_at AS TIMESTAMPTZ), :result, TRUE
    )
""")

# The batch arrives as one JSON parameter (``_fused_batch_parameters``): one
# array per command, holding the task id, the claim generation and the result
# of its row. Each ARRAY keeps the input order, so the arguments of one
# command share one index. The worker id is one parameter: a batch belongs to
# one worker.
_COMPLETE_TASKS_FUSED_SQL = text("""
    WITH batch AS MATERIALIZED (
        SELECT entry.value AS fields, entry.ordinality AS input_order
        FROM jsonb_array_elements(CAST(:batch AS jsonb))
            WITH ORDINALITY AS entry(value, ordinality)
    )
    SELECT * FROM horsies_complete_tasks_fused(
        ARRAY(SELECT CAST(fields->>0 AS uuid) FROM batch ORDER BY input_order),
        ARRAY(SELECT CAST(fields->>1 AS TIMESTAMPTZ) FROM batch ORDER BY input_order),
        ARRAY(SELECT fields->>2 FROM batch ORDER BY input_order),
        :worker_id,
        TRUE
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

# The task advisory lock, by the key every move function takes. A caller
# whose transaction row-locks a task and then calls a move function runs this
# first, in the same transaction: the advisory lock goes before any row lock
# of the task. It waits while another transaction holds the lock, and is held
# until the caller's transaction ends; on an autocommit connection it would
# end with the statement, so it is used only inside a transaction.
LOCK_TASK_SQL = text(f"""
    SELECT pg_advisory_xact_lock(
        hashtextextended(CAST(CAST(:task_id AS uuid) AS text), {TASK_LOCK_SEED})
    )
""")


def call_for(command: ExecutableCommand) -> tuple[Any, dict[str, Any]]:
    """The statement and parameters that execute this command.

    Shared by both drivers so the two cannot diverge in what they send, only
    in how they await it.
    """
    match command:
        case CompleteTaskFused(task_id=task_id, fence=fence, result_json=result):
            return _COMPLETE_TASK_FUSED_SQL, {
                'task_id': task_id,
                'worker_id': fence.worker_id,
                'claimed_at': fence.claimed_at,
                'result': result,
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
                operation=type(command).__name__,
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
    operation: str,
) -> list[TerminalizationOutcome]:
    """Verify the ordinal contract and restore caller input order.

    SQL result order is never trusted, even though the functions order their
    rows. The exact ordinal set proves there is one answer per input and no
    duplicate answer before the adapter gives the outcomes to its caller.
    ``operation`` names the batch in the error.
    """
    by_ordinal: dict[int, TerminalizationOutcome] = {}
    for outcome in outcomes:
        if outcome.ordinality is None:
            raise RuntimeError(f'{operation} returned a row without ordinality')
        if outcome.ordinality in by_ordinal:
            raise RuntimeError(
                f'{operation} returned duplicate ordinality {outcome.ordinality}'
            )
        by_ordinal[outcome.ordinality] = outcome

    expected = set(range(1, expected_count + 1))
    actual = set(by_ordinal)
    if actual != expected:
        raise RuntimeError(
            f'{operation} ordinal set does not match its input: '
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


async def apply_in_autocommit(
    session_factory: async_sessionmaker[AsyncSession],
    command: AutocommitCommand,
) -> TerminalizationOutcome:
    """Execute a one-statement command as its own transaction.

    The call runs through ``run_in_autocommit``: one round trip with the
    pool pre-ping off. ``Applied`` is committed when this returns. A refusal
    (``AlreadyApplied``, ``LostClaim``, ``SourceStateConflict``,
    ``TaskAbsent``) wrote nothing, so there is nothing to roll back.

    Raises:
        Exception: what ``run_in_autocommit`` raises. A connection error
            after the call was sent leaves the outcome unknown; a replay of
            the same command classifies it (``AlreadyApplied`` when it
            committed).
    """
    statement, parameters = call_for(command)
    rows = await run_in_autocommit(
        session_factory,
        SingleStatement(statement=statement, parameters=parameters),
    )
    outcome = decode_outcome_row(_single_row(rows.mappings(), command))
    _log_outcome(command, outcome)
    return outcome


FUSED_BATCH_MAX_ROWS: Final = 64
"""The most rows of one batched plain-success statement. Each row holds its
task's advisory lock and row lock until the statement ends, so this bounds the
locks and the server time of one statement."""


@dataclass(frozen=True, slots=True)
class FusedCompletionBatch:
    """Plain-success completions that one statement writes, in input order.

    Each command keeps its own fence and result: the statement
    (``horsies_complete_tasks_fused``) runs ``horsies_complete_task_fused``
    once per row, in sorted task-id order. The commands of one batch belong
    to one worker: the statement takes one worker id.

    Raises:
        ValueError: no command, more than ``FUSED_BATCH_MAX_ROWS``, a task
            id twice, or commands of more than one worker. The statement
            refuses a repeated id; a repeat means two writes of one row in one
            statement.
    """

    commands: tuple[CompleteTaskFused, ...]

    def __post_init__(self) -> None:
        if not 1 <= len(self.commands) <= FUSED_BATCH_MAX_ROWS:
            raise ValueError(
                f'a fused completion batch holds 1 to {FUSED_BATCH_MAX_ROWS} '
                f'commands, got {len(self.commands)}'
            )
        task_ids = [command.task_id for command in self.commands]
        if len(set(task_ids)) != len(task_ids):
            raise ValueError(
                f'a fused completion batch names a task id twice: {task_ids}'
            )
        worker_ids = {command.fence.worker_id for command in self.commands}
        if len(worker_ids) != 1:
            raise ValueError(
                f'a fused completion batch belongs to one worker, got '
                f'{sorted(worker_ids)}'
            )

    @property
    def worker_id(self) -> str:
        """The one worker of the batch."""
        return self.commands[0].fence.worker_id


COMPLETE_SUCCESSES_SUBJECT: Final = 'CoreHandle.complete_successes_async'
"""The core call of the plain-success batch, named in a contract break of
its rows."""


@dataclass(frozen=True, slots=True)
class CoreNotSent:
    """The core did not send the batch statement: its pools, its client
    family check or its checkout failed (the record ``BatchNotSent``).
    ``error`` is the record's exception: the core's class of the cause,
    with its message and retry classification."""

    error: core.CoreFailure


@dataclass(frozen=True, slots=True)
class CoreStatementRefused:
    """The server refused the batch statement that the core sent (the
    record ``BatchRefused``); one row can be the cause. ``error`` is the
    record's exception: the server's error, with its SQLSTATE and retry
    classification."""

    error: core.CoreFailure


@dataclass(frozen=True, slots=True)
class CoreOutcomeUnknown:
    """The reply of the batch statement that the core sent was lost (the
    record ``BatchCommitUnknown``). ``error`` is the record's exception:
    no answer from the server, or the server's error that ended the
    session."""

    error: core.CoreFailure


@dataclass(frozen=True, slots=True)
class BatchRolledBack:
    """No row of the batch changed.

    ``cause`` is ``NotSent`` or ``CoreNotSent`` (the statement was not sent:
    the pool, the connect or the checkout failed), or ``StatementRefused`` or
    ``CoreStatementRefused`` (the driver or the server refused the statement;
    a row that raises refuses the whole statement).
    """

    cause: NotSent | StatementRefused | CoreNotSent | CoreStatementRefused

    @property
    def retryable(self) -> bool:
        """Whether the cause may clear: a connection error, a pool timeout,
        or a transient server error such as a deadlock. A core cause carries
        the core's classification."""
        return is_retryable_connection_error(self.cause.error)


@dataclass(frozen=True, slots=True)
class BatchCommitUnknown:
    """The connection broke after the statement was sent. Each row may have
    committed or not; a replay of a row classifies it (``AlreadyApplied``
    when it committed)."""

    cause: OutcomeUnknown | CoreOutcomeUnknown


type BatchWriteError = BatchRolledBack | BatchCommitUnknown


def _batch_write_error(failure: AutocommitFailure) -> BatchWriteError:
    """The batch error of a failed autocommit run. Pure."""
    match failure:
        case NotSent() | StatementRefused():
            return BatchRolledBack(cause=failure)
        case OutcomeUnknown():
            return BatchCommitUnknown(cause=failure)
        case _ as unreachable:
            assert_never(unreachable)


def _fused_batch_parameters(batch: FusedCompletionBatch) -> dict[str, object]:
    """The parameters of the batch statement: the worker id, and a JSON
    array with one array per command, in input order. Each holds the task
    id, the claim generation and the result of its row, as text. Pure.

    The driver sends one string, not three arrays that it dumps element by
    element. Every value is a ``str``, so ``json.dumps`` has no value to
    refuse; ``ensure_ascii=False`` keeps each string as it is, apart from the
    escapes of quotes, backslashes and control characters.
    """
    return {
        'batch': json.dumps(
            [
                [
                    command.task_id,
                    _generation_text(command.fence.claimed_at),
                    command.result_json,
                ]
                for command in batch.commands
            ],
            ensure_ascii=False,
            allow_nan=False,
            separators=(',', ':'),
        ),
        'worker_id': batch.worker_id,
    }


def _generation_text(claimed_at: datetime) -> str:
    """The claim generation as ISO 8601 text with its microseconds and its
    UTC offset. Pure."""
    return claimed_at.isoformat()


async def apply_fused_batch(
    runner: StatementRunner,
    batch: FusedCompletionBatch,
) -> Result[tuple[TerminalizationOutcome, ...], BatchWriteError]:
    """Write a batch of plain-success completions as one statement.

    The worker writes through the core (``complete_fused_batch_in_core``);
    this stays for the dual-run reference of the worker's caller and for the
    tests of the statement.

    One round trip in autocommit on ``runner``: a checkout per call
    (``PooledStatements``) or a held connection (``HeldConnection``). A batch of
    one row runs ``horsies_complete_task_fused`` itself; a batch of several
    runs ``horsies_complete_tasks_fused``, which calls that function once per
    row. The statement is atomic: every ``Applied`` row committed with it,
    and a refused row (``AlreadyApplied``, ``LostClaim``,
    ``SourceStateConflict``, ``TaskAbsent``) wrote nothing.

    Outcomes:
        Ok(outcomes): one outcome per command, in input order; the outcome at
            position i is the outcome of the task of command i.
        Err(BatchRolledBack): no row changed.
        Err(BatchCommitUnknown): the commit of each row is unknown.

    Raises:
        OutcomeDecodeError, RuntimeError: the rows break the contract of the
            statement (a row that does not decode, a missing or repeated
            ordinality, an outcome for another task). These are defects; the
            rows of the statement committed or wrote nothing.
    """
    match await runner.run(_fused_statement(batch)):
        case Ok(rows):
            pass
        case Err(failure):
            return Err(_batch_write_error(failure))
    outcomes = _fused_outcomes(batch, rows.mappings())
    for command, outcome in zip(batch.commands, outcomes, strict=True):
        if outcome.task_id != command.task_id:
            raise RuntimeError(
                f'{FusedCompletionBatch.__name__} answered task '
                f'{outcome.task_id} at the position of task {command.task_id}'
            )
        _log_outcome(command, outcome)
    return Ok(tuple(outcomes))


async def complete_fused_batch_in_core(
    handle: core.CoreHandle,
    batch: FusedCompletionBatch,
) -> Result[tuple[TerminalizationOutcome, ...], BatchWriteError]:
    """``apply_fused_batch`` through the core: one statement
    (``CoreHandle.complete_successes_async``), the same outcomes and the
    same log.

    The core sends ``horsies_complete_tasks_fused`` for every batch, also a
    batch of one row; that function runs ``horsies_complete_task_fused``
    once per row. The outcome of a batch of one row has no ordinality, as
    ``apply_fused_batch`` gives it; the outcome of a batch of several rows
    has its 1-based position. A task id that is not a UUID is a defect of
    the claim that gave it: ``ValueError``.

    Outcomes:
        Ok(outcomes): one outcome per command, in input order.
        Err(BatchRolledBack(CoreNotSent)): the statement was not sent; no
            row changed.
        Err(BatchRolledBack(CoreStatementRefused)): the server refused the
            statement; no row changed.
        Err(BatchCommitUnknown(CoreOutcomeUnknown)): the commit of each row
            is unknown.

    Raises:
        CoreError: the core of this process cannot run the call.
        OutcomeDecodeError: a record outside the Python catalogs.
        RuntimeError: an outcome of another task at the position of a
            command, or a count other than the batch. These are defects.
    """
    request = core.SuccessBatchRequest(
        worker_id=batch.worker_id,
        rows=[
            (uuid.UUID(command.task_id), command.fence.claimed_at, command.result_json)
            for command in batch.commands
        ],
    )
    record = await handle.complete_successes_async(request)
    match record:
        case core.SuccessesWritten():
            pass
        case core.BatchNotSent(exception=exception):
            return Err(BatchRolledBack(cause=CoreNotSent(error=exception)))
        case core.BatchRefused(exception=exception):
            return Err(BatchRolledBack(cause=CoreStatementRefused(error=exception)))
        case core.BatchCommitUnknown(exception=exception):
            return Err(BatchCommitUnknown(cause=CoreOutcomeUnknown(error=exception)))
        case _ as unreachable:
            assert_never(unreachable)
    rows = record.rows
    if len(rows) != len(batch.commands):
        raise RuntimeError(
            f'{COMPLETE_SUCCESSES_SUBJECT} answered {len(rows)} rows for a batch '
            f'of {len(batch.commands)}'
        )
    outcomes: list[TerminalizationOutcome] = []
    for position, (command, row) in enumerate(zip(batch.commands, rows, strict=True), start=1):
        outcome = replace(
            _success_row_outcome(row),
            ordinality=None if len(batch.commands) == 1 else position,
        )
        if outcome.task_id != command.task_id:
            raise RuntimeError(
                f'{COMPLETE_SUCCESSES_SUBJECT} answered task {outcome.task_id} at '
                f'the position of task {command.task_id}'
            )
        _log_outcome(command, outcome)
        outcomes.append(outcome)
    return Ok(tuple(outcomes))


def _success_row_outcome(
    row: core.SuccessApplied
    | core.AttemptAlreadyApplied
    | core.AttemptLostClaim
    | core.AttemptSourceStateConflict
    | core.AttemptTaskAbsent,
) -> TerminalizationOutcome:
    """The outcome of one row record of a batch, with no ordinality. Pure.

    Raises:
        OutcomeDecodeError: a record outside the Python catalogs.
    """
    match row:
        case core.SuccessApplied():
            return Applied(
                task_id=str(row.task_id),
                ordinality=None,
                terminal_at=row.terminal_at,
                kind=TerminalizationKind.COMPLETE_FUSED,
                observed=observed_of(
                    row.observed_status, row.observed_worker_id, row.observed_claimed_at,
                ),
            )
        case core.AttemptAlreadyApplied():
            return already_applied_of(row)
        case core.AttemptLostClaim():
            return lost_claim_of(row)
        case core.AttemptSourceStateConflict():
            return source_state_conflict_of(row)
        case core.AttemptTaskAbsent():
            return task_absent_of(row)
        case _ as unreachable:
            assert_never(unreachable)


def _fused_statement(batch: FusedCompletionBatch) -> SingleStatement:
    """The statement of ``batch``: the single fused completion for one row,
    the batch function for several. Pure."""
    match batch.commands:
        case (command,):
            statement, parameters = call_for(command)
            return SingleStatement(statement=statement, parameters=parameters)
        case _:
            return SingleStatement(
                statement=_COMPLETE_TASKS_FUSED_SQL,
                parameters=_fused_batch_parameters(batch),
            )


def _fused_outcomes(
    batch: FusedCompletionBatch,
    rows: Sequence[Mapping[str, Any]],
) -> list[TerminalizationOutcome]:
    """Decode the rows of the statement of ``batch``, in input order.

    The single fused completion answers one row with no ordinality; the
    batch function answers one row per input at its ordinality.
    """
    match batch.commands:
        case (command,):
            return [decode_outcome_row(_single_row(rows, command))]
        case _:
            return _reconstruct_id_keyed_batch(
                [decode_outcome_row(row) for row in rows],
                expected_count=len(batch.commands),
                operation=FusedCompletionBatch.__name__,
            )


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


async def apply_psycopg_async(
    cursor: AsyncCursor[Any],
    command: ExecutableCommand,
) -> TerminalizationOutcome:
    """Execute a command on the async psycopg driver of a shared async child.

    The async twin of ``apply_sync``: the same call, the same decoder, on a
    cursor whose connection runs on the child's event loop. The transaction
    stays the caller's.
    """
    statement, parameters = call_for(command)
    # The text comes from the statement constants of this module, never from
    # input, so it is a literal in all but its type.
    query = cast('LiteralString', _as_psycopg(statement))
    await cursor.execute(query, parameters)
    description = cursor.description
    assert description is not None, 'an operation function returns a row'
    columns = [column.name for column in description]
    rows = [dict(zip(columns, row)) for row in await cursor.fetchall()]
    outcome = decode_outcome_row(_single_row(rows, command))
    _log_outcome(command, outcome)
    return outcome


def _log_outcome(
    command: TerminalizationCommand,
    outcome: TerminalizationOutcome,
) -> None:
    """Emit the operation, its fence, and the locked observation together.

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
    fence_type, logged_expected_fence = _logged_fence(
        fence_of(command),
        outcome.task_id,
    )
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


def _logged_fence(
    fence: TerminalFence | None,
    task_id: str,
) -> tuple[str | None, object | None]:
    """The fence of a command as the outcome log shows it: its type name and
    its value. A batch fence shows only the worker and the claim generation
    of ``task_id``. Pure."""
    match fence:
        case OwnedClaimBatch(worker_id=worker_id, claim_generations=claims):
            return OwnedClaimBatch.__name__, {
                'worker_id': worker_id,
                'claimed_at': dict(claims).get(task_id),
            }
        case None:
            return None, None
        case WorkerOwned() | OwnedClaim():
            return type(fence).__name__, fence
        case _ as unreachable:
            assert_never(unreachable)


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
