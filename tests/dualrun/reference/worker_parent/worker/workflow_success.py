"""The success of a RUNNING workflow task: one statement.

Phase 1 of a workflow task's success writes through
``horsies_finish_workflow_success``. The function takes the task advisory
lock, then the row lock under the full claim fence (worker and claimed_at),
writes the COMPLETED attempt row from the locked row, and moves the task to
history as COMPLETED with the phase-2 record of its node. It sends no
capacity wake: phase 2 sends it, after the dependents of the node are
enqueued.

The statement runs alone in autocommit (``run_in_autocommit``): one round
trip, plus the pool pre-ping when the engine has it on. Phase 2 stays a
separate transaction, so a phase-2 error never undoes phase 1.

The worker sends the statement through the core
(``finish_workflow_success_in_core``, ``CoreHandle.finish_workflow_success_async``),
and the core's record becomes the same outcome types.
``finish_workflow_success`` stays for the dual-run reference of the worker's
caller.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Final, assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import horsies._core as core
from horsies.core.history.terminalization.move import (
    FINISH_WORKFLOW_SUCCESS_FUNCTION,
)
from horsies.core.lifecycle.core_records import (
    already_applied_of,
    lost_claim_of,
    observed_of,
    phase_two_owed_of,
    source_state_conflict_of,
    task_absent_of,
)
from horsies.core.lifecycle.commands import CompleteLockedTask
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    LostClaim,
    SourceStateConflict,
    SuccessApplied,
    TaskAbsent,
    WorkflowSuccessOutcome,
    decode_workflow_success_row,
)
from horsies.core.logging import get_logger
from horsies.core.types.core_failure import CoreOperationFailed
from horsies.core.utils.db import SingleStatement, run_in_autocommit

logger = get_logger('lifecycle')

FINISH_WORKFLOW_SUCCESS_SQL = text(f"""
    SELECT * FROM {FINISH_WORKFLOW_SUCCESS_FUNCTION}(
        CAST(:task_id AS uuid), :worker_id,
        CAST(:claimed_at AS TIMESTAMPTZ), :result
    )
""")


@dataclass(frozen=True, slots=True)
class FinishWorkflowSuccess:
    """The success of a RUNNING workflow task, for one statement.

    ``fence`` is the full claim generation: the function checks it under
    the row lock. ``result_json`` is the encoded result of the task.
    """

    task_id: str
    fence: OwnedClaim
    result_json: str


def success_for(completion: CompleteLockedTask) -> FinishWorkflowSuccess:
    """The one-statement success of a locked completion, fenced on the claim
    that the completion carries. Pure."""
    return FinishWorkflowSuccess(
        task_id=completion.task_id,
        fence=completion.fence,
        result_json=completion.result_json,
    )


def success_statement(command: FinishWorkflowSuccess) -> SingleStatement:
    """The statement and parameters of ``command``. Pure."""
    return SingleStatement(
        statement=FINISH_WORKFLOW_SUCCESS_SQL,
        parameters={
            'task_id': command.task_id,
            'worker_id': command.fence.worker_id,
            'claimed_at': command.fence.claimed_at,
            'result': command.result_json,
        },
    )


async def finish_workflow_success(
    session_factory: async_sessionmaker[AsyncSession],
    command: FinishWorkflowSuccess,
) -> WorkflowSuccessOutcome:
    """Run the success of a workflow task as its own transaction and decode
    what it reports.

    ``SuccessApplied`` is committed when this returns, and phase 2 is owed.
    A refusal (``AlreadyApplied``, ``LostClaim``, ``SourceStateConflict``,
    ``TaskAbsent``) wrote nothing. A lost reply is an unknown outcome: a
    second call of the same command classifies a committed success as
    ``AlreadyApplied``.

    Raises:
        OutcomeDecodeError: the row does not satisfy the wire contract.
        RuntimeError: the function returned other than one row.
        Exception: what ``run_in_autocommit`` raises: a pool timeout, a
            connection error, or an error of the function (a NULL result,
            a locked row that is not a workflow task).
    """
    rows = (
        await run_in_autocommit(session_factory, success_statement(command))
    ).mappings()
    if len(rows) != 1:
        raise RuntimeError(
            f'{FINISH_WORKFLOW_SUCCESS_FUNCTION} returned {len(rows)} rows for '
            f'task {command.task_id}; it reports exactly one outcome'
        )
    outcome = decode_workflow_success_row(rows[0])
    _log_outcome(command, outcome)
    return outcome


def _log_outcome(
    command: FinishWorkflowSuccess, outcome: WorkflowSuccessOutcome
) -> None:
    """Log the outcome with the fence it was asked for.

    A write is debug-level. A refusal or a replay is warning-level: its
    observation cannot be read again later.
    """
    match outcome:
        case SuccessApplied():
            level = logging.DEBUG
        case AlreadyApplied() | LostClaim() | SourceStateConflict() | TaskAbsent():
            level = logging.WARNING
        case _ as unreachable:
            assert_never(unreachable)
    if not logger.isEnabledFor(level):
        return
    logger.log(
        level,
        'workflow success function=%s outcome=%r expected_fence=%r',
        FINISH_WORKFLOW_SUCCESS_FUNCTION,
        outcome,
        command.fence,
    )


WORKFLOW_SUCCESS_SUBJECT: Final = 'finish_workflow_success'
"""The subject of a ``CoreOperationFailed`` of a workflow success."""


def _workflow_success_outcome_of(
    record: core.WorkflowSuccessApplied
    | core.AttemptAlreadyApplied
    | core.AttemptLostClaim
    | core.AttemptSourceStateConflict
    | core.AttemptTaskAbsent
    | core.AttemptWriteFailed,
) -> WorkflowSuccessOutcome:
    """The outcome of a workflow success record. Pure.

    Raises:
        CoreOperationFailed: an ``AttemptWriteFailed`` record: the statement
            wrote nothing, or it committed and its reply was lost.
        OutcomeDecodeError: a text of the record outside the Python catalogs.
    """
    match record:
        case core.WorkflowSuccessApplied():
            return SuccessApplied(
                task_id=str(record.task_id),
                terminal_at=record.terminal_at,
                observed=observed_of(
                    record.observed_status,
                    record.observed_worker_id,
                    record.observed_claimed_at,
                ),
                phase_two=phase_two_owed_of(record.phase_two),
            )
        case core.AttemptAlreadyApplied():
            return already_applied_of(record)
        case core.AttemptLostClaim():
            return lost_claim_of(record)
        case core.AttemptSourceStateConflict():
            return source_state_conflict_of(record)
        case core.AttemptTaskAbsent():
            return task_absent_of(record)
        case core.AttemptWriteFailed(message=message, retryable=retryable):
            raise CoreOperationFailed(WORKFLOW_SUCCESS_SUBJECT, message, retryable)
        case _ as unreachable:
            assert_never(unreachable)


async def finish_workflow_success_in_core(
    handle: core.CoreHandle,
    command: FinishWorkflowSuccess,
) -> WorkflowSuccessOutcome:
    """``finish_workflow_success`` through the core: one statement that
    commits alone, the same outcomes and the same log.

    A task id that is not a UUID is a defect of the claim that gave it:
    ``ValueError``.

    Raises:
        CoreOperationFailed: the statement failed (``retryable`` is the
            core's classification). A lost reply is an unknown outcome: a
            second call classifies a committed success as ``AlreadyApplied``.
        CoreError: the core of this process cannot run the call.
        OutcomeDecodeError: a record outside the Python catalogs.
    """
    record = await handle.finish_workflow_success_async(
        uuid.UUID(command.task_id),
        worker_id=command.fence.worker_id,
        claimed_at=command.fence.claimed_at,
        result_json=command.result_json,
    )
    outcome = _workflow_success_outcome_of(record)
    _log_outcome(command, outcome)
    return outcome
