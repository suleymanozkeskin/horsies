"""Deciding what a finished child means, before any of it is written down.

A child process reports back through two channels that predate this module: a
success flag and a free-text reason, plus a serialized result payload. Five of
those reason strings are not reasons at all — they are control-flow sentinels
telling the worker that finalization must not proceed, or must proceed
differently. Comparing them as strings at the point of use is what let a
sentinel be handled in one branch and missed in another.

Here the sentinels are parsed once, at the boundary, into `AbortReason`, and
every decision downstream is a match over types. The decision itself is data:
this module reads no database, opens no transaction, and writes nothing. It
answers what should happen; the persistence layer answers whether it still
can, because between the answer and the write the row can move.

Retry is the one decision that cannot be made here — eligibility depends on
the attempt count and deadline in the row, which is exactly the kind of read
this module refuses. So a task error yields `ScheduleAutomaticRetry`, and if
the policy declines, the caller asks this module again through
`terminalization_for_refused_retry`. Two calls rather than one command
carrying a spare copy of a task id that could disagree with the first.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import assert_never

from ..codec.error_payload import serialize_error_payload
from ..models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from .commands import (
    CancelOwnedOrphan,
    CompleteLockedTask,
    CompleteTaskFused,
    FailLockedTask,
    TerminalizationCommand,
)
from .fences import OwnedClaim, PriorLockedRead

_WORKFLOW_STOPPED = 'WORKFLOW_STOPPED'
_WORKER_FAILURE_FALLBACK = 'Worker failure'


class AbortReason(Enum):
    """A child stopped before producing a result, and said why.

    These travel on the wire as the failure-reason string. Parsing them here
    is what stops the spelling from being compared at four call sites.
    """

    CLAIM_LOST = 'CLAIM_LOST'
    OWNERSHIP_UNCONFIRMED = 'OWNERSHIP_UNCONFIRMED'
    WORKFLOW_STOPPED = _WORKFLOW_STOPPED
    WORKFLOW_CHECK_FAILED = 'WORKFLOW_CHECK_FAILED'
    TASK_EXPIRED = OutcomeCode.TASK_EXPIRED.value


def parse_abort_reason(failed_reason: str | None) -> AbortReason | None:
    """Read a wire reason as a sentinel, or None if it is prose.

    The single place a reason string is compared. Anything unrecognized is a
    worker-level failure whose text is the operator's only account of it, so
    it is passed through rather than rejected.
    """
    if failed_reason is None:
        return None
    try:
        return AbortReason(failed_reason)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# What the child reported
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AbortedBeforeResult:
    """The child stopped on a sentinel; there is no result to record."""

    reason: AbortReason


@dataclass(frozen=True, slots=True)
class WorkerLevelFailure:
    """The task never ran to a verdict and the worker itself reported why.

    Distinct from a task that failed: the payload here is written by the
    worker, not the task, and the detail is prose rather than a sentinel.
    """

    detail: str | None


@dataclass(frozen=True, slots=True)
class ResultProduced:
    """The child produced a success value that decoded."""


@dataclass(frozen=True, slots=True)
class ErrorProduced:
    """The child produced a typed task error that decoded."""

    error: TaskError


@dataclass(frozen=True, slots=True)
class ResultUndecodable:
    """A payload arrived and could not be read.

    The detail is the decode diagnosis, which becomes the recorded failure —
    the original bytes are unusable by definition, so this is all the operator
    will get.
    """

    detail: str


type ChildReport = (
    AbortedBeforeResult
    | WorkerLevelFailure
    | ResultProduced
    | ErrorProduced
    | ResultUndecodable
)


@dataclass(frozen=True, slots=True)
class FinalizeContext:
    """What the worker knows about the task it is finalizing.

    `claimed_at` is the generation this dispatch was handed. Carrying it into
    the decision keeps the fence built from the same value the claim used,
    rather than one re-read later and possibly newer.
    """

    task_id: str
    worker_id: str
    claimed_at: datetime | None
    is_workflow_task: bool
    queue_name: str
    result_json: str
    orphan_self_heal_enabled: bool


# ---------------------------------------------------------------------------
# What to do about it
# ---------------------------------------------------------------------------


class FinalizeNoOpReason(Enum):
    """Why finalization stops without writing anything.

    Each of these means another party owns the row's next state, so touching
    it would clobber a decision already made elsewhere.
    """

    CLAIM_LOST = 'CLAIM_LOST'
    OWNERSHIP_UNCONFIRMED = 'OWNERSHIP_UNCONFIRMED'
    WORKFLOW_STOPPED = 'WORKFLOW_STOPPED'
    EXPIRED_PLAIN_TASK = 'EXPIRED_PLAIN_TASK'
    ORPHAN_SELF_HEAL_DISABLED = 'ORPHAN_SELF_HEAL_DISABLED'


@dataclass(frozen=True, slots=True)
class NoTerminalAction:
    """Finalization stops here, and the reason is worth logging."""

    reason: FinalizeNoOpReason


@dataclass(frozen=True, slots=True)
class ScheduleAutomaticRetry:
    """A task error that retry policy should judge before anything terminal."""

    task_id: str
    error: TaskError


@dataclass(frozen=True, slots=True)
class ReplayWorkflowPhase2:
    """The terminal row already exists; only workflow progression is left.

    The child wrote the row itself, so phase 1 has nothing to do — but the
    node it belongs to still has to be advanced, or it sits enqueued against
    a terminal task until recovery notices.
    """

    task_id: str


@dataclass(frozen=True, slots=True)
class ApplyTerminalization:
    """Execute this command; the outcome says whether it still applied."""

    command: TerminalizationCommand


type FinalizeDecision = (
    NoTerminalAction
    | ScheduleAutomaticRetry
    | ReplayWorkflowPhase2
    | ApplyTerminalization
)


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def classify(report: ChildReport, context: FinalizeContext) -> FinalizeDecision:
    """Decide what a finished child's report calls for. Reads nothing."""
    match report:
        case AbortedBeforeResult(reason=reason):
            return _classify_abort(reason, context)
        case WorkerLevelFailure(detail=detail):
            return ApplyTerminalization(_worker_failure_command(detail, context))
        case ResultUndecodable(detail=detail):
            return ApplyTerminalization(_undecodable_command(detail, context))
        case ErrorProduced(error=error):
            if _is_workflow_stopped(error):
                return NoTerminalAction(reason=FinalizeNoOpReason.WORKFLOW_STOPPED)
            return ScheduleAutomaticRetry(task_id=context.task_id, error=error)
        case ResultProduced():
            return ApplyTerminalization(_success_command(context))
        case _ as unreachable:
            assert_never(unreachable)


def terminalization_for_refused_retry(
    error: TaskError,
    context: FinalizeContext,
) -> ApplyTerminalization:
    """The terminal form of a task error the retry policy declined.

    Separate from `classify` because the refusal is a database answer. The
    recorded payload is the child's own — the task's account of its failure
    survives the retry decision unchanged.
    """
    return ApplyTerminalization(
        FailLockedTask(
            task_id=context.task_id,
            fence=PriorLockedRead(worker_id=context.worker_id),
            result_json=context.result_json,
            error_code=_error_code_of(error),
            failed_reason=None,
        )
    )


def _classify_abort(
    reason: AbortReason,
    context: FinalizeContext,
) -> FinalizeDecision:
    match reason:
        case AbortReason.CLAIM_LOST:
            return NoTerminalAction(reason=FinalizeNoOpReason.CLAIM_LOST)
        case AbortReason.OWNERSHIP_UNCONFIRMED:
            return NoTerminalAction(
                reason=FinalizeNoOpReason.OWNERSHIP_UNCONFIRMED,
            )
        case AbortReason.WORKFLOW_STOPPED:
            return NoTerminalAction(reason=FinalizeNoOpReason.WORKFLOW_STOPPED)
        case AbortReason.TASK_EXPIRED:
            # The child already wrote the terminal row. A plain task is
            # finished; a workflow node still needs its phase 2.
            if not context.is_workflow_task:
                return NoTerminalAction(
                    reason=FinalizeNoOpReason.EXPIRED_PLAIN_TASK,
                )
            return ReplayWorkflowPhase2(task_id=context.task_id)
        case AbortReason.WORKFLOW_CHECK_FAILED:
            # The claim is held but the node linkage is gone, so this row can
            # never reach RUNNING. Cancelling it is the only progress
            # available; with self-heal off it is left claimed for inspection,
            # which the reaper also declines to touch.
            if not context.orphan_self_heal_enabled:
                return NoTerminalAction(
                    reason=FinalizeNoOpReason.ORPHAN_SELF_HEAL_DISABLED,
                )
            return ApplyTerminalization(
                CancelOwnedOrphan(
                    task_id=context.task_id,
                    fence=OwnedClaim(
                        worker_id=context.worker_id,
                        claimed_at=context.claimed_at,
                    ),
                )
            )
        case _ as unreachable:
            assert_never(unreachable)


def _success_command(context: FinalizeContext) -> TerminalizationCommand:
    """A plain task fuses; a workflow node cannot, because phase 2 follows.

    The fused statement ends the task and wakes queue capacity in one round
    trip, which is only sound when nothing further is owed on this row.
    """
    if context.is_workflow_task:
        return CompleteLockedTask(
            task_id=context.task_id,
            fence=PriorLockedRead(worker_id=context.worker_id),
            result_json=context.result_json,
        )
    queue_name = context.queue_name or 'default'
    return CompleteTaskFused(
        task_id=context.task_id,
        fence=OwnedClaim(
            worker_id=context.worker_id, claimed_at=context.claimed_at,
        ),
        result_json=context.result_json,
        notify_channel=f'task_queue_{queue_name}',
        notify_payload=f'capacity:{context.task_id}',
    )


def _worker_failure_command(
    detail: str | None,
    context: FinalizeContext,
) -> TerminalizationCommand:
    """The worker's own account of the failure, recorded as the result.

    The task produced nothing, so the payload written here is manufactured
    from the reason rather than passed through.
    """
    message = detail or _WORKER_FAILURE_FALLBACK
    error = TaskError(
        error_code=OperationalErrorCode.BROKER_ERROR,
        message=message,
        data={'task_id': context.task_id},
    )
    return FailLockedTask(
        task_id=context.task_id,
        fence=PriorLockedRead(worker_id=context.worker_id),
        result_json=serialize_error_payload(TaskResult(err=error)),
        error_code=OperationalErrorCode.BROKER_ERROR.value,
        failed_reason=message,
    )


def _undecodable_command(
    detail: str,
    context: FinalizeContext,
) -> TerminalizationCommand:
    """A payload that cannot be read is recorded as a serialization failure.

    No failure reason: the reason column belongs to worker-level failures, and
    a row can be carrying one from an earlier attempt that this transition has
    no business erasing.
    """
    error = TaskError(
        error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
        message=detail,
        data={'task_id': context.task_id},
    )
    return FailLockedTask(
        task_id=context.task_id,
        fence=PriorLockedRead(worker_id=context.worker_id),
        result_json=serialize_error_payload(TaskResult(err=error)),
        error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value,
        failed_reason=None,
    )


def _is_workflow_stopped(error: TaskError) -> bool:
    """The workflow-stop sentinel also travels inside a decoded payload."""
    return _error_code_of(error) == _WORKFLOW_STOPPED


def _error_code_of(error: TaskError) -> str | None:
    """The error code as it is written to the row.

    Task errors carry either a registered enum member or a caller's own
    string; the column stores one spelling for both.
    """
    code = error.error_code
    if isinstance(code, Enum):
        return str(code.value)
    return code
