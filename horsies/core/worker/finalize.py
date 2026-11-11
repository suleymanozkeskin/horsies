"""Task finalization: persist terminal state, workflow phase, retries.

FinalizeMixin owns the two-phase finalize pipeline: await the slot
outcome (with optional timeout), persist the terminal row + attempt
history (phase 1), run the workflow completion phase (phase 2), and the
bounded retry machinery for failures in any stage. Worker-internal
mixin: the ``TYPE_CHECKING`` block declares the slice of ``Worker`` it
relies on.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional, assert_never, cast

from sqlalchemy import text

from horsies.core.codec.json_io import SerializationError, loads_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.payload_guard import warn_if_payload_large
from horsies.core.codec.typed import (
    decode_task_error,
    decode_task_result,
    validate_task_result_envelope,
)
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.history.identity.uuid7 import UUID
from horsies.core.history.reads.detail import (
    HistoryTaskDetail,
    read_task_detail,
    staged_detail_published,
)
from horsies.core.lifecycle.classify import (
    AbortedBeforeResult,
    ApplyTerminalization,
    ErrorProduced,
    FinalizeContext,
    NoTerminalAction,
    ReplayWorkflowPhase2,
    ResultProduced,
    ResultUndecodable,
    ScheduleAutomaticRetry,
    WorkerLevelFailure,
    classify,
    parse_abort_reason,
    terminalization_for_refused_retry,
)
from horsies.core.lifecycle.commands import (
    CancelOwnedOrphan,
    CompleteLockedTask,
    CompleteTaskFused,
    FailLockedTask,
)
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    CapacityWoken,
    FailedAttemptOutcome,
    FailureApplied,
    LostClaim,
    PhaseTwoOwed,
    RetryScheduled,
    SourceStateConflict,
    SuccessApplied,
    TaskAbsent,
    TerminalizationOutcome,
)
from horsies.core.lifecycle.persistence import (
    BatchCommitUnknown,
    BatchRolledBack,
    BatchWriteError,
    FusedCompletionBatch,
    apply_fused_batch_in_autocommit,
    apply_in_autocommit,
)
from horsies.core.logging import get_logger
from horsies.core.models.tasks import (
    BuiltInTaskCode,
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.worker.child_protocol import (
    SeatFinished,
    SeatRefusal,
    SeatTask,
    SharedWork,
    ThreadSeatTask,
)
from horsies.core.worker.slots import (
    SHARED_BACKSTOP_MARGIN_S,
    SHARED_REFUSAL_BACKSTOP_S,
    Assigned,
    ChildLost,
    ExitedWith,
    ExitUnknown,
    KilledBySignal,
    LostAfterReturn,
    NotDispatched,
    NotRunCause,
    ChildKilledPastDeadlines,
    ReturnSeen,
    SeatDeadlineEnded,
    SeatKilled,
    SeatTaskFinished,
    SharedHandle,
    SharedOutcome,
    SlotHandle,
    SlotOutcome,
    TaskFinished,
)
from horsies.core.utils.db import (
    SingleStatement,
    autocommit_connection,
    is_retryable_connection_error,
    run_in_autocommit,
)
from horsies.core.worker.runtime import (
    CrashRecoveryCode,
    FinalizeStage,
    _AttemptEndUnknown,
    _TimeoutOutcome,
    _TimeoutSettled,
    _TimeoutWriteFailed,
    _FINALIZE_FUTURE_MAX_RETRIES,
    _FINALIZE_PARENT_MAX_RETRIES,
    _FINALIZE_PHASE1_MAX_RETRIES,
    _FINALIZE_PHASE2_MAX_RETRIES,
    _FINALIZE_RETRY_BASE_DELAY_S,
    _FINALIZE_RETRY_MAX_DELAY_S,
    _FINALIZE_STAGE_FUTURE,
    _FINALIZE_STAGE_PARENT,
    _FINALIZE_STAGE_PHASE1,
    _FINALIZE_STAGE_PHASE2,
    _FINALIZE_STAGE_TIMEOUT,
    _FINALIZE_TIMEOUT_MAX_RETRIES,
    _FinalizeError,
    _RequeueOutcome,
)
from horsies.core.worker.failed_attempt import (
    AttemptFailure,
    AttemptFailureKind,
    AttemptNotHeld,
    ClaimedAttempt,
    NoRetry,
    RetryRequest,
    RunningAttempt,
    finish_failed_attempt,
    finish_for,
    read_in_flight_attempt,
    retry_request_for,
)
from horsies.core.worker.shared_finalize import (
    future_retry_data,
    future_retry_write,
    timeout_retry_data,
    timeout_retry_write,
)
from horsies.core.worker.shared_stop import RetryPlacement, culprit_retry
from horsies.core.worker.stored_timeout import (
    CancelGraceInvalid,
    NoTimeout,
    OptionsUnreadable,
    StoredTimeout,
    TaskTimeout,
    TimeoutValueInvalid,
)
from horsies.core.worker.sql import NOTIFY_TASK_QUEUE_SQL
from horsies.core.worker.workflow_success import (
    finish_workflow_success,
    success_for,
)

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from sqlalchemy.ext.asyncio import (
        AsyncConnection,
        AsyncSession,
        async_sessionmaker,
    )

    from horsies.core.app import Horsies
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.worker.config import WorkerConfig
    from horsies.core.worker.finalize_batch import FinalizeBatcher

logger = get_logger('worker')

GET_TASK_STATUS_RESULT_SQL = text("""
    SELECT status, task_name, queue_name, is_workflow_task, result
    FROM horsies_tasks
    WHERE id = CAST(:id AS uuid)
""")


@dataclass(frozen=True, slots=True)
class _PersistedTaskRow:
    """The persisted terminal facts phase-2 replay decodes.

    Sourced from whichever side of the lifecycle holds the task: the live
    row before its terminalization moves it, the history record after.
    """

    status: str
    task_name: str | None
    queue_name: str | None
    is_workflow_task: bool
    result: str | None


@dataclass(frozen=True, slots=True)
class _PersistedTaskOutcome:
    """Terminal result and phase-2 routing facts read from one row."""

    result: TaskResult[Any, TaskError]
    task_name: str
    queue_name: str
    is_workflow_task: bool


@dataclass(frozen=True, slots=True)
class _Phase1Input:
    """The child's report as phase 1 received it. A phase-1 retry replays it
    from the error data."""

    ok: bool
    result_json_str: str
    failed_reason: str | None


def _phase2_of_a_replay(
    loaded: Result[_PersistedTaskOutcome, _FinalizeError],
) -> Result[_PersistedTaskOutcome | None, _FinalizeError]:
    """What phase 2 still owes after a replay of a committed failure. Pure.

    The failed-attempt finish sent the capacity wake of a plain task in the
    statement that committed, so a plain task owes nothing; a workflow task
    owes its phase 2.
    """
    match loaded:
        case Ok(ok_value=persisted) if not persisted.is_workflow_task:
            return Ok(None)
        case Ok() | Err():
            return loaded


def _attempt_failure_of(
    report: WorkerLevelFailure | ResultUndecodable | ErrorProduced,
) -> AttemptFailure:
    """What the attempt row records for a failure report. Pure."""
    match report:
        case WorkerLevelFailure(detail=detail):
            message = detail or 'Worker failure'
            return AttemptFailure(
                kind=AttemptFailureKind.WORKER_FAILURE,
                error_message=message,
                failed_reason=message,
            )
        case ResultUndecodable(detail=detail):
            return AttemptFailure(
                kind=AttemptFailureKind.FAILED,
                error_message=detail,
                failed_reason=None,
            )
        case ErrorProduced(error=error):
            return AttemptFailure(
                kind=AttemptFailureKind.FAILED,
                error_message=error.message,
                failed_reason=None,
            )
        case _ as unreachable:
            assert_never(unreachable)


def _refused_retry_command(
    error: TaskError,
    context: FinalizeContext,
) -> FailLockedTask:
    """Narrow the pure retry-refusal decision to its contracted command."""
    command = terminalization_for_refused_retry(error, context).command
    if not isinstance(command, FailLockedTask):
        raise RuntimeError(
            'retry refusal classified to a non-failure command: '
            f'{type(command).__name__}'
        )
    return command


def _finalize_data_for_log(data: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return finalize error ``data`` for a log line, without the task result.

    ``data['outcome']['result_json_str']`` is the encoded task result, kept
    for the phase-1 replay. The returned copy holds its length as
    ``result_json_chars`` in its place. ``data`` itself is not changed. Data
    without an outcome result is returned as it is.
    """
    match data:
        case {'outcome': {'result_json_str': str() as result_json_str} as outcome}:
            loggable_outcome: dict[str, Any] = {
                key: value
                for key, value in outcome.items()
                if key != 'result_json_str'
            }
            loggable_outcome['result_json_chars'] = len(result_json_str)
            return {**data, 'outcome': loggable_outcome}
        case _:
            return data


def _shared_timeout_ms(task_id: str, work: SharedWork) -> int:
    """The ``timeout_ms`` of a shared run that ended at its deadline. The
    pool refuses a deadline report of a refusal, so a ``SeatRefusal`` never
    ends that way: ``AssertionError``."""
    match work:
        case SeatTask(timeout_ms=timeout_ms) | ThreadSeatTask(timeout_ms=timeout_ms):
            return timeout_ms
        case SeatRefusal():
            raise AssertionError(
                f'the refused task {task_id} ended at a deadline it does not have',
            )
        case _ as unreachable:
            assert_never(unreachable)


def _max_retries_of(stage: FinalizeStage) -> int:
    """The bound of the finalize retries of ``stage``. Pure."""
    match stage:
        case FinalizeStage.FUTURE:
            return _FINALIZE_FUTURE_MAX_RETRIES
        case FinalizeStage.TIMEOUT:
            return _FINALIZE_TIMEOUT_MAX_RETRIES
        case FinalizeStage.PHASE1:
            return _FINALIZE_PHASE1_MAX_RETRIES
        case FinalizeStage.PHASE2:
            return _FINALIZE_PHASE2_MAX_RETRIES
        case FinalizeStage.PARENT:
            return _FINALIZE_PARENT_MAX_RETRIES
        case _ as unreachable:
            assert_never(unreachable)


class FinalizeMixin:
    """Two-phase finalize pipeline with bounded stage retries."""

    if TYPE_CHECKING:
        # Worker state this mixin reads.
        cfg: WorkerConfig
        sf: async_sessionmaker[AsyncSession]
        broker: PostgresBroker | None
        worker_instance_id: str
        _app: Horsies | None
        _stop: asyncio.Event
        _finalize_retry_attempts: dict[tuple[str, FinalizeStage], int]
        _finalize_batcher: FinalizeBatcher

        # Cross-concern methods provided by sibling mixins / Worker.
        async def _handle_task_timeout(
            self,
            handle: SlotHandle,
            task_id: str,
            timeout_ms: int,
            claimed_at: Optional[datetime] = None,
        ) -> _TimeoutOutcome | ReturnSeen: ...
        async def _handle_shared_backstop(
            self,
            handle: SharedHandle,
            task_id: str,
            work: SharedWork,
        ) -> SharedOutcome | _AttemptEndUnknown: ...
        async def _record_task_timeout(
            self,
            task_id: str,
            timeout_ms: int,
            claimed_at: Optional[datetime],
            *,
            retry: RetryPlacement,
        ) -> _TimeoutSettled | _TimeoutWriteFailed: ...
        async def _wait_for_unseen_exit(
            self,
            handle: SlotHandle | SharedHandle,
            task_id: str,
            pid: int,
        ) -> ExitedWith | KilledBySignal | _AttemptEndUnknown: ...
        async def _finalize_killed_run(
            self,
            task_id: str,
            killed: SeatKilled,
            work: SharedWork,
            claimed_at: Optional[datetime],
            *,
            queue_name: str,
            is_workflow_task: bool,
            task_name: str,
        ) -> Result[None, _FinalizeError]: ...
        async def _requeue_claimed_task(
            self,
            task_id: str,
            reason: str,
            claimed_at: Optional[datetime] = None,
        ) -> _RequeueOutcome: ...
        async def _recover_worker_future_failure(
            self,
            task_id: str,
            reason: str,
            claimed_at: Optional[datetime] = None,
            *,
            failure_code: CrashRecoveryCode = OperationalErrorCode.WORKER_CRASHED,
            retry: RetryPlacement = RetryPlacement.SAME_CLASS,
        ) -> _RequeueOutcome: ...
        def _wake_when_retry_due(
            self,
            scheduled: RetryScheduled,
            request: RetryRequest,
        ) -> None: ...
        async def _sleep_with_stop(self, delay_seconds: float) -> None: ...
        def _spawn_background(
            self,
            coro: Coroutine[Any, Any, Any],
            *,
            name: str,
            finalizer: bool = False,
            service: bool = False,
        ) -> asyncio.Task[Any]: ...

    async def _await_slot_outcome(
        self,
        handle: SlotHandle,
        task_id: str,
        timeout: StoredTimeout,
        claimed_at: Optional[datetime],
    ) -> SlotOutcome | _TimeoutOutcome:
        """Wait for the outcome. The timeout starts when a child gets the task.

        Time waiting for a free slot does not count: a ``TaskTimeout`` bounds
        execution. A refused stored timeout has no deadline: the child fails
        the task at once, before it resolves the task. Never raises for a
        task.

        Once the deadline fires, ``_handle_task_timeout`` owns the row, and
        its outcome is returned: the slot's outcome is not read, even when
        the slot answered after the timer fired. The deadline decides, so a
        late result is dropped. ``_TimeoutSettled`` and ``_AttemptEndUnknown``
        mean that the timeout path wrote the row, or left it on purpose;
        ``_TimeoutWriteFailed`` means that the attempt ended and its
        TASK_TIMEOUT write failed, which the caller retries.

        The deadline bounds the body of the task. When the slot read that the
        body returned with work left on its task loop before the deadline
        fired (``ReturnSeen``), the deadline no longer applies: the slot's
        outcome is returned when it comes, and the slot bounds that wait
        (``ISOLATED_LEFTOVER_BACKSTOP_S``).
        """
        assigned = await asyncio.shield(handle.assigned)
        match assigned:
            case NotDispatched():
                return assigned
            case Assigned():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        match timeout:
            case TaskTimeout(ms=timeout_ms):
                pass
            case (
                NoTimeout()
                | OptionsUnreadable()
                | TimeoutValueInvalid()
                | CancelGraceInvalid()
            ):
                return await asyncio.shield(handle.outcome)
            case _ as unreachable:
                assert_never(unreachable)
        try:
            # shield: wait_for must not cancel the outcome. The child keeps
            # running regardless, and the outcome may be awaited again below.
            return await asyncio.wait_for(
                asyncio.shield(handle.outcome),
                timeout=timeout_ms / 1000.0,
            )
        except asyncio.TimeoutError:
            pass
        timed_out = await self._handle_task_timeout(
            handle,
            task_id,
            timeout_ms,
            claimed_at=claimed_at,
        )
        match timed_out:
            case ReturnSeen():
                return await asyncio.shield(handle.outcome)
            case _TimeoutSettled() | _TimeoutWriteFailed() | _AttemptEndUnknown():
                return timed_out
            case _ as unreachable:
                assert_never(unreachable)

    async def _finalize_after(
        self,
        handle: SlotHandle,
        task_id: str,
        queue_name: str = 'default',
        is_workflow_task: bool = True,
        timeout: StoredTimeout = NoTimeout(),
        *,
        task_name: str,
        claimed_at: Optional[datetime] = None,
    ) -> Result[None, _FinalizeError]:
        outcome = await self._await_slot_outcome(
            handle,
            task_id,
            timeout,
            claimed_at,
        )
        match outcome:
            case TaskFinished(ok=ok, result_json=result_json, failed_reason=reason):
                return await self._finalize_task_result(
                    task_id,
                    ok,
                    result_json,
                    reason,
                    queue_name=queue_name,
                    is_workflow_task=is_workflow_task,
                    task_name=task_name,
                    claimed_at=claimed_at,
                )
            case NotDispatched(cause=cause):
                return await self._requeue_not_dispatched(task_id, cause, claimed_at)
            case ChildLost():
                return await self._finalize_child_loss(
                    handle,
                    task_id,
                    outcome,
                    claimed_at,
                )
            case LostAfterReturn(returned=returned, lost=lost):
                return await self._finalize_return_after_loss(
                    handle,
                    task_id,
                    returned,
                    lost,
                    queue_name=queue_name,
                    is_workflow_task=is_workflow_task,
                    task_name=task_name,
                    claimed_at=claimed_at,
                )
            case _TimeoutSettled() | _AttemptEndUnknown():
                # The timeout path settled this row, or left it to stale-task
                # recovery. Crash recovery must not write it as WORKER_CRASHED.
                return Ok(None)
            case _TimeoutWriteFailed():
                match timeout:
                    case TaskTimeout(ms=timeout_ms):
                        return Err(
                            self._timeout_write_error(
                                task_id,
                                timeout_ms,
                                claimed_at,
                                RetryPlacement.SAME_CLASS,
                            ),
                        )
                    case (
                        NoTimeout()
                        | OptionsUnreadable()
                        | TimeoutValueInvalid()
                        | CancelGraceInvalid()
                    ):
                        raise AssertionError(
                            f'task {task_id} timed out with no deadline',
                        )
                    case _ as unreachable:
                        assert_never(unreachable)
            case _ as unreachable:
                assert_never(unreachable)

    async def _finalize_return_after_loss(
        self,
        handle: SlotHandle,
        task_id: str,
        returned: SeatFinished,
        lost: ChildLost,
        *,
        queue_name: str,
        is_workflow_task: bool,
        task_name: str,
        claimed_at: Optional[datetime],
    ) -> Result[None, _FinalizeError]:
        """Write what the body of a task returned, after its child ended
        while work that the task left on its loop was alive.

        The write waits for the exit of the child, so no work of the attempt
        runs when the row moves. A seen exit: ``returned`` goes through
        ``_finalize_task_result``, as a result does: no TASK_TIMEOUT, no
        crash path. No exit seen: ``_wait_for_unseen_exit`` decides, as for
        any loss. ``_AttemptEndUnknown`` writes nothing and returns
        ``Ok(None)``; an exit seen later writes ``returned``.
        """
        match lost.child_exit:
            case ExitUnknown():
                match await self._wait_for_unseen_exit(handle, task_id, lost.pid):
                    case ExitedWith() | KilledBySignal():
                        pass
                    case _AttemptEndUnknown():
                        return Ok(None)
                    case _ as unreachable:
                        assert_never(unreachable)
            case ExitedWith() | KilledBySignal():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        return await self._finalize_task_result(
            task_id,
            returned.ok,
            returned.result_json,
            returned.failed_reason,
            queue_name=queue_name,
            is_workflow_task=is_workflow_task,
            task_name=task_name,
            claimed_at=claimed_at,
        )

    async def _requeue_not_dispatched(
        self,
        task_id: str,
        cause: NotRunCause,
        claimed_at: Optional[datetime],
    ) -> Result[None, _FinalizeError]:
        """Return a claimed row that never reached a child to the queue.

        A database error is a retryable future-stage error: the row may stay
        CLAIMED until its lease ends.
        """
        requeued = await self._requeue_claimed_task(
            task_id,
            cause.value,
            claimed_at=claimed_at,
        )
        if requeued is _RequeueOutcome.DB_ERROR:
            logger.critical(
                'Requeue DB_ERROR: task %s may remain orphaned CLAIMED '
                '(worker=%s, reason=%s)',
                task_id,
                self.worker_instance_id,
                cause.value,
            )
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_FUTURE,
                    message=f'Requeue failed: {cause.value}',
                    retryable=True,
                    data={
                        'requeue_outcome': requeued.value,
                        'claimed_at': claimed_at,
                    },
                )
            )
        return Ok(None)

    async def _finalize_task_result(
        self,
        task_id: str,
        ok: bool,
        result_json_str: str,
        failed_reason: str | None,
        *,
        queue_name: str,
        is_workflow_task: bool,
        task_name: str,
        claimed_at: Optional[datetime],
    ) -> Result[None, _FinalizeError]:
        """Persist the result that a child reported, then run phase 2.

        ``ok`` has the ``_run_task_entry`` meaning. Phase 1 writes the
        terminal row and its attempt, or the retry; phase 2 runs the workflow
        step and the capacity wakes.
        """
        now = datetime.now(timezone.utc)

        # Warn-only: results are never rejected over size (the work is
        # done; destroying it would convert a size concern into data loss).
        if self._app is not None:
            warn_if_payload_large(
                self._app.config.payload,
                task_name=task_name,
                kind='result',
                encoded_len=len(result_json_str),
            )

        phase1_r = await self._persist_task_terminal_state(
            task_id=task_id,
            now=now,
            ok=ok,
            result_json_str=result_json_str,
            failed_reason=failed_reason,
            task_name=task_name,
            queue_name=queue_name,
            is_workflow_task=is_workflow_task,
            claimed_at=claimed_at,
        )
        if is_err(phase1_r):
            return Err(
                self._with_finalize_context(
                    phase1_r.err_value,
                    queue_name=queue_name,
                    is_workflow_task=is_workflow_task,
                    task_name=task_name,
                    claimed_at=claimed_at,
                )
            )
        persisted = phase1_r.ok_value
        if persisted is None:
            self._clear_finalize_retry_attempts(task_id, _FINALIZE_STAGE_PHASE1)
            return Ok(None)

        # Phase 2: workflow progression + capacity wakeups in a separate transaction.
        # Plain tasks only need capacity notifications.
        phase2_r = await self._finalize_workflow_phase(
            task_id,
            persisted.result,
            queue_name=persisted.queue_name,
            is_workflow_task=persisted.is_workflow_task,
            task_name=persisted.task_name,
        )
        if is_err(phase2_r):
            return phase2_r
        self._clear_finalize_retry_attempts(task_id, _FINALIZE_STAGE_PHASE1)
        self._clear_finalize_retry_attempts(task_id, _FINALIZE_STAGE_PHASE2)
        return Ok(None)

    async def _await_shared_outcome(
        self,
        handle: SharedHandle,
        task_id: str,
        work: SharedWork,
    ) -> SharedOutcome | _AttemptEndUnknown:
        """Wait for the end of a shared task, with one backstop timer.

        The child holds the deadline of the task and reports each step of
        its stop: a shared async child cancels the task at ``timeout_ms``
        and sends a second cancel after ``cancel_grace_ms``; a shared thread
        child closes at ``timeout_ms``. The parent waits from the placement
        until the due step (``timeout_ms``, plus ``cancel_grace_ms`` for an
        async task) and ``SHARED_BACKSTOP_MARGIN_S`` more; past that it
        hands the run to ``_handle_shared_backstop``, which kills the child
        only when it is frozen. A ``SeatRefusal`` runs no task code and has
        no deadline: its backstop is ``SHARED_REFUSAL_BACKSTOP_S``. The timer
        ends when the outcome arrives. Never raises for a task.
        """
        assigned = await asyncio.shield(handle.assigned)
        match assigned:
            case NotDispatched():
                return assigned
            case Assigned():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        match work:
            case SeatTask(timeout_ms=timeout_ms, cancel_grace_ms=cancel_grace_ms):
                backstop_s = (
                    timeout_ms + cancel_grace_ms
                ) / 1000.0 + SHARED_BACKSTOP_MARGIN_S
            case ThreadSeatTask(timeout_ms=timeout_ms):
                backstop_s = timeout_ms / 1000.0 + SHARED_BACKSTOP_MARGIN_S
            case SeatRefusal():
                backstop_s = SHARED_REFUSAL_BACKSTOP_S
            case _ as unreachable:
                assert_never(unreachable)
        try:
            return await asyncio.wait_for(
                asyncio.shield(handle.outcome),
                timeout=backstop_s,
            )
        except asyncio.TimeoutError:
            return await self._handle_shared_backstop(handle, task_id, work)

    async def _finalize_shared_after(
        self,
        handle: SharedHandle,
        task_id: str,
        queue_name: str,
        is_workflow_task: bool,
        work: SharedWork,
        *,
        task_name: str,
        claimed_at: Optional[datetime],
    ) -> Result[None, _FinalizeError]:
        """Finalize a shared task from its outcome.

        A result takes the path of an isolated result; the result of a
        ``SeatRefusal`` is its ``WORKER_SERIALIZATION_ERROR``. A task that
        ended after its deadline, with its marked work for an async task,
        and a task whose child was killed when no task in it had time left
        (``ChildKilledPastDeadlines``, after its exit was seen), ended before
        its row moves: TASK_TIMEOUT, the retry and phase 2 go through
        ``_record_task_timeout``, the writer of isolated timeouts. The retry
        of a culprit runs isolated. The pool refuses a deadline report of a
        run without a deadline, so a ``SeatRefusal`` never ends that way:
        ``AssertionError``. A child that the worker killed for another named
        cause or after the deadline of the run gives its verdict
        (``_finalize_killed_run``); a run that returned before its deadline
        and was stuck only in the work that it left writes what it returned.
        A lost child takes crash recovery, which writes only after the exit
        of the child was seen.
        """
        outcome = await self._await_shared_outcome(handle, task_id, work)
        match outcome:
            case SeatTaskFinished(ok=ok, result_json=result_json, failed_reason=reason):
                return await self._finalize_task_result(
                    task_id,
                    ok,
                    result_json,
                    reason,
                    queue_name=queue_name,
                    is_workflow_task=is_workflow_task,
                    task_name=task_name,
                    claimed_at=claimed_at,
                )
            case SeatDeadlineEnded(retry=retry):
                return await self._finalize_shared_timeout(
                    task_id,
                    _shared_timeout_ms(task_id, work),
                    claimed_at,
                    retry,
                )
            case ChildKilledPastDeadlines(culprit=culprit):
                return await self._finalize_shared_timeout(
                    task_id,
                    _shared_timeout_ms(task_id, work),
                    claimed_at,
                    culprit_retry(culprit, handle.frame.dispatch),
                )
            case SeatKilled():
                return await self._finalize_killed_run(
                    task_id,
                    outcome,
                    work,
                    claimed_at,
                    queue_name=queue_name,
                    is_workflow_task=is_workflow_task,
                    task_name=task_name,
                )
            case NotDispatched(cause=cause):
                return await self._requeue_not_dispatched(task_id, cause, claimed_at)
            case ChildLost():
                return await self._finalize_child_loss(
                    handle,
                    task_id,
                    outcome,
                    claimed_at,
                )
            case _AttemptEndUnknown():
                return Ok(None)
            case _ as unreachable:
                assert_never(unreachable)

    async def _finalize_shared_timeout(
        self,
        task_id: str,
        timeout_ms: int,
        claimed_at: Optional[datetime],
        retry: RetryPlacement,
    ) -> Result[None, _FinalizeError]:
        """Record the timeout of a shared task whose attempt has ended: its
        coroutine and its marked work ended, or its child exited. ``retry``
        is where the retry runs.

        A failed write gives a timeout-stage error, as the timeout of an
        isolated task does: the finalize retry handler runs the same write
        again (``_retry_finalize_timeout``), and crash recovery writes
        TASK_TIMEOUT only after those retries failed.
        """
        match await self._record_task_timeout(
            task_id,
            timeout_ms,
            claimed_at,
            retry=retry,
        ):
            case _TimeoutSettled():
                return Ok(None)
            case _TimeoutWriteFailed():
                return Err(
                    self._timeout_write_error(task_id, timeout_ms, claimed_at, retry),
                )
            case _ as unreachable:
                assert_never(unreachable)

    def _timeout_write_error(
        self,
        task_id: str,
        timeout_ms: int,
        claimed_at: Optional[datetime],
        retry: RetryPlacement,
    ) -> _FinalizeError:
        """The retryable timeout-stage error of an attempt that has ended and
        whose TASK_TIMEOUT write failed. Its data keeps that write
        (``timeout_retry_data``). Pure."""
        return self._make_finalize_error(
            task_id=task_id,
            stage=_FINALIZE_STAGE_TIMEOUT,
            message=(
                f'Task reached timeout_ms={timeout_ms}, and the TASK_TIMEOUT '
                'write failed'
            ),
            retryable=True,
            data=timeout_retry_data(timeout_ms, retry, claimed_at),
        )

    async def _finalize_child_loss(
        self,
        handle: SlotHandle | SharedHandle,
        task_id: str,
        lost: ChildLost,
        claimed_at: Optional[datetime],
    ) -> Result[None, _FinalizeError]:
        """Recover the row of a task whose child ended with no result.

        Crash recovery moves the row only after the child's exit was seen,
        so a retry never runs next to a child that may still be alive.
        ``ExitUnknown`` (the slot gave up waiting after SIGKILL) goes to
        ``_wait_for_unseen_exit``: with stale RUNNING recovery on it writes
        nothing and returns Ok(None), and the row keeps its state for
        stale-task recovery; with it off, the worker waits for the exit of
        that child and then recovers the row as at a seen exit, unless the
        worker stops first. A seen exit goes to
        ``_recover_worker_future_failure``; its result is a future-stage
        error, retryable only when the recovery hit a database error.
        """
        match lost.child_exit:
            case ExitUnknown():
                match await self._wait_for_unseen_exit(handle, task_id, lost.pid):
                    case ExitedWith() | KilledBySignal() as child_exit:
                        lost = replace(lost, child_exit=child_exit)
                    case _AttemptEndUnknown():
                        return Ok(None)
                    case _ as unreachable:
                        assert_never(unreachable)
            case ExitedWith() | KilledBySignal():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        reason = f'Worker child lost: {lost.describe()}'
        requeue_outcome = await self._recover_worker_future_failure(
            task_id,
            reason,
            claimed_at=claimed_at,
        )
        return Err(
            self._make_finalize_error(
                task_id=task_id,
                stage=_FINALIZE_STAGE_FUTURE,
                message=reason,
                # Retryability is keyed on the recovery DB failure: a
                # requeue that hit a transient DB blip must retry the
                # recovery instead of waiting on the reaper.
                retryable=requeue_outcome is _RequeueOutcome.DB_ERROR,
                data={
                    'loss_cause': lost.cause.value,
                    'requeue_outcome': requeue_outcome.value,
                    'claimed_at': claimed_at,
                },
            )
        )

    def _make_finalize_error(
        self,
        *,
        task_id: str,
        stage: FinalizeStage,
        message: str,
        retryable: bool,
        data: dict[str, Any] | None = None,
        error_code: BuiltInTaskCode | str = OperationalErrorCode.BROKER_ERROR,
    ) -> _FinalizeError:
        return _FinalizeError(
            error_code=error_code,
            message=message,
            stage=stage,
            task_id=task_id,
            retryable=retryable,
            data=data,
        )

    def _with_finalize_context(
        self,
        err: _FinalizeError,
        *,
        queue_name: str,
        is_workflow_task: bool,
        task_name: str,
        claimed_at: Optional[datetime] = None,
    ) -> _FinalizeError:
        data = dict(err.data or {})
        data.setdefault('queue_name', queue_name or 'default')
        data.setdefault('is_workflow_task', is_workflow_task)
        data.setdefault('task_name', task_name)
        data.setdefault('claimed_at', claimed_at)
        return _FinalizeError(
            error_code=err.error_code,
            message=err.message,
            stage=err.stage,
            task_id=err.task_id,
            retryable=err.retryable,
            data=data,
        )

    def _finalize_context_from_error(
        self,
        err: _FinalizeError,
    ) -> tuple[str, bool, str, datetime | None]:
        """Recover (queue_name, is_workflow_task, task_name, claimed_at)
        from error data.

        A missing task_name degrades to '' — the engine's ok_type
        resolution then falls back to ``Any`` (same degradation as a
        broker-less completion), it does not fail the retry. A missing
        claimed_at degrades to None, which disables the claim-generation
        fence for the replay (pre-fence behavior).
        """
        data = err.data or {}
        is_workflow_task_raw = data.get('is_workflow_task')
        claimed_at_raw = data.get('claimed_at')
        return (
            str(data.get('queue_name') or 'default'),
            is_workflow_task_raw if isinstance(is_workflow_task_raw, bool) else True,
            str(data.get('task_name') or ''),
            claimed_at_raw if isinstance(claimed_at_raw, datetime) else None,
        )

    def _clear_finalize_retry_attempts(
        self,
        task_id: str,
        stage: FinalizeStage,
    ) -> None:
        self._finalize_retry_attempts.pop((task_id, stage), None)

    async def _persist_task_terminal_state(
        self,
        *,
        task_id: str,
        now: datetime,
        ok: bool,
        result_json_str: str,
        failed_reason: str | None,
        task_name: str,
        queue_name: str,
        is_workflow_task: bool,
        claimed_at: datetime | None = None,
    ) -> Result[_PersistedTaskOutcome | None, _FinalizeError]:
        """Phase 1 of finalization: persist task terminal state/result durably.

        The child wire report is classified into a typed decision before any
        persistence statement is selected. Plain success uses the fused
        operation. A failure, with or without a retry, uses one unlocked read
        of the retry facts and ``horsies_finish_failed_attempt``
        (``_persist_failed_attempt``). A workflow task's success is one
        statement, ``horsies_finish_workflow_success``
        (``_finish_workflow_success``). A retry of a
        committed phase 1 is classified as ``AlreadyApplied`` and reloads the
        persisted result for phase 2; ``LostClaim`` and other refusals do not
        advance the workflow. ``Ok(None)``: nothing is owed to phase 2.
        """
        recovery_cfg = self.cfg.recovery_config
        context = FinalizeContext(
            task_id=task_id,
            worker_id=self.worker_instance_id,
            claimed_at=claimed_at,
            is_workflow_task=is_workflow_task,
            queue_name=queue_name,
            result_json=result_json_str,
            orphan_self_heal_enabled=(
                recovery_cfg is not None
                and recovery_cfg.auto_terminate_orphaned_workflow_tasks
            ),
        )

        tr: TaskResult[Any, TaskError] | None = None
        decode_failure: str | None = None
        if not ok:
            abort_reason = parse_abort_reason(failed_reason)
            if abort_reason is not None:
                report = AbortedBeforeResult(reason=abort_reason)
            else:
                report = WorkerLevelFailure(detail=failed_reason)
                message = failed_reason or 'Worker failure'
                tr = TaskResult(
                    err=TaskError(
                        error_code=OperationalErrorCode.BROKER_ERROR,
                        message=message,
                        data={'task_id': task_id},
                    )
                )
        else:
            # Strict serde reads the fixed-schema err slot before resolving the
            # task's ok type. Unknown tasks can still return a valid TaskError.
            _loads_r = loads_json(result_json_str)
            if is_err(_loads_r):
                decode_failure = f'Result JSON corrupt: {_loads_r.err_value}'
            else:
                _decode_err: Exception | None = None
                try:
                    envelope = validate_task_result_envelope(_loads_r.ok_value)
                except StrictJsonError as exc:
                    _decode_err = exc
                else:
                    err_slot = envelope.get('err')
                    if err_slot is not None:
                        try:
                            err_value = decode_task_error(err_slot)
                            tr = TaskResult(err=err_value)
                        except (StrictJsonError, ValueValidationError) as exc:
                            _decode_err = exc
                    else:
                        source_task = (
                            self._app.tasks.get(task_name)
                            if self._app is not None
                            else None
                        )
                        source_ok_type = (
                            getattr(source_task, 'task_ok_type', None)
                            if source_task is not None
                            else None
                        )
                        if source_ok_type is None:
                            _decode_err = SerializationError(
                                f'Task {task_name!r} not registered or '
                                f'missing task_ok_type during finalize'
                            )
                        else:
                            try:
                                tr = decode_task_result(
                                    _loads_r.ok_value,
                                    source_ok_type,
                                )
                            except (StrictJsonError, ValueValidationError) as exc:
                                _decode_err = exc
                if _decode_err is not None:
                    decode_failure = f'Result decode failed: {_decode_err}'

            if decode_failure is not None:
                logger.error('Task %s: %s', task_id, decode_failure)
                report = ResultUndecodable(detail=decode_failure)
                tr = TaskResult(
                    err=TaskError(
                        error_code=(OperationalErrorCode.WORKER_SERIALIZATION_ERROR),
                        message=decode_failure,
                        data={'task_id': task_id},
                    )
                )
            else:
                assert tr is not None
                if tr.is_err():
                    report = ErrorProduced(error=tr.unwrap_err())
                else:
                    report = ResultProduced()

        try:
            decision = classify(report, context)
        except Exception as exc:
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE1,
                    message='Failed to classify terminal task state',
                    retryable=is_retryable_connection_error(exc),
                    data={
                        'exception_type': type(exc).__name__,
                        'exception': str(exc)[:500],
                        'outcome': {
                            'ok': ok,
                            'result_json_str': result_json_str,
                            'failed_reason': failed_reason,
                        },
                    },
                )
            )
        fused_command: CompleteTaskFused | None = None
        locked_command: CompleteLockedTask | FailLockedTask | None = None
        retry_error: TaskError | None = None
        match decision:
            case NoTerminalAction(reason=reason):
                logger.debug(
                    'Task %s terminalization classified as no-op: %s',
                    task_id,
                    reason.value,
                )
                return Ok(None)
            case ReplayWorkflowPhase2():
                load_r = await self._load_persisted_task_result(task_id)
                if is_err(load_r):
                    return Err(load_r.err_value)
                return load_r
            case ApplyTerminalization(command=CancelOwnedOrphan() as command):
                try:
                    outcome = await apply_in_autocommit(self.sf, command)
                    match outcome:
                        case Applied():
                            terminated = True
                        case (
                            AlreadyApplied()
                            | LostClaim()
                            | SourceStateConflict()
                            | TaskAbsent()
                        ):
                            terminated = False
                        case _ as unreachable:
                            assert_never(unreachable)
                except Exception as exc:
                    return Err(
                        self._make_finalize_error(
                            task_id=task_id,
                            stage=_FINALIZE_STAGE_PHASE1,
                            message='Failed to terminate orphaned workflow task',
                            retryable=is_retryable_connection_error(exc),
                            data={
                                'exception_type': type(exc).__name__,
                                'exception': str(exc)[:500],
                                'outcome': {
                                    'ok': ok,
                                    'result_json_str': result_json_str,
                                    'failed_reason': failed_reason,
                                },
                            },
                        )
                    )
                if terminated:
                    logger.info(
                        'Task %s cancelled: orphaned workflow task '
                        '(no live workflow_task linkage)',
                        task_id,
                    )
                else:
                    logger.debug(
                        'Task %s was no longer a terminable owned orphan',
                        task_id,
                    )
                return Ok(None)
            case ApplyTerminalization(command=CompleteTaskFused() as command):
                fused_command = command
            case ApplyTerminalization(
                command=(CompleteLockedTask() | FailLockedTask()) as command,
            ):
                locked_command = command
            case ScheduleAutomaticRetry(error=error):
                retry_error = error
            case ApplyTerminalization(command=unsupported):
                raise RuntimeError(
                    f'{type(unsupported).__name__} is not part of this '
                    f"cutover's authorized scope"
                )
            case _ as unreachable:
                assert_never(unreachable)

        if fused_command is not None:
            return await self._complete_plain_success(
                fused_command,
                outcome_data={
                    'ok': ok,
                    'result_json_str': result_json_str,
                    'failed_reason': failed_reason,
                },
            )

        match (locked_command, report):
            case (CompleteLockedTask() as completion, ResultProduced()):
                assert tr is not None
                return await self._finish_workflow_success(
                    completion,
                    tr,
                    claimed_at=claimed_at,
                    child=_Phase1Input(ok, result_json_str, failed_reason),
                )
            case (
                FailLockedTask() as failure,
                WorkerLevelFailure()
                | ResultUndecodable() as failure_report,
            ):
                assert tr is not None
                return await self._persist_failed_attempt(
                    failure,
                    failure_report,
                    tr,
                    claimed_at=claimed_at,
                    child=_Phase1Input(ok, result_json_str, failed_reason),
                )
            case (None, ErrorProduced() as failure_report) if (retry_error is not None):
                assert tr is not None
                return await self._persist_failed_attempt(
                    _refused_retry_command(retry_error, context),
                    failure_report,
                    tr,
                    claimed_at=claimed_at,
                    child=_Phase1Input(ok, result_json_str, failed_reason),
                )
            case _:
                raise AssertionError(
                    'terminal report and locked command disagree: '
                    f'{type(report).__name__} / {type(locked_command).__name__}'
                )

    async def _finish_workflow_success(
        self,
        completion: CompleteLockedTask,
        tr: TaskResult[Any, TaskError],
        *,
        claimed_at: datetime | None,
        child: _Phase1Input,
    ) -> Result[_PersistedTaskOutcome | None, _FinalizeError]:
        """Phase 1 of a workflow task's success: one statement.

        ``finish_workflow_success`` runs ``horsies_finish_workflow_success``
        in autocommit: the attempt row, the move and the phase-2 record of
        the node commit together. ``SuccessApplied`` owes phase 2;
        ``AlreadyApplied`` (a replay of a committed success) reloads the
        persisted result for the phase-2 replay; a refusal wrote nothing and
        owes nothing. Database errors become a phase-1 error, retryable when
        the connection failed; its data keeps the child's report for the
        replay.
        """
        try:
            outcome = await finish_workflow_success(
                self.sf,
                success_for(completion, claimed_at=claimed_at),
            )
        except Exception as exc:
            return Err(
                self._phase1_error(
                    completion.task_id,
                    'Failed to persist terminal task state',
                    exc,
                    child,
                )
            )
        match outcome:
            case SuccessApplied(
                phase_two=PhaseTwoOwed(task_name=task_name, queue_name=queue_name),
            ):
                return Ok(
                    _PersistedTaskOutcome(
                        result=tr,
                        task_name=task_name,
                        queue_name=queue_name,
                        is_workflow_task=True,
                    )
                )
            case AlreadyApplied(task_id=task_id):
                return await self._load_persisted_task_result(task_id)
            case LostClaim() | SourceStateConflict() | TaskAbsent():
                return Ok(None)
            case _ as unreachable:
                assert_never(unreachable)

    async def _write_fused_batch(
        self,
        batch: FusedCompletionBatch,
    ) -> Result[tuple[TerminalizationOutcome, ...], BatchWriteError]:
        """Write ``batch`` on the worker pool. The write of the batcher."""
        return await apply_fused_batch_in_autocommit(self.sf, batch)

    async def _complete_plain_success(
        self,
        command: CompleteTaskFused,
        *,
        outcome_data: dict[str, Any],
    ) -> Result[_PersistedTaskOutcome | None, _FinalizeError]:
        """Write a plain success through the finalize batcher.

        Returns after the statement of the row's batch returned: ``Applied``
        is committed, and a refusal wrote nothing. A plain success has no
        phase 2: its capacity NOTIFY commits with the row. ``outcome_data``
        is the child outcome that a phase-1 retry replays.

        Outcomes:
            Ok(None): the row has its outcome (any of the five).
            Err(PHASE1): ``BatchRolledBack`` (retryable when its cause may
                clear), ``BatchCommitUnknown`` (retryable: the replay finds
                the row ``AlreadyApplied`` when it committed), or a defect of
                the batch write (retryable when it is a connection error).
        """
        try:
            written = await self._finalize_batcher.complete(command)
        except Exception as exc:
            return Err(
                self._make_finalize_error(
                    task_id=command.task_id,
                    stage=_FINALIZE_STAGE_PHASE1,
                    message='Failed to persist terminal task state',
                    retryable=is_retryable_connection_error(exc),
                    data={
                        'exception_type': type(exc).__name__,
                        'exception': str(exc)[:500],
                        'outcome': outcome_data,
                    },
                )
            )
        match written:
            case Ok(outcome):
                match outcome:
                    case (
                        Applied()
                        | AlreadyApplied()
                        | LostClaim()
                        | SourceStateConflict()
                        | TaskAbsent()
                    ):
                        return Ok(None)
                    case _ as unreachable:
                        assert_never(unreachable)
            case Err(error):
                return Err(
                    self._batch_write_error(command.task_id, error, outcome_data)
                )

    def _batch_write_error(
        self,
        task_id: str,
        error: BatchWriteError,
        outcome_data: dict[str, Any],
    ) -> _FinalizeError:
        """The phase-1 error of a failed batch write of ``task_id``. Pure."""
        match error:
            case BatchRolledBack(cause=cause):
                retryable = error.retryable
                write_state = 'ROLLED_BACK'
            case BatchCommitUnknown(cause=cause):
                retryable = True
                write_state = 'COMMIT_UNKNOWN'
            case _ as unreachable:
                assert_never(unreachable)
        return self._make_finalize_error(
            task_id=task_id,
            stage=_FINALIZE_STAGE_PHASE1,
            message='Failed to persist terminal task state',
            retryable=retryable,
            data={
                'batch_write': write_state,
                'exception_type': type(cause.error).__name__,
                'exception': str(cause.error)[:500],
                'outcome': outcome_data,
            },
        )

    async def _persist_failed_attempt(
        self,
        failure: FailLockedTask,
        report: WorkerLevelFailure | ResultUndecodable | ErrorProduced,
        tr: TaskResult[Any, TaskError],
        *,
        claimed_at: datetime | None,
        child: _Phase1Input,
    ) -> Result[_PersistedTaskOutcome | None, _FinalizeError]:
        """Phase 1 of a failed attempt: one read and one statement.

        For a task error the retry facts are read first, and the policy
        decides the retry; a worker-level failure and an undecodable result
        are never retried, so they skip the read. ``finish_failed_attempt``
        writes the attempt row with the retry or the terminal failure, both
        in autocommit. The result routes phase 2: a workflow task's failure
        and a replay of a committed failure return the persisted outcome; a
        plain task's failure (its capacity wake is sent), a retry and a
        refusal return ``Ok(None)``. Database errors become a phase-1 error,
        retryable when the connection failed; its data keeps the child's
        report for the replay.
        """
        task_id = failure.task_id
        try:
            async with self.sf() as s:
                connection = await autocommit_connection(s)
                match report:
                    case ErrorProduced():
                        retry = await self._finalize_retry_request(
                            connection,
                            task_id,
                            failure.error_code,
                            claimed_at,
                        )
                    case WorkerLevelFailure() | ResultUndecodable():
                        retry = NoRetry()
                    case _ as unreachable:
                        assert_never(unreachable)
                outcome = await finish_failed_attempt(
                    connection,
                    finish_for(
                        failure,
                        claimed_at=claimed_at,
                        retry=retry,
                        attempt=_attempt_failure_of(report),
                    ),
                )
                return await self._after_failed_attempt(s, outcome, retry, tr)
        except Exception as exc:
            return Err(
                self._phase1_error(
                    task_id,
                    'Failed to persist terminal task state',
                    exc,
                    child,
                )
            )

    async def _finalize_retry_request(
        self,
        connection: AsyncConnection,
        task_id: str,
        error_code: str | None,
        claimed_at: datetime | None,
    ) -> RetryRequest:
        """The retry that the policy asks for, from the unlocked read.

        A row that this claim generation does not hold RUNNING gets
        ``NoRetry``: the finish then classifies it and writes nothing.
        Database errors raise.
        """
        attempt = await read_in_flight_attempt(
            connection,
            task_id=task_id,
            worker_id=self.worker_instance_id,
            claimed_at=claimed_at,
        )
        match attempt:
            case RunningAttempt():
                return retry_request_for(
                    attempt,
                    error_code,
                    RetryPlacement.SAME_CLASS,
                )
            case ClaimedAttempt() | AttemptNotHeld():
                return NoRetry()
            case _ as unreachable:
                assert_never(unreachable)

    async def _after_failed_attempt(
        self,
        s: AsyncSession,
        outcome: FailedAttemptOutcome,
        retry: RetryRequest,
        tr: TaskResult[Any, TaskError],
    ) -> Result[_PersistedTaskOutcome | None, _FinalizeError]:
        """Route the outcome of a failed-attempt finish to phase 2 or to done.

        Database errors of the replay read raise.
        """
        match outcome:
            case FailureApplied(after=after):
                match after:
                    case CapacityWoken():
                        return Ok(None)
                    case PhaseTwoOwed(task_name=task_name, queue_name=queue_name):
                        return Ok(
                            _PersistedTaskOutcome(
                                result=tr,
                                task_name=task_name,
                                queue_name=queue_name,
                                is_workflow_task=True,
                            )
                        )
                    case _ as unreachable:
                        assert_never(unreachable)
            case RetryScheduled():
                self._wake_when_retry_due(outcome, retry)
                return Ok(None)
            case AlreadyApplied(task_id=task_id):
                return _phase2_of_a_replay(
                    await self._load_persisted_task_result(task_id, session=s),
                )
            case LostClaim() | SourceStateConflict() | TaskAbsent():
                return Ok(None)
            case _ as unreachable:
                assert_never(unreachable)

    def _phase1_error(
        self,
        task_id: str,
        message: str,
        exc: Exception,
        child: _Phase1Input,
    ) -> _FinalizeError:
        """The phase-1 error of a failed write. Its data keeps the child's
        report, which the phase-1 retry replays. Pure."""
        return self._make_finalize_error(
            task_id=task_id,
            stage=_FINALIZE_STAGE_PHASE1,
            message=message,
            retryable=is_retryable_connection_error(exc),
            data={
                'exception_type': type(exc).__name__,
                'exception': str(exc)[:500],
                'outcome': {
                    'ok': child.ok,
                    'result_json_str': child.result_json_str,
                    'failed_reason': child.failed_reason,
                },
            },
        )

    async def _finalize_workflow_phase(
        self,
        task_id: str,
        tr: 'TaskResult[Any, TaskError]',
        *,
        queue_name: str = 'default',
        is_workflow_task: bool = True,
        task_name: str,
    ) -> Result[None, _FinalizeError]:
        """Phase 2 of finalization: workflow advancement and worker wake notifications.

        A plain task has only the wake (``_wake_queue_after_plain_task``).
        Parent propagation queued by the completion check is driven AFTER
        this transaction commits, one fresh transaction per ancestor level
        (_drive_parent_propagations) — the pre-denest in-transaction
        recursion accumulated FOR UPDATE locks root-ward across the chain.
        """
        from horsies.core.workflows.engine import pop_pending_parent_propagations

        match is_workflow_task:
            case False:
                return await self._wake_queue_after_plain_task(task_id, queue_name)
            case True:
                pass

        pending_propagations: list[str] = []
        try:
            async with self.sf() as s:
                # Handle workflow task completion (if this task is part of a workflow)
                await self._handle_workflow_task_if_needed(
                    s,
                    task_id,
                    tr,
                    is_workflow_task=is_workflow_task,
                    task_name=task_name,
                )

                # Proactively wake workers of this queue to re-check
                # capacity/backlog (workers no longer listen on task_new).
                # The wake is a statement of this transaction: when it fails
                # on the server, the transaction is aborted and its commit
                # would roll the workflow writes back without an error, so a
                # failure goes to the phase-2 error below, never past it.
                await s.execute(
                    NOTIFY_TASK_QUEUE_SQL,
                    {
                        'c2': f'task_queue_{queue_name or "default"}',
                        'p': f'capacity:{task_id}',
                    },
                )

                # Trigger automatically sends NOTIFY on UPDATE; commit to flush NOTIFYs
                await s.commit()
                pending_propagations = pop_pending_parent_propagations(s)
        except Exception as exc:
            return Err(
                self._phase2_error(
                    task_id,
                    queue_name,
                    is_workflow_task=is_workflow_task,
                    exc=exc,
                )
            )

        # Outside the try deliberately: a phase2 retry cannot re-reach
        # propagation past the task-level terminal CAS, so propagation must
        # never fold into a phase2 error. The driver is total — each level's
        # failure goes to the parent_propagation retry stage.
        if pending_propagations:
            await self._drive_parent_propagations(pending_propagations)
        return Ok(None)

    async def _wake_queue_after_plain_task(
        self,
        task_id: str,
        queue_name: str,
    ) -> Result[None, _FinalizeError]:
        """Phase 2 of a plain task: wake the workers of its queue to claim.

        One statement in autocommit (``run_in_autocommit``). A failure is a
        phase-2 error with the classification of every phase-2 error: a
        connection error is retryable, and the retry sends the wake again.
        The terminal state of the task is already committed either way.
        """
        try:
            _ = await run_in_autocommit(
                self.sf,
                SingleStatement(
                    statement=NOTIFY_TASK_QUEUE_SQL,
                    parameters={
                        'c2': f'task_queue_{queue_name or "default"}',
                        'p': f'capacity:{task_id}',
                    },
                ),
            )
        except Exception as exc:
            return Err(
                self._phase2_error(
                    task_id,
                    queue_name,
                    is_workflow_task=False,
                    exc=exc,
                )
            )
        return Ok(None)

    def _phase2_error(
        self,
        task_id: str,
        queue_name: str,
        *,
        is_workflow_task: bool,
        exc: Exception,
    ) -> _FinalizeError:
        """The phase-2 error of ``exc``: retryable when it is a transient
        connection error."""
        return self._make_finalize_error(
            task_id=task_id,
            stage=_FINALIZE_STAGE_PHASE2,
            message='Workflow finalize phase failed after task terminal state persisted',
            retryable=is_retryable_connection_error(exc),
            data={
                'task_id': task_id,
                'phase': 'finalize_phase_2',
                'queue_name': queue_name or 'default',
                'is_workflow_task': is_workflow_task,
                'exception_type': type(exc).__name__,
                'exception': str(exc)[:500],
            },
        )

    async def _load_persisted_task_result(
        self,
        task_id: str,
        *,
        session: AsyncSession | None = None,
    ) -> Result[_PersistedTaskOutcome, _FinalizeError]:
        """Load the typed terminal result and its phase-2 routing facts.

        Values come from the task row rather than retry metadata. A NULL task
        name becomes ``''``; the workflow engine then degrades to an ``Any``
        encode instead of failing.
        """
        try:
            if session is not None:
                return await self._read_persisted_task_result(task_id, session)
            async with self.sf() as owned_session:
                return await self._read_persisted_task_result(
                    task_id,
                    owned_session,
                )
        except Exception as exc:
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE2,
                    message='Cannot replay finalize phase-2: loading persisted task result failed',
                    retryable=is_retryable_connection_error(exc),
                    data={
                        'exception_type': type(exc).__name__,
                        'exception': str(exc)[:500],
                    },
                )
            )

    async def _read_persisted_task_row(
        self,
        task_id: str,
        session: AsyncSession,
    ) -> _PersistedTaskRow | None:
        """Resolve the persisted terminal facts from live, else from history.

        Phase-2 replay runs after phase 1 has terminalized the task, and
        terminalization moves the row out of the live table. A live miss is
        therefore the expected shape on replay, not an absent task: the
        record answers from history, where its result payload also lives.
        """
        res = await session.execute(GET_TASK_STATUS_RESULT_SQL, {'id': task_id})
        live_row = res.fetchone()
        if live_row is not None:
            return _PersistedTaskRow(
                status=str(live_row.status) if live_row.status is not None else '',
                task_name=live_row.task_name,
                queue_name=live_row.queue_name,
                is_workflow_task=bool(live_row.is_workflow_task),
                result=live_row.result,
            )
        try:
            UUID(task_id)
        except ValueError:
            return None
        connection = await session.connection()
        if not await staged_detail_published(connection):
            return None
        detail = await read_task_detail(connection, task_id=task_id)
        match detail:
            case HistoryTaskDetail():
                return _PersistedTaskRow(
                    status=detail.status.value,
                    task_name=detail.task_name,
                    queue_name=detail.queue_name,
                    is_workflow_task=detail.is_workflow_task,
                    result=(
                        detail.result_payload.decode('utf-8')
                        if detail.result_payload is not None
                        else None
                    ),
                )
            case _:
                return None

    async def _read_persisted_task_result(
        self,
        task_id: str,
        session: AsyncSession,
    ) -> Result[_PersistedTaskOutcome, _FinalizeError]:
        """Read and decode a persisted result through the supplied transaction."""
        row = await self._read_persisted_task_row(task_id, session)
        if row is None:
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE2,
                    message='Cannot replay finalize phase-2: task row not found',
                    retryable=False,
                )
            )
        status = row.status
        raw_result = row.result
        if status not in ('COMPLETED', 'FAILED', 'EXPIRED') or raw_result is None:
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE2,
                    message='Cannot replay finalize phase-2: terminal task result unavailable',
                    retryable=False,
                    data={'status': status},
                )
            )
        loads_r = loads_json(raw_result)
        if is_err(loads_r):
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE2,
                    message=f'Cannot replay finalize phase-2: stored result JSON corrupt: {loads_r.err_value}',
                    retryable=False,
                )
            )
        # TaskError has a fixed schema and decodes without OkT, so read the err
        # slot before requiring a local registry entry. A persisted error for an
        # unknown task name remains recoverable during phase-2 replay.
        source_task_name = row.task_name
        try:
            envelope = validate_task_result_envelope(loads_r.ok_value)
        except StrictJsonError as exc:
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE2,
                    message=(
                        f'Cannot replay finalize phase-2: stored '
                        f'result envelope invalid: {exc}'
                    ),
                    retryable=False,
                )
            )
        err_slot = envelope.get('err')
        if err_slot is not None:
            try:
                err_value = decode_task_error(err_slot)
            except (StrictJsonError, ValueValidationError) as exc:
                return Err(
                    self._make_finalize_error(
                        task_id=task_id,
                        stage=_FINALIZE_STAGE_PHASE2,
                        message=(
                            f'Cannot replay finalize phase-2: '
                            f'stored err decode failed: {exc}'
                        ),
                        retryable=False,
                    )
                )
            return Ok(
                _PersistedTaskOutcome(
                    result=TaskResult(err=err_value),
                    task_name=str(source_task_name or ''),
                    queue_name=str(row.queue_name or 'default'),
                    is_workflow_task=bool(row.is_workflow_task),
                )
            )
        source_task = (
            self._app.tasks.get(source_task_name)
            if self._app is not None and source_task_name is not None
            else None
        )
        source_ok_type = (
            getattr(source_task, 'task_ok_type', None)
            if source_task is not None
            else None
        )
        if source_ok_type is None:
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE2,
                    message=(
                        f'Cannot replay finalize phase-2: task '
                        f'{source_task_name!r} not registered or '
                        f'missing task_ok_type'
                    ),
                    retryable=False,
                )
            )
        try:
            decoded_tr = decode_task_result(loads_r.ok_value, source_ok_type)
        except (StrictJsonError, ValueValidationError) as exc:
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE2,
                    message=(
                        f'Cannot replay finalize phase-2: '
                        f'stored result decode failed: {exc}'
                    ),
                    retryable=False,
                )
            )
        return Ok(
            _PersistedTaskOutcome(
                result=decoded_tr,
                task_name=str(source_task_name or ''),
                queue_name=str(row.queue_name or 'default'),
                is_workflow_task=bool(row.is_workflow_task),
            )
        )

    async def _handle_finalize_error(self, err: Any) -> None:
        """Handle finalize Result errors with bounded retries.

        Total: ``err`` is ``Any`` because the background-task done-callback
        hands over an untyped ``Err`` payload; the isinstance guard is the
        boundary defense. Each ``FinalizeStage`` has its retry bound
        (``_max_retries_of``) and its retry (``_spawn_finalize_retry``).
        After the last retry of a TASK_TIMEOUT write, crash recovery writes
        the row (``_recover_timed_out_row``). Retry coroutines are spawned on
        the running loop (done-callbacks only execute while the loop runs, so
        there is no closed-loop raise window); their failures terminate at
        the done-callback.
        """
        if not isinstance(err, _FinalizeError):
            logger.error(
                f'Unexpected finalize error payload type: {type(err).__name__}'
            )
            return

        stage = err.stage
        task_id = err.task_id
        max_attempts = _max_retries_of(stage)
        key = (task_id, stage)
        attempts = self._finalize_retry_attempts.get(key, 0)

        if not err.retryable:
            # ``retryable`` scopes only this finalize-retry loop. Whether the
            # TASK retries is decided by the recovery path that produced the
            # error (broken-pool recovery, for one, requeues the task per its
            # retry policy before this handler ever runs) — so the log must
            # not claim anything about the task's fate.
            logger.error(
                f'Finalize stage will not be re-attempted ({task_id}) '
                f'stage={stage}: {err.message}; data={_finalize_data_for_log(err.data)}'
            )
            self._clear_finalize_retry_attempts(task_id, stage)
            return

        if attempts >= max_attempts:
            logger.critical(
                f'Finalize retries exhausted for task {task_id} stage={stage} after {attempts} attempts: '
                f'{err.message}; data={_finalize_data_for_log(err.data)}'
            )
            self._clear_finalize_retry_attempts(task_id, stage)
            self._after_the_last_retry(err)
            return

        attempt_no = attempts + 1
        self._finalize_retry_attempts[key] = attempt_no
        delay = min(
            _FINALIZE_RETRY_MAX_DELAY_S,
            _FINALIZE_RETRY_BASE_DELAY_S * (2 ** (attempt_no - 1)),
        )
        logger.warning(
            f'Finalize retry scheduled for task {task_id} stage={stage} '
            f'({attempt_no}/{max_attempts}) in {delay:.1f}s: {err.message}'
        )
        self._spawn_finalize_retry(err, delay)

    def _after_the_last_retry(self, err: _FinalizeError) -> None:
        """Hand the row of a TASK_TIMEOUT write whose retries all failed to
        crash recovery. The other stages end with the log of the caller."""
        match err.stage:
            case FinalizeStage.TIMEOUT:
                self._spawn_background(
                    self._recover_timed_out_row(err),
                    name=f'finalize-timeout-recovery-{err.task_id}',
                    finalizer=True,
                )
            case (
                FinalizeStage.FUTURE
                | FinalizeStage.PHASE1
                | FinalizeStage.PHASE2
                | FinalizeStage.PARENT
            ):
                return
            case _ as unreachable:
                assert_never(unreachable)

    def _spawn_finalize_retry(self, err: _FinalizeError, delay_s: float) -> None:
        """Spawn the retry of the stage of ``err`` after ``delay_s``."""
        retry: Coroutine[Any, Any, None]
        match err.stage:
            case FinalizeStage.FUTURE:
                retry, label = self._retry_finalize_future(err, delay_s), 'future'
            case FinalizeStage.TIMEOUT:
                retry, label = self._retry_finalize_timeout(err, delay_s), 'timeout'
            case FinalizeStage.PHASE1:
                retry, label = self._retry_finalize_phase1(err, delay_s), 'phase1'
            case FinalizeStage.PARENT:
                retry, label = self._retry_finalize_parent(err, delay_s), 'parent'
            case FinalizeStage.PHASE2:
                retry, label = self._retry_finalize_phase2(err, delay_s), 'phase2'
            case _ as unreachable:
                assert_never(unreachable)
        self._spawn_background(
            retry,
            name=f'finalize-retry-{label}-{err.task_id}',
            finalizer=True,
        )

    async def _retry_finalize_future(self, err: _FinalizeError, delay_s: float) -> None:
        """Retry requeue after a child future failed before producing an outcome.

        Total by composition: every subcall either returns a Result/outcome
        value or is itself total. Cancellation is the exemption: it escapes
        every await and the done-callback drops it silently. Any other
        escape is logged by the background-task done-callback.
        """
        await self._sleep_with_stop(delay_s)
        if self._stop.is_set():
            return

        claimed_at_raw = (err.data or {}).get('claimed_at')
        failure_code, retry = future_retry_write(err.data)
        outcome = await self._recover_worker_future_failure(
            err.task_id,
            f'Retry after future-stage finalize error: {err.message}',
            claimed_at=(
                claimed_at_raw if isinstance(claimed_at_raw, datetime) else None
            ),
            failure_code=failure_code,
            retry=retry,
        )
        if outcome in (
            _RequeueOutcome.REQUEUED,
            _RequeueOutcome.TERMINAL_REPLAYED,
        ):
            self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_FUTURE)
            return
        if outcome is _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED:
            logger.warning(
                f'Future-stage finalize retry found no owned in-flight row for task {err.task_id}'
            )
            self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_FUTURE)
            return

        data = dict(err.data or {})
        data['requeue_outcome'] = outcome.value
        await self._handle_finalize_error(
            self._make_finalize_error(
                task_id=err.task_id,
                stage=_FINALIZE_STAGE_FUTURE,
                message=err.message,
                retryable=True,
                data=data,
            )
        )

    async def _retry_finalize_timeout(
        self,
        err: _FinalizeError,
        delay_s: float,
    ) -> None:
        """Run the failed TASK_TIMEOUT write of an ended attempt again.

        The write is ``_record_task_timeout``, with the ``timeout_ms``, the
        retry placement and the claim generation that the error kept. A
        settled row clears the retries; a failed write goes back to
        ``_handle_finalize_error``, which bounds the retries and hands the
        row to crash recovery after the last one. A stop drops the retry,
        as for every finalize retry.
        """
        await self._sleep_with_stop(delay_s)
        if self._stop.is_set():
            return
        timeout_ms, retry, claimed_at = timeout_retry_write(err.data)
        match await self._record_task_timeout(
            err.task_id,
            timeout_ms,
            claimed_at,
            retry=retry,
        ):
            case _TimeoutSettled():
                self._clear_finalize_retry_attempts(
                    err.task_id,
                    _FINALIZE_STAGE_TIMEOUT,
                )
            case _TimeoutWriteFailed():
                await self._handle_finalize_error(err)
            case _ as unreachable:
                assert_never(unreachable)

    async def _recover_timed_out_row(self, err: _FinalizeError) -> None:
        """Crash recovery of an ended attempt whose TASK_TIMEOUT write failed
        on every retry.

        The writer of crash recovery reads the row under its lock and writes
        TASK_TIMEOUT, so the retry policy decides on that code; the retry runs
        at the placement that the error kept. A database error there becomes
        a future-stage error whose data keeps the same code and placement
        (``future_retry_data``).
        """
        timeout_ms, retry, claimed_at = timeout_retry_write(err.data)
        code: CrashRecoveryCode = OutcomeCode.TASK_TIMEOUT
        reason = (
            f'Task reached timeout_ms={timeout_ms}, and the TASK_TIMEOUT write '
            f'failed {_FINALIZE_TIMEOUT_MAX_RETRIES + 1} times'
        )
        match await self._recover_worker_future_failure(
            err.task_id,
            reason,
            claimed_at=claimed_at,
            failure_code=code,
            retry=retry,
        ):
            case (
                _RequeueOutcome.REQUEUED
                | _RequeueOutcome.TERMINAL_REPLAYED
                | _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED
            ):
                return
            case _RequeueOutcome.DB_ERROR:
                await self._handle_finalize_error(
                    self._make_finalize_error(
                        task_id=err.task_id,
                        stage=_FINALIZE_STAGE_FUTURE,
                        message=reason,
                        retryable=True,
                        data=future_retry_data(code, retry, claimed_at),
                    ),
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def _retry_finalize_phase1(self, err: _FinalizeError, delay_s: float) -> None:
        """Retry phase-1 terminal persistence from captured child outcome payload.

        Total by composition: every subcall either returns a Result/outcome
        value or is itself total. Cancellation is the exemption: it escapes
        every await and the done-callback drops it silently. Any other
        escape is logged by the background-task done-callback.
        """
        await self._sleep_with_stop(delay_s)
        if self._stop.is_set():
            return

        outcome_raw = (err.data or {}).get('outcome')
        if not isinstance(outcome_raw, dict):
            logger.error(
                f'Finalize phase-1 retry missing outcome payload for task {err.task_id}'
            )
            self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE1)
            return
        outcome = cast(dict[str, object], outcome_raw)

        ok = bool(outcome.get('ok', False))
        result_json_str = outcome.get('result_json_str')
        if not isinstance(result_json_str, str):
            logger.error(
                f'Finalize phase-1 retry missing result_json_str for task {err.task_id}'
            )
            self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE1)
            return
        failed_reason_raw = outcome.get('failed_reason')
        failed_reason = (
            str(failed_reason_raw) if failed_reason_raw is not None else None
        )

        retry_queue_name, retry_is_workflow_task, retry_task_name, retry_claimed_at = (
            self._finalize_context_from_error(err)
        )
        phase1_r = await self._persist_task_terminal_state(
            task_id=err.task_id,
            now=datetime.now(timezone.utc),
            ok=ok,
            result_json_str=result_json_str,
            failed_reason=failed_reason,
            task_name=retry_task_name,
            queue_name=retry_queue_name,
            is_workflow_task=retry_is_workflow_task,
            claimed_at=retry_claimed_at,
        )
        if is_err(phase1_r):
            await self._handle_finalize_error(
                self._with_finalize_context(
                    phase1_r.err_value,
                    queue_name=retry_queue_name,
                    is_workflow_task=retry_is_workflow_task,
                    task_name=retry_task_name,
                    claimed_at=retry_claimed_at,
                )
            )
            return

        persisted = phase1_r.ok_value
        if persisted is None:
            self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE1)
            return

        phase2_r = await self._finalize_workflow_phase(
            err.task_id,
            persisted.result,
            queue_name=persisted.queue_name,
            is_workflow_task=persisted.is_workflow_task,
            task_name=persisted.task_name,
        )
        if is_err(phase2_r):
            await self._handle_finalize_error(phase2_r.err_value)
            return

        self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE1)
        self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE2)

    async def _retry_finalize_phase2(self, err: _FinalizeError, delay_s: float) -> None:
        """Retry phase-2 workflow advancement from persisted terminal task result.

        Total by composition: every subcall either returns a Result/outcome
        value or is itself total. Cancellation is the exemption: it escapes
        every await and the done-callback drops it silently. Any other
        escape is logged by the background-task done-callback.
        """
        await self._sleep_with_stop(delay_s)
        if self._stop.is_set():
            return

        load_r = await self._load_persisted_task_result(err.task_id)
        if is_err(load_r):
            await self._handle_finalize_error(load_r.err_value)
            return
        persisted = load_r.ok_value
        phase2_r = await self._finalize_workflow_phase(
            err.task_id,
            persisted.result,
            queue_name=persisted.queue_name,
            is_workflow_task=persisted.is_workflow_task,
            task_name=persisted.task_name,
        )
        if is_err(phase2_r):
            await self._handle_finalize_error(phase2_r.err_value)
            return

        self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PHASE2)

    async def _drive_parent_propagations(self, child_workflow_ids: list[str]) -> None:
        """Advance parent workflows, one fresh transaction per ancestor level.

        Each completed child workflow advances its parent in its own
        transaction; a level's completion check may queue the grandparent,
        extending the worklist — iterative, never recursive, no nested
        FOR UPDATE locks.

        Total: a level's failure folds into a parent_propagation-stage
        finalize error carrying the remaining worklist; the bounded retry
        re-enters the chain from there (a phase2 retry could not — the
        task-level terminal CAS blocks it). Recovery case 1.6 is the
        backstop when retries exhaust or the worker dies (requires
        recovery_config; without it a dropped chain waits for resume).
        """
        from horsies.core.workflows.engine import (
            on_subworkflow_complete,
            pop_pending_parent_propagations,
        )

        worklist = list(child_workflow_ids)
        while worklist:
            child_id = worklist.pop(0)
            try:
                async with self.sf() as s:
                    await on_subworkflow_complete(s, child_id, self.broker)
                    await s.commit()
                    worklist.extend(pop_pending_parent_propagations(s))
            except Exception as exc:
                await self._handle_finalize_error(
                    self._make_finalize_error(
                        task_id=child_id,
                        stage=_FINALIZE_STAGE_PARENT,
                        message='Parent propagation failed after child workflow completed',
                        retryable=is_retryable_connection_error(exc),
                        data={
                            'child_workflow_ids': [child_id, *worklist],
                            'exception_type': type(exc).__name__,
                            'exception': str(exc)[:500],
                        },
                    )
                )
                return
            self._clear_finalize_retry_attempts(child_id, _FINALIZE_STAGE_PARENT)

    async def _retry_finalize_parent(self, err: _FinalizeError, delay_s: float) -> None:
        """Re-enter the parent-propagation chain from the captured worklist.

        Total by composition: every subcall either returns a Result/outcome
        value or is itself total. Cancellation is the exemption: it escapes
        every await and the done-callback drops it silently. Any other
        escape is logged by the background-task done-callback.
        """
        await self._sleep_with_stop(delay_s)
        if self._stop.is_set():
            return

        raw_ids = (err.data or {}).get('child_workflow_ids')
        if not isinstance(raw_ids, list) or not all(
            isinstance(i, str) for i in cast('list[Any]', raw_ids)
        ):
            logger.error(
                f'Parent-propagation retry missing worklist payload for {err.task_id}'
            )
            self._clear_finalize_retry_attempts(err.task_id, _FINALIZE_STAGE_PARENT)
            return
        await self._drive_parent_propagations(cast('list[str]', raw_ids))

    async def _handle_workflow_task_if_needed(
        self,
        session: 'AsyncSession',
        task_id: str,
        result: 'TaskResult[Any, TaskError]',
        *,
        is_workflow_task: bool = True,
        task_name: str,
    ) -> None:
        """
        Check if task is part of a workflow and handle accordingly.

        This method is called after a task completes (success or failure).
        It updates the workflow_task record and triggers dependency resolution.

        The old pre-flight existence check is gone: the engine's single
        locate+lock+CAS statement returns zero rows for a non-workflow
        task and the call no-ops, at the same one-statement cost the
        check itself had.

        Raises:
            Exception: DB and engine errors propagate to the single caller,
                ``_finalize_workflow_phase``, which folds them into
                ``Err(_FinalizeError(stage=phase2))`` with retryable
                classification.
        """
        from horsies.core.workflows.engine import on_workflow_task_complete

        if not is_workflow_task:
            return

        # Handle workflow task completion
        await on_workflow_task_complete(
            session,
            task_id,
            result,
            self.broker,
            task_name=task_name,
        )
