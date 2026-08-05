"""Task finalization: persist terminal state, workflow phase, retries.

FinalizeMixin owns the two-phase finalize pipeline: await the child
future (with optional timeout), persist the terminal row + attempt
history (phase 1), run the workflow completion phase (phase 2), and the
bounded retry machinery for failures in any stage. Worker-internal
mixin: the ``TYPE_CHECKING`` block declares the slice of ``Worker`` it
relies on.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal, Optional, assert_never, cast

from pydantic import ValidationError
from sqlalchemy import text

from horsies.core.codec.json_io import SerializationError, loads_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.payload_guard import enforce_payload_policy
from horsies.core.codec.typed import (
    decode_task_error,
    decode_task_result,
    validate_task_result_envelope,
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
    LostClaim,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.lifecycle.persistence import (
    apply_async,
    classify_locked_read_miss_async,
)
from horsies.core.logging import get_logger
from horsies.core.models.tasks import (
    BuiltInTaskCode,
    OperationalErrorCode,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.worker.runtime import (
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
    _FinalizeError,
    _RequeueOutcome,
    _RetryError,
)
from horsies.core.worker.sql import (
    NOTIFY_TASK_QUEUE_SQL,
    SELECT_RUNNING_TASK_CONTEXT_FOR_UPDATE_SQL,
    TERMINATE_ORPHANED_WORKFLOW_TASK_SQL,
    UPSERT_TASK_ATTEMPT_SQL,
)

if TYPE_CHECKING:
    from collections.abc import Coroutine
    from concurrent.futures import ProcessPoolExecutor

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from horsies.core.app import Horsies
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.worker.config import WorkerConfig

logger = get_logger('worker')

GET_TASK_STATUS_RESULT_SQL = text("""
    SELECT status, task_name, queue_name, is_workflow_task, result
    FROM horsies_tasks
    WHERE id = :id
""")


@dataclass(frozen=True, slots=True)
class _PersistedTaskOutcome:
    """Terminal result and phase-2 routing facts read from one row."""

    result: TaskResult[Any, TaskError]
    task_name: str
    queue_name: str
    is_workflow_task: bool


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
        _finalize_retry_attempts: dict[tuple[str, str], int]

        # Cross-concern methods provided by sibling mixins / Worker.
        async def _handle_broken_pool(
            self,
            task_id: str,
            exc: BaseException,
            failed_executor: ProcessPoolExecutor,
            claimed_at: Optional[datetime] = None,
        ) -> None: ...
        async def _handle_task_timeout(
            self,
            task_id: str,
            timeout_ms: int,
            claimed_at: Optional[datetime] = None,
        ) -> None: ...
        async def _recover_worker_future_failure(
            self,
            task_id: str,
            reason: str,
            claimed_at: Optional[datetime] = None,
        ) -> _RequeueOutcome: ...
        async def _should_retry_task(
            self, task_id: str, error: TaskError, session: AsyncSession
        ) -> Result[bool, _RetryError]: ...
        async def _schedule_retry(
            self, task_id: str, session: AsyncSession, queue_name: str
        ) -> Result[
            Literal['scheduled', 'reaper_reclaimed', 'expired'], _RetryError
        ]: ...
        async def _sleep_with_stop(self, delay_seconds: float) -> None: ...
        def _spawn_background(
            self,
            coro: Coroutine[Any, Any, Any],
            *,
            name: str,
            finalizer: bool = False,
            service: bool = False,
        ) -> asyncio.Task[Any]: ...

    async def _finalize_after(
        self,
        fut: 'asyncio.Future[tuple[bool, str, Optional[str]]]',
        task_id: str,
        queue_name: str = 'default',
        is_workflow_task: bool = True,
        timeout_ms: Optional[int] = None,
        *,
        executor: ProcessPoolExecutor,
        task_name: str,
        claimed_at: Optional[datetime] = None,
    ) -> Result[None, _FinalizeError]:
        try:
            if timeout_ms is not None:
                try:
                    # shield: wait_for must not cancel the executor future —
                    # the child keeps running regardless, and the original
                    # future is awaited again below after the kill.
                    ok, result_json_str, failed_reason = await asyncio.wait_for(
                        asyncio.shield(fut),
                        timeout=timeout_ms / 1000.0,
                    )
                except asyncio.TimeoutError:
                    await self._handle_task_timeout(
                        task_id,
                        timeout_ms,
                        claimed_at=claimed_at,
                    )
                    # The SIGKILL breaks the shared process pool; awaiting
                    # the original future routes into the BrokenProcessPool
                    # branch below, which restarts the executor (sibling
                    # tasks recover through their own finalizers).
                    ok, result_json_str, failed_reason = await fut
            else:
                ok, result_json_str, failed_reason = await fut
        except asyncio.CancelledError:
            raise
        except BrokenProcessPool as exc:
            await self._handle_broken_pool(
                task_id,
                exc,
                executor,
                claimed_at=claimed_at,
            )
            # By this point _handle_broken_pool has already recovered the
            # task (requeue per its retry policy) and replaced the executor;
            # the message states that so the log line cannot be read as a
            # lost task.
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_FUTURE,
                    message=(
                        f'Broken process pool during finalize; task already '
                        f'recovered per its retry policy and the pool '
                        f'replaced: {exc}'
                    ),
                    retryable=False,
                    data={
                        'exception_type': type(exc).__name__,
                        'claimed_at': claimed_at,
                    },
                )
            )
        except Exception as exc:
            requeue_outcome = await self._recover_worker_future_failure(
                task_id,
                f'Worker future failed before result: {exc}',
                claimed_at=claimed_at,
            )
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_FUTURE,
                    message=f'Worker future failed before result: {exc}',
                    # Retryability is keyed on the recovery DB failure, not
                    # the child-future exception: a non-connection future
                    # error whose requeue hit a transient DB blip must still
                    # retry the recovery instead of waiting on the reaper.
                    retryable=requeue_outcome is _RequeueOutcome.DB_ERROR,
                    data={
                        'exception_type': type(exc).__name__,
                        'requeue_outcome': requeue_outcome.value,
                        'claimed_at': claimed_at,
                    },
                )
            )
        now = datetime.now(timezone.utc)

        # Warn-only: results are never rejected over size (the work is
        # done; destroying it would convert a size concern into data loss).
        if self._app is not None:
            enforce_payload_policy(
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

    def _make_finalize_error(
        self,
        *,
        task_id: str,
        stage: str,
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

    def _finalize_error_from_retry_error(
        self,
        err: _RetryError,
        *,
        ok: bool,
        result_json_str: str,
        failed_reason: str | None,
    ) -> _FinalizeError:
        """Translate a retry-concern DB failure into a phase1 finalize error.

        The outcome triple must ride along in ``data``: ``_retry_finalize_phase1``
        replays ``_persist_task_terminal_state`` from ``data['outcome']`` and
        abandons the retry if it is missing.
        """
        return _FinalizeError(
            error_code=err.error_code,
            message=err.message,
            stage=_FINALIZE_STAGE_PHASE1,
            task_id=err.task_id,
            retryable=err.retryable,
            data={
                **(err.data or {}),
                'outcome': {
                    'ok': ok,
                    'result_json_str': result_json_str,
                    'failed_reason': failed_reason,
                },
            },
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

    def _clear_finalize_retry_attempts(self, task_id: str, stage: str) -> None:
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
        operation. Every other normal terminal result uses the generation-
        fenced locked read, an attempt write, and its operation function in one
        caller-owned transaction. A retry of a committed phase 1 is classified
        as ``AlreadyApplied`` and reloads the persisted result for phase 2;
        ``LostClaim`` and other refusals do not advance the workflow.
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
                        except (StrictJsonError, ValidationError) as exc:
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
                            except (StrictJsonError, ValidationError) as exc:
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
            case ApplyTerminalization(command=CancelOwnedOrphan()):
                # The orphan family migrates in T6. Classification is shared
                # now, but its legacy writer remains until that gated cutover.
                try:
                    async with self.sf() as s:
                        terminate_res = await s.execute(
                            TERMINATE_ORPHANED_WORKFLOW_TASK_SQL,
                            {
                                'id': task_id,
                                'wid': self.worker_instance_id,
                                'claimed_at': claimed_at,
                            },
                        )
                        terminated = terminate_res.fetchone() is not None
                        await s.commit()
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
                    f'{type(unsupported).__name__} is not part of the T3 '
                    f'normal-finalization cutover'
                )
            case _ as unreachable:
                assert_never(unreachable)

        if fused_command is not None:
            try:
                async with self.sf() as s:
                    outcome = await apply_async(
                        await s.connection(),
                        fused_command,
                    )
                    match outcome:
                        case Applied():
                            await s.commit()
                        case (
                            AlreadyApplied()
                            | LostClaim()
                            | SourceStateConflict()
                            | TaskAbsent()
                        ):
                            await s.rollback()
                        case _ as unreachable:
                            assert_never(unreachable)
                    return Ok(None)
            except Exception as exc:
                return Err(
                    self._make_finalize_error(
                        task_id=task_id,
                        stage=_FINALIZE_STAGE_PHASE1,
                        message='Failed to persist terminal task state',
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

        try:
            async with self.sf() as s:
                ctx_result = await s.execute(
                    SELECT_RUNNING_TASK_CONTEXT_FOR_UPDATE_SQL,
                    {
                        'id': task_id,
                        'wid': self.worker_instance_id,
                        'claimed_at': claimed_at,
                    },
                )
                ctx_row = ctx_result.fetchone()
                if ctx_row is None:
                    miss_command = locked_command
                    if miss_command is None:
                        assert retry_error is not None
                        miss_command = _refused_retry_command(
                            retry_error,
                            context,
                        )
                    miss_outcome = await classify_locked_read_miss_async(
                        await s.connection(),
                        miss_command,
                        claimed_at=claimed_at,
                    )
                    match miss_outcome:
                        case AlreadyApplied():
                            load_r = await self._load_persisted_task_result(
                                task_id,
                                session=s,
                            )
                            await s.rollback()
                            if is_err(load_r):
                                return Err(load_r.err_value)
                            return load_r
                        case LostClaim() | SourceStateConflict() | TaskAbsent():
                            await s.rollback()
                            return Ok(None)
                        case Applied():
                            await s.rollback()
                            raise RuntimeError(
                                'locked-read miss classifier returned APPLIED'
                            )
                        case _ as unreachable:
                            assert_never(unreachable)
                else:
                    attempt_num = (ctx_row.retry_count or 0) + 1
                    db_now = ctx_row.db_now or now
                    attempt_started_at = ctx_row.started_at or db_now
                    attempt_worker = {
                        'worker_id': ctx_row.claimed_by_worker_id,
                        'worker_hostname': ctx_row.worker_hostname,
                        'worker_pid': ctx_row.worker_pid,
                        'worker_process_name': ctx_row.worker_process_name,
                    }

                    if retry_error is not None:
                        failure_command = _refused_retry_command(
                            retry_error,
                            context,
                        )
                        should_retry_r = await self._should_retry_task(
                            task_id,
                            retry_error,
                            s,
                        )
                        if is_err(should_retry_r):
                            return Err(
                                self._finalize_error_from_retry_error(
                                    should_retry_r.err_value,
                                    ok=ok,
                                    result_json_str=result_json_str,
                                    failed_reason=failed_reason,
                                ),
                            )
                        if should_retry_r.ok_value:
                            retry_r = await self._schedule_retry(
                                task_id,
                                s,
                                queue_name=ctx_row.queue_name or 'default',
                            )
                            if is_err(retry_r):
                                return Err(
                                    self._finalize_error_from_retry_error(
                                        retry_r.err_value,
                                        ok=ok,
                                        result_json_str=result_json_str,
                                        failed_reason=failed_reason,
                                    ),
                                )
                            match retry_r.ok_value:
                                case 'scheduled':
                                    await s.execute(
                                        UPSERT_TASK_ATTEMPT_SQL,
                                        {
                                            'task_id': task_id,
                                            'attempt': attempt_num,
                                            'outcome': 'FAILED',
                                            'will_retry': True,
                                            'started_at': attempt_started_at,
                                            'finished_at': db_now,
                                            'error_code': failure_command.error_code,
                                            'error_message': retry_error.message,
                                            'failed_reason': None,
                                            **attempt_worker,
                                        },
                                    )
                                    await s.commit()
                                    return Ok(None)
                                case 'reaper_reclaimed':
                                    logger.warning(
                                        'Task %s retry aborted during finalize: '
                                        'task no longer RUNNING',
                                        task_id,
                                    )
                                    await s.commit()
                                    return Ok(None)
                                case 'expired':
                                    logger.info(
                                        'Task %s retry skipped: good_until '
                                        'exceeded; terminalizing failure',
                                        task_id,
                                    )
                        locked_command = failure_command

                    assert locked_command is not None
                    assert tr is not None
                    match (report, locked_command):
                        case (
                            WorkerLevelFailure(detail=detail),
                            FailLockedTask(error_code=error_code),
                        ):
                            message = detail or 'Worker failure'
                            attempt_outcome = 'WORKER_FAILURE'
                            attempt_error_code = error_code
                            attempt_error_message = message
                            attempt_failed_reason = message
                        case (
                            ResultUndecodable(detail=detail),
                            FailLockedTask(error_code=error_code),
                        ):
                            attempt_outcome = 'FAILED'
                            attempt_error_code = error_code
                            attempt_error_message = detail
                            attempt_failed_reason = None
                        case (
                            ErrorProduced(error=error),
                            FailLockedTask(error_code=error_code),
                        ):
                            attempt_outcome = 'FAILED'
                            attempt_error_code = error_code
                            attempt_error_message = error.message
                            attempt_failed_reason = None
                        case (ResultProduced(), CompleteLockedTask()):
                            attempt_outcome = 'COMPLETED'
                            attempt_error_code = None
                            attempt_error_message = None
                            attempt_failed_reason = None
                        case unexpected:
                            raise RuntimeError(
                                'terminal report and locked command disagree: '
                                f'{type(unexpected[0]).__name__} / '
                                f'{type(unexpected[1]).__name__}'
                            )

                    await s.execute(
                        UPSERT_TASK_ATTEMPT_SQL,
                        {
                            'task_id': task_id,
                            'attempt': attempt_num,
                            'outcome': attempt_outcome,
                            'will_retry': False,
                            'started_at': attempt_started_at,
                            'finished_at': db_now,
                            'error_code': attempt_error_code,
                            'error_message': attempt_error_message,
                            'failed_reason': attempt_failed_reason,
                            **attempt_worker,
                        },
                    )
                    applied_outcome = await apply_async(
                        await s.connection(),
                        locked_command,
                    )
                    match applied_outcome:
                        case Applied():
                            await s.commit()
                            return Ok(
                                _PersistedTaskOutcome(
                                    result=tr,
                                    task_name=str(ctx_row.task_name or ''),
                                    queue_name=str(ctx_row.queue_name or 'default'),
                                    is_workflow_task=bool(ctx_row.is_workflow_task),
                                )
                            )
                        case AlreadyApplied():
                            load_r = await self._load_persisted_task_result(
                                task_id,
                                session=s,
                            )
                            await s.rollback()
                            if is_err(load_r):
                                return Err(load_r.err_value)
                            return load_r
                        case LostClaim() | SourceStateConflict() | TaskAbsent():
                            await s.rollback()
                            return Ok(None)
                        case _ as unreachable:
                            assert_never(unreachable)

            return Ok(None)
        except Exception as exc:
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE1,
                    message='Failed to persist terminal task state',
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

        Parent propagation queued by the completion check is driven AFTER
        this transaction commits, one fresh transaction per ancestor level
        (_drive_parent_propagations) — the pre-denest in-transaction
        recursion accumulated FOR UPDATE locks root-ward across the chain.
        """
        from horsies.core.workflows.engine import pop_pending_parent_propagations

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
                try:
                    payload = f'capacity:{task_id}'
                    await s.execute(
                        NOTIFY_TASK_QUEUE_SQL,
                        {'c2': f'task_queue_{queue_name or "default"}', 'p': payload},
                    )
                except Exception:
                    # Non-fatal if NOTIFY fails; workflow state is already persisted.
                    pass

                # Trigger automatically sends NOTIFY on UPDATE; commit to flush NOTIFYs
                await s.commit()
                pending_propagations = pop_pending_parent_propagations(s)
        except Exception as exc:
            return Err(
                self._make_finalize_error(
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
            )

        # Outside the try deliberately: a phase2 retry cannot re-reach
        # propagation past the task-level terminal CAS, so propagation must
        # never fold into a phase2 error. The driver is total — each level's
        # failure goes to the parent_propagation retry stage.
        if pending_propagations:
            await self._drive_parent_propagations(pending_propagations)
        return Ok(None)

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

    async def _read_persisted_task_result(
        self,
        task_id: str,
        session: AsyncSession,
    ) -> Result[_PersistedTaskOutcome, _FinalizeError]:
        """Read and decode a persisted result through the supplied transaction."""
        res = await session.execute(GET_TASK_STATUS_RESULT_SQL, {'id': task_id})
        row = res.fetchone()
        if row is None:
            return Err(
                self._make_finalize_error(
                    task_id=task_id,
                    stage=_FINALIZE_STAGE_PHASE2,
                    message='Cannot replay finalize phase-2: task row not found',
                    retryable=False,
                )
            )
        status = str(row.status) if row.status is not None else ''
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
            except (StrictJsonError, ValidationError) as exc:
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
        except (StrictJsonError, ValidationError) as exc:
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
        boundary defense. Retry coroutines are spawned on the running loop
        (done-callbacks only execute while the loop runs, so there is no
        closed-loop raise window); their failures terminate at the
        done-callback.
        """
        if not isinstance(err, _FinalizeError):
            logger.error(
                f'Unexpected finalize error payload type: {type(err).__name__}'
            )
            return

        stage = err.stage
        task_id = err.task_id
        if stage == _FINALIZE_STAGE_FUTURE:
            max_attempts = _FINALIZE_FUTURE_MAX_RETRIES
        elif stage == _FINALIZE_STAGE_PHASE1:
            max_attempts = _FINALIZE_PHASE1_MAX_RETRIES
        elif stage == _FINALIZE_STAGE_PHASE2:
            max_attempts = _FINALIZE_PHASE2_MAX_RETRIES
        elif stage == _FINALIZE_STAGE_PARENT:
            max_attempts = _FINALIZE_PARENT_MAX_RETRIES
        else:
            logger.error(
                f'Finalize error ({task_id}) stage={stage}: {err.message}; data={err.data}'
            )
            return

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
                f'stage={stage}: {err.message}; data={err.data}'
            )
            self._clear_finalize_retry_attempts(task_id, stage)
            return

        if attempts >= max_attempts:
            logger.critical(
                f'Finalize retries exhausted for task {task_id} stage={stage} after {attempts} attempts: '
                f'{err.message}; data={err.data}'
            )
            self._clear_finalize_retry_attempts(task_id, stage)
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

        if stage == _FINALIZE_STAGE_FUTURE:
            self._spawn_background(
                self._retry_finalize_future(err, delay),
                name=f'finalize-retry-future-{task_id}',
                finalizer=True,
            )
        elif stage == _FINALIZE_STAGE_PHASE1:
            self._spawn_background(
                self._retry_finalize_phase1(err, delay),
                name=f'finalize-retry-phase1-{task_id}',
                finalizer=True,
            )
        elif stage == _FINALIZE_STAGE_PARENT:
            self._spawn_background(
                self._retry_finalize_parent(err, delay),
                name=f'finalize-retry-parent-{task_id}',
                finalizer=True,
            )
        else:
            self._spawn_background(
                self._retry_finalize_phase2(err, delay),
                name=f'finalize-retry-phase2-{task_id}',
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
        outcome = await self._recover_worker_future_failure(
            err.task_id,
            f'Retry after future-stage finalize error: {err.message}',
            claimed_at=(
                claimed_at_raw if isinstance(claimed_at_raw, datetime) else None
            ),
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
