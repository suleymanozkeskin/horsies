"""Reaper: stale-task recovery, workflow recovery, retention cleanup.

ReaperMixin holds the periodic reaper loop and its gated pass. It is a
worker-internal mixin: the ``TYPE_CHECKING`` block declares the slice of
``Worker`` state it relies on, and ``Worker`` is the only intended
subclass.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import hashlib
import time
from typing import TYPE_CHECKING, Any, Literal

from horsies.core.logging import get_logger
from horsies.core.types.result import Err, Ok, is_err
from horsies.core.worker.runtime import (
    _REAPER_MAX_PERMANENT_FAILURES,
    _ReaperPassState,
)
from horsies.core.worker.sql import (
    DELETE_EXPIRED_WORKER_STATES_SQL,
    DELETE_EXPIRED_WORKFLOWS_SQL,
    REAPER_GATE_TRY_LOCK_SQL,
    _RETENTION_PASS_TIME_BUDGET_S,
)

if TYPE_CHECKING:
    from sqlalchemy import TextClause
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from horsies.core.app import Horsies
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.recovery import RecoveryConfig
    from horsies.core.worker.config import WorkerConfig

logger = get_logger('worker')


class ReaperMixin:
    """Periodic reaper loop, advisory gate, and the recovery/retention pass."""

    if TYPE_CHECKING:
        # Worker state this mixin reads (provided by the composed Worker).
        sf: async_sessionmaker[AsyncSession]
        cfg: WorkerConfig
        broker: PostgresBroker | None
        _app: Horsies | None
        _stop: asyncio.Event
        _partition_coverage_health: dict[str, Any] | None

    def _advisory_key_reaper(self) -> int:
        """Fixed 64-bit key gating reaper passes (one reaper per interval)."""
        h = hashlib.sha256(b'horsies:reaper:v1').digest()
        return int.from_bytes(h[:8], byteorder='big', signed=True)

    def _reaper_gate_enabled(self) -> bool:
        """Whether the advisory reaper gate can be used without starving the pool.

        A gated pass needs two simultaneous coordinator connections: the
        gate session holding the xact lock, plus the pass body's own
        session. With total pool capacity below two, the second checkout
        blocks until pool timeout — so the gate must be skipped.
        """
        return (self.cfg.parent_pool_size + self.cfg.parent_max_overflow) >= 2

    # Stale detection is handled via heartbeat policy for RUNNING tasks.

    async def _run_reaper_pass(
        self,
        temp_broker: 'PostgresBroker',
        recovery_cfg: 'RecoveryConfig',
        state: _ReaperPassState,
    ) -> None:
        """One reaper pass: stale-task recovery, pending expiry, workflow
        recovery, retention cleanup. Runs under the cluster-wide reaper
        gate (one executing worker per interval)."""
        # Auto-requeue stale CLAIMED tasks
        if recovery_cfg.auto_requeue_stale_claimed and not state.requeue_disabled:
            match await temp_broker.requeue_stale_claimed(
                stale_threshold_ms=recovery_cfg.claimed_stale_threshold_ms,
            ):
                case Ok(requeued):
                    state.requeue_permanent_failures = 0
                    if requeued > 0:
                        logger.info(
                            f'Reaper requeued {requeued} stale CLAIMED task(s)',
                        )
                case Err(err) if err.retryable:
                    state.requeue_permanent_failures = 0
                    logger.warning(
                        f'Reaper requeue_stale_claimed transient failure '
                        f'(will retry next cycle): {err.message}',
                    )
                case Err(err):
                    state.requeue_permanent_failures += 1
                    if (
                        state.requeue_permanent_failures
                        >= _REAPER_MAX_PERMANENT_FAILURES
                    ):
                        state.requeue_disabled = True
                        logger.critical(
                            f'Reaper requeue_stale_claimed disabled after '
                            f'{state.requeue_permanent_failures} consecutive permanent '
                            f'failures. Last error: {err.message}. '
                            f'Requires deploy or manual intervention.',
                        )
                    else:
                        logger.error(
                            f'Reaper requeue_stale_claimed permanent failure '
                            f'({state.requeue_permanent_failures}/{_REAPER_MAX_PERMANENT_FAILURES} '
                            f'before disable): {err.message}',
                        )

        # Cancel orphaned workflow tasks (no live workflow_task linkage). These
        # cannot reach RUNNING, so requeue (above) skips them and they would
        # otherwise stay CLAIMED forever; cancelling frees budget and lets
        # retention sweep them.
        if (
            recovery_cfg.auto_terminate_orphaned_workflow_tasks
            and not state.terminate_orphans_disabled
        ):
            match await temp_broker.terminate_orphaned_workflow_tasks():
                case Ok(terminated):
                    state.terminate_orphans_permanent_failures = 0
                    if terminated > 0:
                        logger.warning(
                            f'Reaper cancelled {terminated} orphaned workflow '
                            f'task(s) (no live workflow_task linkage)',
                        )
                case Err(err) if err.retryable:
                    state.terminate_orphans_permanent_failures = 0
                    logger.warning(
                        f'Reaper terminate_orphaned_workflow_tasks transient '
                        f'failure (will retry next cycle): {err.message}',
                    )
                case Err(err):
                    state.terminate_orphans_permanent_failures += 1
                    if (
                        state.terminate_orphans_permanent_failures
                        >= _REAPER_MAX_PERMANENT_FAILURES
                    ):
                        state.terminate_orphans_disabled = True
                        logger.critical(
                            f'Reaper terminate_orphaned_workflow_tasks disabled '
                            f'after {state.terminate_orphans_permanent_failures} '
                            f'consecutive permanent failures. Last error: '
                            f'{err.message}. Requires deploy or manual '
                            f'intervention.',
                        )
                    else:
                        logger.error(
                            f'Reaper terminate_orphaned_workflow_tasks permanent '
                            f'failure ({state.terminate_orphans_permanent_failures}/'
                            f'{_REAPER_MAX_PERMANENT_FAILURES} before disable): '
                            f'{err.message}',
                        )

        # Auto-fail stale RUNNING tasks
        if (
            recovery_cfg.auto_fail_stale_running
            and not state.mark_failed_disabled
        ):
            match await temp_broker.mark_stale_tasks_as_failed(
                stale_threshold_ms=recovery_cfg.running_stale_threshold_ms,
                finalizing_stale_threshold_ms=(
                    recovery_cfg.finalizing_stale_threshold_ms
                ),
            ):
                case Ok(failed):
                    state.mark_failed_permanent_failures = 0
                    if failed > 0:
                        logger.warning(
                            f'Reaper marked {failed} stale RUNNING task(s) as FAILED',
                        )
                case Err(err) if err.retryable:
                    state.mark_failed_permanent_failures = 0
                    logger.warning(
                        f'Reaper mark_stale_tasks_as_failed transient failure '
                        f'(will retry next cycle): {err.message}',
                    )
                case Err(err):
                    state.mark_failed_permanent_failures += 1
                    if (
                        state.mark_failed_permanent_failures
                        >= _REAPER_MAX_PERMANENT_FAILURES
                    ):
                        state.mark_failed_disabled = True
                        logger.critical(
                            f'Reaper mark_stale_tasks_as_failed disabled after '
                            f'{state.mark_failed_permanent_failures} consecutive permanent '
                            f'failures. Last error: {err.message}. '
                            f'Requires deploy or manual intervention.',
                        )
                    else:
                        logger.error(
                            f'Reaper mark_stale_tasks_as_failed permanent failure '
                            f'({state.mark_failed_permanent_failures}/{_REAPER_MAX_PERMANENT_FAILURES} '
                            f'before disable): {err.message}',
                        )

        # Expire PENDING tasks past good_until deadline
        try:
            match await temp_broker.expire_pending_tasks():
                case Ok(expired):
                    if expired > 0:
                        logger.info(
                            f'Reaper expired {expired} PENDING task(s) past good_until',
                        )
                case Err(err):
                    logger.warning(
                        f'Reaper expire_pending_tasks failed: {err.message}',
                    )
        except Exception as expire_err:
            logger.error(f'Reaper expire_pending_tasks error: {expire_err}')

        # Recover stuck workflows
        try:
            from horsies.core.workflows.recovery import (
                recover_stuck_workflows,
            )

            async with temp_broker.session_factory() as s:
                recovered = await recover_stuck_workflows(
                    s,
                    temp_broker,
                    finalizing_grace_ms=(
                        recovery_cfg.crashed_worker_recovery_grace_ms
                    ),
                )
                if recovered > 0:
                    logger.info(
                        f'Reaper recovered {recovered} stuck workflow task(s)'
                    )
                await s.commit()
        except Exception as wf_err:
            logger.error(f'Workflow recovery error: {wf_err}')

        now_monotonic = time.monotonic()
        if now_monotonic >= state.next_retention_cleanup_at:
            try:
                deleted_worker_states = 0
                deleted_workflows = 0

                # Shared wall-clock budget across both statements. A
                # backlog that outlives the budget resumes next pass.
                # Terminal TASK rows move to the task-history archive at
                # terminalization and age by retention class; heartbeat
                # rows live in partitions that drop whole — neither is
                # row-deleted here anymore.
                deadline = now_monotonic + _RETENTION_PASS_TIME_BUDGET_S

                batch_size = recovery_cfg.retention_delete_batch_size

                if recovery_cfg.worker_state_retention_hours is not None:
                    deleted_worker_states = await self._delete_expired_in_batches(
                        temp_broker,
                        DELETE_EXPIRED_WORKER_STATES_SQL,
                        {
                            'retention_hours': recovery_cfg.worker_state_retention_hours,
                        },
                        deadline,
                        batch_size,
                    )

                if recovery_cfg.terminal_record_retention_hours is not None:
                    # Workflows and their node rows go together in one
                    # workflow-batched statement (node purge is a CTE inside
                    # it). The all-backing-tasks-terminal guard makes
                    # partial progress safe. The statement's rowcount
                    # counts workflows, which the node budget keeps below
                    # :batch_size while backlog remains — hence
                    # drained_when='empty_batch'.
                    deleted_workflows = await self._delete_expired_in_batches(
                        temp_broker,
                        DELETE_EXPIRED_WORKFLOWS_SQL,
                        {
                            'retention_hours': recovery_cfg.terminal_record_retention_hours,
                        },
                        deadline,
                        batch_size,
                        drained_when='empty_batch',
                    )

                if deleted_worker_states > 0 or deleted_workflows > 0:
                    logger.info(
                        'Reaper retention cleanup: worker_states=%s workflows=%s',
                        deleted_worker_states,
                        deleted_workflows,
                    )
            except Exception as cleanup_err:
                logger.error(f'Retention cleanup error: {cleanup_err}')
            finally:
                state.next_retention_cleanup_at = (
                    now_monotonic + recovery_cfg.retention_sweep_interval_s
                )

        # Paused-age expiry: the declared policy's sweep. Gated on the
        # opt-in knob (None = the pass reads nothing) and running under
        # the same cluster-wide reaper gate; the writer is the cancel
        # path's locked sequence with EXPIRED as the terminal status,
        # so an expiring workflow leaves no claimable backing rows and
        # no ENQUEUED nodes behind.
        if recovery_cfg.paused_workflow_auto_cancel_after is not None:
            from horsies.core.workflows.lifecycle import (
                expire_paused_workflows,
            )

            try:
                expired = await expire_paused_workflows(
                    temp_broker.session_factory,
                    older_than=(
                        recovery_cfg.paused_workflow_auto_cancel_after
                    ),
                )
                if expired > 0:
                    logger.info(
                        f'Expired {expired} paused workflow(s) past the '
                        f'declared age policy'
                    )
            except Exception as expiry_err:
                logger.error(
                    f'Paused-workflow expiry sweep error: {expiry_err}'
                )

        # Coverage/publication maintenance: keep history and heartbeat
        # leaves created ahead of writes and the staged readers
        # published. Rides the reaper's cluster-wide gate, so one
        # worker per interval runs it; a refusal is reported verbatim
        # and never retried into silence — existing leaves keep
        # absorbing writes until the horizon, and the next pass tries
        # again.
        if now_monotonic >= state.next_partition_maintenance_at:
            from horsies.core.history.maintenance.coverage import (
                CoverageEnsureFailed,
                CoverageEnsured,
                ensure_partition_coverage,
            )

            try:
                async with temp_broker.async_engine.begin() as coverage_conn:
                    coverage = await ensure_partition_coverage(
                        coverage_conn,
                        history_horizon_days=(
                            recovery_cfg.history_leaf_horizon_days
                        ),
                        heartbeat_horizon_hours=(
                            recovery_cfg.heartbeat_leaf_horizon_hours
                        ),
                    )
                match coverage:
                    case CoverageEnsureFailed():
                        logger.error(
                            f'Partition coverage ensure failed: {coverage!r}'
                        )
                        self._partition_coverage_health = {
                            'state': 'failed',
                            'stage': coverage.stage,
                            'class_key': coverage.class_key,
                            'refusal': coverage.refusal,
                            'heartbeat_covered_now': (
                                coverage.heartbeat_covered_now
                            ),
                        }
                    case CoverageEnsured(
                        created_history_leaves=created_history,
                        created_heartbeat_leaves=created_heartbeats,
                        republished=republished,
                    ) if created_history or created_heartbeats or republished:
                        logger.info(
                            'Partition coverage: '
                            f'+{created_history} history leaves, '
                            f'+{created_heartbeats} heartbeat leaves, '
                            f'republished={republished}'
                        )
                    case _:
                        pass
                match coverage:
                    case CoverageEnsured():
                        self._partition_coverage_health = {
                            'state': 'ensured',
                            'heartbeat_covered_now': (
                                coverage.heartbeat_covered_now
                            ),
                            'history_covered_through': (
                                coverage.history_covered_through.isoformat()
                            ),
                            'heartbeats_covered_through': (
                                coverage.heartbeats_covered_through.isoformat()
                            ),
                        }
                    case _:
                        pass
            except Exception as coverage_err:
                logger.error(
                    f'Partition coverage ensure error: {coverage_err}'
                )
            finally:
                state.next_partition_maintenance_at = (
                    now_monotonic
                    + recovery_cfg.partition_maintenance_interval_s
                )

    async def _delete_expired_in_batches(
        self,
        temp_broker: 'PostgresBroker',
        statement: 'TextClause',
        params: dict[str, Any],
        deadline_monotonic: float,
        batch_size: int,
        *,
        drained_when: Literal['short_batch', 'empty_batch'] = 'short_batch',
    ) -> int:
        """Run one retention DELETE in bounded batches, committing per batch.

        Always runs at least one batch; stops when the backlog reads as
        drained or the pass deadline is reached (backlog resumes next pass).
        Per-batch commits bound transaction size so a large backlog never
        becomes one unbounded DELETE.

        ``drained_when`` names the drained signal: ``short_batch`` for
        statements whose rowcount equals the rows the batch selects (a short
        batch means nothing eligible is left); ``empty_batch`` for the
        workflow statement, whose rowcount counts workflows while its node
        budget keeps batches routinely short of ``batch_size`` — there only
        a zero-row batch means drained, at the cost of one empty statement
        per drained pass.
        """
        total_deleted = 0
        while True:
            async with temp_broker.session_factory() as s:
                result = await s.execute(
                    statement,
                    {**params, 'batch_size': batch_size},
                )
                deleted = int(getattr(result, 'rowcount', 0) or 0)
                await s.commit()
            total_deleted += deleted
            match drained_when:
                case 'short_batch':
                    drained = deleted < batch_size
                case 'empty_batch':
                    drained = deleted == 0
            if drained:
                return total_deleted
            if time.monotonic() >= deadline_monotonic:
                logger.info(
                    'Retention pass time budget reached; %s rows deleted, '
                    'remaining backlog resumes next pass',
                    total_deleted,
                )
                return total_deleted

    async def _reaper_loop(self) -> None:
        """Automatic stale task handling loop.

        Periodically checks for and recovers stale tasks based on RecoveryConfig:
        - Requeues tasks stuck in CLAIMED (safe - user code never ran)
        - Marks stale RUNNING tasks as FAILED (not safe to requeue)
        - Prunes old telemetry and terminal records based on retention settings

        Non-retryable errors (schema drift, code bugs) are tracked per-operation.
        After _REAPER_MAX_PERMANENT_FAILURES consecutive permanent failures, the
        failing operation is disabled for the process lifetime to avoid spamming
        logs every check_interval_ms forever.
        """
        if not self.cfg.recovery_config:
            return

        recovery_cfg = self.cfg.recovery_config
        check_interval_ms = recovery_cfg.check_interval_ms
        temp_broker = None
        owns_temp_broker = False
        state = _ReaperPassState(
            next_retention_cleanup_at=time.monotonic(),
        )

        logger.info(
            f'Reaper configuration: auto_requeue_claimed={recovery_cfg.auto_requeue_stale_claimed}, '
            f'auto_fail_running={recovery_cfg.auto_fail_stale_running}, '
            f'check_interval={check_interval_ms}ms ({check_interval_ms/1000:.1f}s)',
        )

        try:
            # Prefer the worker-owned broker to avoid creating redundant
            # engines/listeners or falling back to producer-sized pool defaults.
            temp_broker = self.broker

            if temp_broker is None:
                from horsies.core.brokers.postgres import PostgresBroker
                from horsies.core.models.broker import PostgresConfig

                temp_broker_config = PostgresConfig(
                    database_url=self.cfg.dsn,
                    session_database_url=self.cfg.session_dsn or None,
                    pgbouncer_transaction_mode=self.cfg.pgbouncer_transaction_mode,
                    pool_size=self.cfg.parent_pool_size,
                    max_overflow=self.cfg.parent_max_overflow,
                )
                temp_broker = PostgresBroker(
                    temp_broker_config,
                    assume_initialized=True,
                )
                owns_temp_broker = True
                if self._app is not None:
                    temp_broker.app = self._app

            # The gate session holds a pool connection (and the xact lock)
            # for the duration of the pass, while the pass body checks out
            # its own session from the same pool — peak two connections.
            # A single-connection coordinator pool would deadlock on the
            # second checkout until pool timeout, every interval, so the
            # gate is skipped there. Ungated concurrency is safe for
            # every step the pass runs: SKIP LOCKED covers the recovery
            # and retention statements, and the coverage/publication
            # ensure is idempotent (registration, leaf creation, and
            # republication all converge under concurrent callers) — the
            # gate only dedupes redundant work across workers.
            gate_enabled = self._reaper_gate_enabled()
            if not gate_enabled:
                logger.warning(
                    'Reaper gate disabled: coordinator pool has a single '
                    'connection (pool_size=%s, max_overflow=%s); passes run '
                    'ungated (safe via SKIP LOCKED, may be redundant across '
                    'workers)',
                    self.cfg.parent_pool_size,
                    self.cfg.parent_max_overflow,
                )

            while not self._stop.is_set():
                try:
                    # Gate: every worker runs this loop, but one executing
                    # reaper per interval is enough — SKIP LOCKED makes
                    # concurrent passes safe, just redundant. The try-lock
                    # is held by the gate session for the duration of the
                    # pass and releases automatically (xact scope) if the
                    # holder dies mid-pass.
                    if not gate_enabled:
                        await self._run_reaper_pass(
                            temp_broker, recovery_cfg, state,
                        )
                    else:
                        async with self.sf() as gate_session:
                            gate_result = await gate_session.execute(
                                REAPER_GATE_TRY_LOCK_SQL,
                                {'key': self._advisory_key_reaper()},
                            )
                            if bool(gate_result.scalar()):
                                await self._run_reaper_pass(
                                    temp_broker, recovery_cfg, state,
                                )
                            else:
                                logger.debug(
                                    'Reaper pass skipped: another worker holds the gate',
                                )
                except Exception as e:
                    logger.error(f'Reaper loop error: {e}')

                # Wait for check interval or stop signal (convert ms to seconds for asyncio)
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=check_interval_ms / 1000.0
                    )
                    break  # Stop signal received
                except asyncio.TimeoutError:
                    continue  # Continue to next iteration

        except asyncio.CancelledError:
            logger.info('Reaper loop cancelled')
            return
        finally:
            if owns_temp_broker and temp_broker is not None:
                close_result = await temp_broker.close_async()
                if is_err(close_result):
                    logger.error(
                        f'Error closing reaper broker: {close_result.err_value.message}'
                    )
