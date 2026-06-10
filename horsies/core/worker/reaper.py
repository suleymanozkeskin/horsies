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
from typing import TYPE_CHECKING

from horsies.core.logging import get_logger
from horsies.core.types.result import Err, Ok, is_err
from horsies.core.worker.runtime import (
    _REAPER_MAX_PERMANENT_FAILURES,
    _ReaperPassState,
)
from horsies.core.worker.sql import (
    DELETE_EXPIRED_HEARTBEATS_SQL,
    DELETE_EXPIRED_TASKS_SQL,
    DELETE_EXPIRED_WORKER_STATES_SQL,
    DELETE_EXPIRED_WORKFLOWS_SQL,
    DELETE_EXPIRED_WORKFLOW_TASKS_SQL,
    REAPER_GATE_TRY_LOCK_SQL,
    TASK_TERMINAL_VALUES,
    WORKFLOW_TERMINAL_VALUES,
    _RETENTION_CLEANUP_INTERVAL_S,
)

if TYPE_CHECKING:
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
                recovered = await recover_stuck_workflows(s, temp_broker)
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
                deleted_heartbeats = 0
                deleted_worker_states = 0
                deleted_workflow_tasks = 0
                deleted_workflows = 0
                deleted_tasks = 0

                async with temp_broker.session_factory() as s:
                    if recovery_cfg.heartbeat_retention_hours is not None:
                        hb_result = await s.execute(
                            DELETE_EXPIRED_HEARTBEATS_SQL,
                            {
                                'retention_hours': recovery_cfg.heartbeat_retention_hours,
                            },
                        )
                        deleted_heartbeats = int(
                            getattr(hb_result, 'rowcount', 0) or 0
                        )

                    if (
                        recovery_cfg.worker_state_retention_hours
                        is not None
                    ):
                        ws_result = await s.execute(
                            DELETE_EXPIRED_WORKER_STATES_SQL,
                            {
                                'retention_hours': recovery_cfg.worker_state_retention_hours,
                            },
                        )
                        deleted_worker_states = int(
                            getattr(ws_result, 'rowcount', 0) or 0
                        )

                    if (
                        recovery_cfg.terminal_record_retention_hours
                        is not None
                    ):
                        wf_params = {
                            'retention_hours': recovery_cfg.terminal_record_retention_hours,
                            'wf_terminal_states': WORKFLOW_TERMINAL_VALUES,
                        }
                        task_params = {
                            'retention_hours': recovery_cfg.terminal_record_retention_hours,
                            'wf_terminal_states': WORKFLOW_TERMINAL_VALUES,
                            'task_terminal_states': TASK_TERMINAL_VALUES,
                        }
                        wt_result = await s.execute(
                            DELETE_EXPIRED_WORKFLOW_TASKS_SQL,
                            wf_params,
                        )
                        wf_result = await s.execute(
                            DELETE_EXPIRED_WORKFLOWS_SQL,
                            wf_params,
                        )
                        task_result = await s.execute(
                            DELETE_EXPIRED_TASKS_SQL,
                            task_params,
                        )
                        deleted_workflow_tasks = int(
                            getattr(wt_result, 'rowcount', 0) or 0
                        )
                        deleted_workflows = int(
                            getattr(wf_result, 'rowcount', 0) or 0
                        )
                        deleted_tasks = int(
                            getattr(task_result, 'rowcount', 0) or 0
                        )

                    await s.commit()

                if (
                    deleted_heartbeats > 0
                    or deleted_worker_states > 0
                    or deleted_workflow_tasks > 0
                    or deleted_workflows > 0
                    or deleted_tasks > 0
                ):
                    logger.info(
                        'Reaper retention cleanup: heartbeats=%s worker_states=%s workflow_tasks=%s workflows=%s tasks=%s',
                        deleted_heartbeats,
                        deleted_worker_states,
                        deleted_workflow_tasks,
                        deleted_workflows,
                        deleted_tasks,
                    )
            except Exception as cleanup_err:
                logger.error(f'Retention cleanup error: {cleanup_err}')
            finally:
                state.next_retention_cleanup_at = (
                    now_monotonic + _RETENTION_CLEANUP_INTERVAL_S
                )

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
            # gate is skipped there: SKIP LOCKED keeps concurrent passes
            # safe — the gate only dedupes redundant work across workers.
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
