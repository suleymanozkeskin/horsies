"""Worker health: heartbeats, ping responder, state snapshots.

HealthMixin emits claimer heartbeats (with lease renewal), answers
broadcast/targeted pings, and persists periodic worker-state snapshots.
Worker-internal mixin: the ``TYPE_CHECKING`` block declares the slice of
``Worker`` it relies on; ``Worker`` is the only intended subclass.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import os
import socket
from typing import TYPE_CHECKING, Any

from psycopg.types.json import Jsonb
from pydantic import ValidationError
from sqlalchemy import text

from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.logging import get_logger
from horsies.core.models.health import (
    WorkerPingRequest,
    WorkerPongPayload,
)
from horsies.core.types.result import is_err
from horsies.core.worker.runtime import _collect_psutil_metrics
from horsies.core.worker.sql import (
    INSERT_CLAIMER_HEARTBEAT_SQL,
    INSERT_WORKER_STATE_SQL,
    RENEW_CLAIM_LEASE_SQL,
)

if TYPE_CHECKING:
    from datetime import datetime

    from horsies.core.models.retention import RetentionConfig
    from horsies.core.worker.config import WorkerConfig

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from horsies.core.worker.config import WorkerConfig

logger = get_logger('worker')


def _effective_retention(cfg: 'WorkerConfig') -> 'RetentionConfig':
    """The retention config the worker actually runs on.

    The maintenance pass binds `retention_config or RetentionConfig()`,
    so an unset section means the worker runs on DEFAULTS, not on
    nothing. The health surface reports what governs, so it must resolve
    the same way — otherwise a worker running on defaults publishes
    nulls, and the snapshot disagrees with the behaviour it describes.
    """
    from horsies.core.models.retention import RetentionConfig

    return cfg.retention_config or RetentionConfig()


class HealthMixin:
    """Claimer heartbeats, ping responder, worker-state snapshots."""

    if TYPE_CHECKING:
        # Worker state this mixin reads.
        sf: async_sessionmaker[AsyncSession]
        cfg: WorkerConfig
        worker_instance_id: str
        _stop: asyncio.Event
        _ping_queue: asyncio.Queue[Any] | None
        _started_at: datetime
        _partition_coverage_health: dict[str, Any] | None
        _partition_pruning_health: dict[str, Any] | None
        _phase2_recovery_health: dict[str, Any] | None
        _workflow_recovery_health: dict[str, Any] | None
        _orphan_task_recovery_health: dict[str, Any] | None

        # Cross-concern methods provided by sibling mixins / Worker.
        def _claim_lease_ms(self) -> int: ...
        async def _count_claimed_for_worker(
            self, session: AsyncSession
        ) -> int: ...
        async def _count_only_running_for_worker(
            self, session: AsyncSession
        ) -> int: ...

    async def _claimer_heartbeat_loop(self) -> None:
        """Emit claimer heartbeats for tasks we've claimed but not yet started."""
        # Get heartbeat interval from recovery config (milliseconds)
        claimer_heartbeat_interval_ms = 30_000  # default: 30 seconds
        if self.cfg.recovery_config:
            claimer_heartbeat_interval_ms = (
                self.cfg.recovery_config.claimer_heartbeat_interval_ms
            )

        try:
            while not self._stop.is_set():
                try:
                    async with self.sf() as s:
                        await s.execute(
                            INSERT_CLAIMER_HEARTBEAT_SQL,
                            {
                                'wid': self.worker_instance_id,
                                'host': socket.gethostname(),
                                'pid': os.getpid(),
                            },
                        )
                        # Extend claim lease for CLAIMED tasks owned by this worker,
                        # but only if claimed_at is recent enough. Tasks claimed longer
                        # ago than max_claim_renew_age_ms are left to expire, preventing
                        # indefinite renewal of orphaned claims.
                        await s.execute(
                            RENEW_CLAIM_LEASE_SQL,
                            {
                                'wid': self.worker_instance_id,
                                'claim_lease_ms': self._claim_lease_ms(),
                                'max_claim_age_ms': self.cfg.max_claim_renew_age_ms,
                            },
                        )
                        await s.commit()
                except Exception as e:
                    logger.error(f'Claimer heartbeat error: {e}')
                # Convert to seconds only for asyncio.sleep
                await asyncio.sleep(claimer_heartbeat_interval_ms / 1000.0)
        except asyncio.CancelledError:
            return

    async def _ping_responder_loop(self) -> None:
        """Serve liveness pings: reply on the request's reply channel.

        A reply proves this worker's event loop is responsive and that it can
        reach Postgres (the pong travels through ``self.sf()``).
        """
        if self._ping_queue is None:
            return
        try:
            while not self._stop.is_set():
                notify = await self._ping_queue.get()
                try:
                    await self._handle_ping(notify.payload)
                except Exception as e:
                    logger.error(f'Ping responder error: {e}')
        except asyncio.CancelledError:
            return

    async def _handle_ping(self, raw_payload: str) -> None:
        """Decode a ping and reply when broadcast or addressed to this worker."""
        parsed = loads_json(raw_payload)
        if is_err(parsed):
            logger.warning('Discarding unparseable ping payload: %s', parsed.err_value)
            return
        body = parsed.ok_value
        if not isinstance(body, dict):
            return
        try:
            request = WorkerPingRequest.model_validate(body)
        except ValidationError as e:
            logger.warning('Discarding invalid ping payload: %s', e)
            return

        if (
            request.target_worker_id is not None
            and request.target_worker_id != self.worker_instance_id
        ):
            return  # Addressed to a different worker.

        pong = WorkerPongPayload(
            correlation_id=request.correlation_id,
            worker_id=self.worker_instance_id,
            hostname=socket.gethostname(),
            pid=os.getpid(),
        )
        payload_r = dumps_json(pong.model_dump())
        if is_err(payload_r):
            logger.error('Failed to encode pong payload: %s', payload_r.err_value)
            return

        async with self.sf() as s:
            await s.execute(
                text('SELECT pg_notify(:ch, :p)'),
                {'ch': request.reply_channel, 'p': payload_r.ok_value},
            )
            await s.commit()

    async def _update_worker_state(self) -> None:
        """Update worker state snapshot in database for monitoring."""
        try:
            rss_mb, mem_pct, cpu_pct, children_rss_mb = await asyncio.to_thread(
                _collect_psutil_metrics,
            )

            # Serialize recovery config
            recovery_dict = None
            if self.cfg.recovery_config:
                recovery_dict = {
                    'auto_requeue_stale_claimed': self.cfg.recovery_config.auto_requeue_stale_claimed,
                    'claimed_stale_threshold_ms': self.cfg.recovery_config.claimed_stale_threshold_ms,
                    'auto_fail_stale_running': self.cfg.recovery_config.auto_fail_stale_running,
                    'running_stale_threshold_ms': self.cfg.recovery_config.running_stale_threshold_ms,
                    'finalizing_stale_threshold_ms': self.cfg.recovery_config.finalizing_stale_threshold_ms,
                    'crashed_worker_recovery_grace_ms': self.cfg.recovery_config.crashed_worker_recovery_grace_ms,
                    'check_interval_ms': self.cfg.recovery_config.check_interval_ms,
                    'orphan_task_audit_interval_ms': self.cfg.recovery_config.orphan_task_audit_interval_ms,
                    'runner_heartbeat_interval_ms': self.cfg.recovery_config.runner_heartbeat_interval_ms,
                    'claimer_heartbeat_interval_ms': self.cfg.recovery_config.claimer_heartbeat_interval_ms,
                    'worker_state_retention_hours': _effective_retention(self.cfg).worker_state_retention_hours,
                    'terminal_record_retention_hours': _effective_retention(self.cfg).terminal_record_retention_hours,
                }

            # One session for the counts and the snapshot INSERT — three
            # pool checkouts per snapshot per worker is needless churn.
            async with self.sf() as s:
                running = await self._count_only_running_for_worker(s)
                claimed = await self._count_claimed_for_worker(s)
                await s.execute(
                    INSERT_WORKER_STATE_SQL,
                    {
                        'wid': self.worker_instance_id,
                        'host': socket.gethostname(),
                        'pid': os.getpid(),
                        'procs': self.cfg.processes,
                        'mcb': self.cfg.max_claim_batch,
                        'mcpw': self.cfg.max_claim_per_worker
                        if self.cfg.max_claim_per_worker > 0
                        else self.cfg.processes,
                        'cwc': self.cfg.cluster_wide_cap,
                        'queues': self.cfg.queues,
                        'qp': Jsonb(self.cfg.queue_priorities)
                        if self.cfg.queue_priorities
                        else None,
                        'qmc': Jsonb(self.cfg.queue_max_concurrency)
                        if self.cfg.queue_max_concurrency
                        else None,
                        'recovery': Jsonb(
                            {
                                **(recovery_dict or {}),
                                'partition_coverage': (
                                    self._partition_coverage_health
                                ),
                                'partition_pruning': (
                                    self._partition_pruning_health
                                ),
                                'phase2_recovery': (
                                    self._phase2_recovery_health
                                ),
                                'workflow_recovery': (
                                    self._workflow_recovery_health
                                ),
                                'orphan_task_recovery': (
                                    self._orphan_task_recovery_health
                                ),
                            }
                        )
                        if (
                            recovery_dict
                            or self._partition_coverage_health
                            or self._partition_pruning_health
                            or self._phase2_recovery_health
                            or self._workflow_recovery_health
                            or self._orphan_task_recovery_health
                        )
                        else None,
                        'running': running,
                        'claimed': claimed,
                        'mem_mb': rss_mb,
                        'mem_pct': mem_pct,
                        'cpu_pct': cpu_pct,
                        'children_mem_mb': children_rss_mb,
                        'started': self._started_at,
                    },
                )
                await s.commit()
        except Exception as e:
            logger.error(f'Failed to update worker state: {e}')

    async def _worker_state_heartbeat_loop(self) -> None:
        """Periodically persist a worker-state snapshot for monitoring."""
        worker_state_interval_ms = 30_000  # default: 30 seconds
        if self.cfg.recovery_config:
            worker_state_interval_ms = (
                self.cfg.recovery_config.worker_state_snapshot_interval_ms
            )

        try:
            while not self._stop.is_set():
                try:
                    await self._update_worker_state()
                except Exception as e:
                    logger.error(f'Worker state heartbeat error: {e}')

                # Wait for interval or stop signal
                await asyncio.sleep(worker_state_interval_ms / 1000.0)
        except asyncio.CancelledError:
            return
