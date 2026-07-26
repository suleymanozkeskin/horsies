# pyright: reportUnusedFunction=false
# Route handlers are registered by their decorator, not called by name.
"""Worker, liveness and schedule endpoints.

Worker reads go through the app's existing monitoring methods rather than the
query package: those methods already return typed snapshots, and this layer
only derives the presentation fields (age, staleness, uptime) on top.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import is_err
from horsies.monitoring import (
    LivenessReport,
    ScheduleStateInfo,
    WorkerHistoryPoint,
    WorkerPingInfo,
    WorkerStateInfo,
    elapsed_s,
    list_schedules,
)
from horsies.web.routes._common import query_failed

# A worker writes a snapshot periodically; treat its latest one as stale once
# it is older than this, which signals a worker that stopped reporting. Idle
# workers snapshot less often, so the threshold is deliberately loose.
# Liveness right now is proven separately by the active ping.
STALE_SNAPSHOT_THRESHOLD_S = 120


def build_router(app: Horsies, broker: PostgresBroker) -> APIRouter:
    """Build the ``/workers`` router bound to one app and broker."""
    router = APIRouter(prefix='/workers', tags=['workers'])

    @router.get('/ping')
    async def read_liveness(
        timeout_seconds: float = Query(default=2.0, ge=0.1, le=10.0),
    ) -> LivenessReport:
        """Active liveness: a database round-trip plus every worker that replies.

        An unreachable database is reported as data, not as a failure: the
        point of this endpoint is to say what is reachable.
        """
        database = await app.ping_database_async()
        reachable = not is_err(database)
        latency_ms = database.ok_value.latency_ms if not is_err(database) else None

        pings = await app.ping_workers_async(timeout_seconds=timeout_seconds)
        if is_err(pings):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f'Worker ping failed: {pings.err_value.message}',
            )

        return LivenessReport(
            db_latency_ms=latency_ms,
            db_reachable=reachable,
            workers=[
                WorkerPingInfo(
                    worker_id=pong.worker_id,
                    hostname=pong.hostname,
                    pid=pong.pid,
                    round_trip_ms=pong.round_trip_ms,
                )
                for pong in pings.ok_value
            ],
        )

    @router.get('/schedules')
    async def read_schedules() -> list[ScheduleStateInfo]:
        """Recurring schedule states, soonest next-run first."""
        result = await list_schedules(broker)
        if is_err(result):
            raise query_failed('Schedule state', result.err_value)
        return result.ok_value

    @router.get('')
    async def read_workers() -> list[WorkerStateInfo]:
        """Latest state snapshot per worker, including idle workers."""
        result = await app.list_worker_states_async()
        if is_err(result):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f'Worker state query failed: {result.err_value.message}',
            )

        workers: list[WorkerStateInfo] = []
        for snapshot in result.ok_value:
            age_s = elapsed_s(snapshot.snapshot_at, None)
            workers.append(
                WorkerStateInfo(
                    worker_id=snapshot.worker_id,
                    hostname=snapshot.hostname,
                    pid=snapshot.pid,
                    snapshot_at=snapshot.snapshot_at,
                    snapshot_age_s=age_s,
                    stale=(age_s or 0) > STALE_SNAPSHOT_THRESHOLD_S,
                    worker_started_at=snapshot.worker_started_at,
                    uptime_s=elapsed_s(snapshot.worker_started_at, None),
                    processes=snapshot.processes,
                    queues=snapshot.queues,
                    queue_max_concurrency=snapshot.queue_max_concurrency,
                    tasks_running=snapshot.tasks_running,
                    tasks_claimed=snapshot.tasks_claimed,
                    cluster_wide_cap=snapshot.cluster_wide_cap,
                    memory_usage_mb=snapshot.memory_usage_mb,
                    memory_percent=snapshot.memory_percent,
                    cpu_percent=snapshot.cpu_percent,
                )
            )
        return workers

    @router.get('/{worker_id}/history')
    async def read_history(
        worker_id: str,
        limit: int = Query(default=120, ge=1, le=1000),
    ) -> list[WorkerHistoryPoint]:
        """Timeseries snapshots for one worker, newest first.

        An unknown worker id yields an empty series rather than a 404: a
        worker that has not reported yet is not a missing resource.
        """
        result = await app.get_worker_state_history_async(worker_id, limit=limit)
        if is_err(result):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f'Worker history query failed: {result.err_value.message}',
            )
        return [
            WorkerHistoryPoint(
                snapshot_at=snapshot.snapshot_at,
                tasks_running=snapshot.tasks_running,
                tasks_claimed=snapshot.tasks_claimed,
                cpu_percent=snapshot.cpu_percent,
                memory_usage_mb=snapshot.memory_usage_mb,
                memory_percent=snapshot.memory_percent,
            )
            for snapshot in result.ok_value
        ]

    return router
