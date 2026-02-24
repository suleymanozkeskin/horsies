"""App instance for requeue-guard e2e test.

Short claim_lease_ms + small max_claim_renew_age_ms so an orphaned CLAIMED
task stops getting its lease renewed quickly and becomes reclaimable.

When the env var INJECT_REQUEUE_DB_ERROR=1 is set (worker A subprocess only),
the Worker class is monkeypatched at import time so that:
  - _dispatch_one always fails (simulates executor unavailable)
  - _requeue_claimed_task always returns DB_ERROR

This keeps worker A alive (heartbeat loop running) but orphans every claimed
task in CLAIMED state with a DB_ERROR requeue outcome.
"""

from __future__ import annotations

import os

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import TaskResult, TaskError

DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ["DB_PASSWORD"]}@localhost:5432/horsies',
)

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    prefetch_buffer=2,
    claim_lease_ms=2_000,           # 2s lease — expires fast after renewal stops
    max_claim_renew_age_ms=5_000,   # 5s — stop renewing after 5s of CLAIMED
    broker=PostgresConfig(
        database_url=DB_URL,
        pool_size=2,
        max_overflow=2,
    ),
    recovery=RecoveryConfig(
        claimer_heartbeat_interval_ms=1_000,  # 1s heartbeat (2s >= 2*1s)
        check_interval_ms=2_000,              # 2s reaper cycle
        # Disable auto-requeue so the test relies purely on lease expiry +
        # CLAIM_SQL reclaim branch, not the reaper's stale-claim logic.
        auto_requeue_stale_claimed=False,
        claimed_stale_threshold_ms=120_000,   # high, won't trigger
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker

app.discover_tasks([
    'tests.e2e.tasks.basic',
])


@app.task(task_name='e2e_requeue_guard_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_requeue_guard_task')
def requeue_guard_task(token: str) -> TaskResult[str, TaskError]:
    """Return the token. Appends a marker line so executions are countable."""
    from pathlib import Path

    log_dir = os.environ.get('E2E_REQUEUE_GUARD_LOG_DIR')
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        marker_path = Path(log_dir) / token
        with marker_path.open('a', encoding='utf-8') as f:
            f.write('executed\n')
    return TaskResult(ok=f'done:{token}')


# ---------------------------------------------------------------------------
# Fault injection: activated only in worker A's subprocess via env var.
# ---------------------------------------------------------------------------

if os.environ.get('INJECT_REQUEUE_DB_ERROR') == '1':
    from horsies.core.worker.worker import Worker, _RequeueOutcome
    from typing import Optional

    _original_dispatch = Worker._dispatch_one

    async def _failing_dispatch(
        self: Worker,
        task_id: str,
        task_name: str,
        args_json: Optional[str],
        kwargs_json: Optional[str],
    ) -> None:
        # Simulate: executor unavailable → requeue → DB_ERROR
        outcome = await self._requeue_claimed_task(task_id, 'INJECTED dispatch failure')
        if outcome is _RequeueOutcome.DB_ERROR:
            import logging
            logging.getLogger('horsies.worker').critical(
                'Requeue DB_ERROR: task %s may remain orphaned CLAIMED '
                '(worker=%s, reason=INJECTED dispatch failure)',
                task_id, self.worker_instance_id,
            )

    _original_requeue = Worker._requeue_claimed_task

    async def _failing_requeue(
        self: Worker,
        task_id: str,
        reason: str,
    ) -> _RequeueOutcome:
        import logging
        logging.getLogger('horsies.worker').error(
            'INJECTED DB_ERROR for requeue of task %s: %s',
            task_id, reason,
        )
        return _RequeueOutcome.DB_ERROR

    Worker._dispatch_one = _failing_dispatch  # type: ignore[assignment]
    Worker._requeue_claimed_task = _failing_requeue  # type: ignore[assignment]
