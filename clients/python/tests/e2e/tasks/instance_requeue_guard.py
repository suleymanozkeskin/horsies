"""App instance for requeue-guard e2e test.

Short claim_lease_ms + small max_claim_renew_age_ms so an orphaned CLAIMED
task stops getting its lease renewed quickly and becomes reclaimable.

The fault of the test is on the connections of worker A, not in this module:
worker A connects through a proxy that refuses the start of a task and the
release of a claim (``test_layer2_cluster.REFUSED_FOR_WORKER_A``). Worker A
stays alive (its claimer heartbeat runs) and every task it claims stays
CLAIMED.
"""

from __future__ import annotations

import os

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result

from tests.e2e.helpers.env import e2e_database_url
from tests.e2e.tasks.profiles import E2E_WORKER_PROFILES

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

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
        requeue_stale_claimed=False,
        claimed_stale_threshold_ms=120_000,   # high, won't trigger
    ),
    worker_profiles=E2E_WORKER_PROFILES,
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app

app.discover_tasks([
    'tests.e2e.tasks.basic',
])


@app.task(task_name='e2e_requeue_guard_healthcheck')
def healthcheck() -> Result[str, TaskError]:
    return Ok('ready')


@app.task(task_name='e2e_requeue_guard_task')
def requeue_guard_task(*, token: str) -> Result[str, TaskError]:
    """Return the token. Appends a marker line so executions are countable."""
    from pathlib import Path

    log_dir = os.environ.get('E2E_REQUEUE_GUARD_LOG_DIR')
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        marker_path = Path(log_dir) / token
        with marker_path.open('a', encoding='utf-8') as f:
            f.write('executed\n')
    return Ok(f'done:{token}')
