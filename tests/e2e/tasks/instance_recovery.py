"""Recovery-mode app instance for e2e crash/stale-task tests.

Uses aggressive thresholds so reaper detects orphaned tasks in ~2-3 seconds
instead of the default 2-5 minutes.
"""

from __future__ import annotations

import os

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode, CustomQueueConfig
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import TaskNode

DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ["DB_PASSWORD"]}@localhost:5432/horsies',
)

config = AppConfig(
    queue_mode=QueueMode.CUSTOM,
    custom_queues=[
        CustomQueueConfig(name='default', priority=1, max_concurrency=5),
        CustomQueueConfig(name='recovery', priority=1, max_concurrency=1),
    ],
    broker=PostgresConfig(
        database_url=DB_URL,
        pool_size=5,
        max_overflow=5,
    ),
    recovery=RecoveryConfig(
        runner_heartbeat_interval_ms=1_000,
        claimer_heartbeat_interval_ms=1_000,
        running_stale_threshold_ms=2_000,
        claimed_stale_threshold_ms=2_000,
        check_interval_ms=1_000,
    ),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker


@app.task(task_name='e2e_recovery_healthcheck', queue_name='default')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_recovery_slow', queue_name='default')
def slow_task(duration_ms: int) -> TaskResult[str, TaskError]:
    """Long-running task for crash-while-RUNNING tests."""
    import time

    time.sleep(duration_ms / 1000)
    return TaskResult(ok=f'slept_{duration_ms}')


@app.task(task_name='e2e_recovery_wf_step', queue_name='default')
def recovery_wf_step(step: str, delay_ms: int) -> TaskResult[str, TaskError]:
    """Workflow step with configurable delay for crash-recovery tests."""
    import time

    time.sleep(delay_ms / 1000)
    return TaskResult(ok=f'step_{step}_done')


# ---------------------------------------------------------------------------
# Workflow spec: crash-recovery DAG
#
#   A(50ms) → B(50ms) → C(60s)  ──→ E(50ms) → F(50ms)
#                      → D(60s)  ──/
#
# A, B complete fast on "default" queue.
# C, D are long-running (killed mid-flight) on "default" queue.
# E depends on C AND D with allow_failed_deps=True — runs after recovery
#   on "recovery" queue (max_concurrency=1).
# F depends on E, also on "recovery" queue — runs sequentially after E.
# ---------------------------------------------------------------------------

_node_a = TaskNode(fn=recovery_wf_step, kwargs={'step': 'A', 'delay_ms': 50})
_node_b = TaskNode(fn=recovery_wf_step, kwargs={'step': 'B', 'delay_ms': 50}, waits_for=[_node_a])
_node_c = TaskNode(fn=recovery_wf_step, kwargs={'step': 'C', 'delay_ms': 60_000}, waits_for=[_node_b])
_node_d = TaskNode(fn=recovery_wf_step, kwargs={'step': 'D', 'delay_ms': 60_000}, waits_for=[_node_b])
_node_e = TaskNode(
    fn=recovery_wf_step,
    kwargs={'step': 'E', 'delay_ms': 50},
    waits_for=[_node_c, _node_d],
    allow_failed_deps=True,
    queue='recovery',
)
_node_f = TaskNode(
    fn=recovery_wf_step,
    kwargs={'step': 'F', 'delay_ms': 50},
    waits_for=[_node_e],
    queue='recovery',
)


# Register tasks for worker subprocess discovery
app.discover_tasks(['tests.e2e.tasks.instance_recovery'])

spec_recovery_crash = app.workflow(
    name='e2e_recovery_crash',
    tasks=[_node_a, _node_b, _node_c, _node_d, _node_e, _node_f],
)
