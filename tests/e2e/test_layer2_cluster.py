"""Layer 2 e2e tests: Multi-Worker / Cluster behaviors."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import tempfile
from pathlib import Path
from typing import Protocol, Any

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.task_pg import TaskModel
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.status import TaskStatus
from sqlalchemy import text

from tests.e2e.helpers.db import (
    poll_max_during,
    wait_for_all_terminal,
    wait_for_any_status,
    wait_for_status,
)
from tests.e2e.helpers.worker import run_worker, run_workers
from tests.e2e.tasks import basic as basic_tasks
from tests.e2e.tasks import retry as retry_tasks
from tests.e2e.tasks import queues_custom
from tests.e2e.tasks import instance_cluster_cap
from tests.e2e.tasks import instance_recovery
from tests.e2e.tasks import instance_softcap


DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'
CUSTOM_INSTANCE = 'tests.e2e.tasks.instance_custom:app'
CLUSTER_CAP_INSTANCE = 'tests.e2e.tasks.instance_cluster_cap:app'
RECOVERY_INSTANCE = 'tests.e2e.tasks.instance_recovery:app'
SOFTCAP_INSTANCE = 'tests.e2e.tasks.instance_softcap:app'


class _HealthcheckTask(Protocol):
    def send(self) -> TaskHandle[str]: ...


def _make_ready_check(task_func: _HealthcheckTask):
    """Create a ready check that polls a healthcheck task."""
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = task_func.send()
        result = handle.get(timeout_ms=2000)
        return result.is_ok()

    return _check


async def _wait_for_claimed_owner(
    session_factory,
    task_id: str,
    timeout_s: float = 15.0,
    poll_interval: float = 0.1,
) -> str:
    """Wait until task is CLAIMED and return claimed_by_worker_id."""
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        async with session_factory() as session:
            result = await session.execute(
                text(
                    """
                    SELECT status, claimed_by_worker_id
                    FROM horsies_tasks
                    WHERE id = :id
                    """
                ),
                {'id': task_id},
            )
            row = result.fetchone()
            if row is not None and row[0] == 'CLAIMED' and row[1]:
                return str(row[1])
        await asyncio.sleep(poll_interval)
    raise TimeoutError(f'Task {task_id} was not CLAIMED within {timeout_s}s')


async def _prepare_execution_ledger_tables(broker: PostgresBroker) -> None:
    """Create and clear DB-ledger tables used by the double-execution race test."""
    async with broker.session_factory() as session:
        await session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS e2e_execution_attempts (
                    id BIGSERIAL PRIMARY KEY,
                    token TEXT NOT NULL,
                    worker_identity TEXT NOT NULL,
                    worker_pid INTEGER NOT NULL,
                    entered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        await session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS e2e_execution_winner (
                    token TEXT PRIMARY KEY,
                    worker_identity TEXT NOT NULL,
                    worker_pid INTEGER NOT NULL,
                    won_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        await session.execute(text("TRUNCATE e2e_execution_attempts, e2e_execution_winner"))
        await session.commit()


# =============================================================================
# L2.1 Multi-Worker Distribution
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_multi_worker_distribution(broker: PostgresBroker) -> None:
    """L2.1: Prove more than one worker claims tasks."""
    num_workers = 2
    num_tasks = 10
    task_duration_ms = 500

    with run_workers(
        DEFAULT_INSTANCE,
        count=num_workers,
        processes=1,
        ready_check=_make_ready_check(basic_tasks.healthcheck),
    ):
        # Enqueue slow tasks to force overlap and spread across workers
        task_ids = [
            broker.enqueue(
                task_name='e2e_slow',
                args=(task_duration_ms,),
                kwargs={},
                queue_name='default',
            ).unwrap()
            for _ in range(num_tasks)
        ]

        # Wait for all tasks to complete
        await wait_for_all_terminal(broker.session_factory, task_ids, timeout_s=30.0)

        # Query distinct claimed_by_worker_id values
        async with broker.session_factory() as session:
            result = await session.execute(
                text("""
                    SELECT DISTINCT claimed_by_worker_id
                    FROM horsies_tasks
                    WHERE id = ANY(:ids)
                    AND claimed_by_worker_id IS NOT NULL
                """),
                {'ids': task_ids},
            )
            worker_ids = [row[0] for row in result.fetchall()]

        # With 2 workers and 10 slow tasks, we expect at least 2 distinct worker IDs
        assert (
            len(worker_ids) >= 2
        ), f'Expected at least 2 distinct worker IDs, got {len(worker_ids)}: {worker_ids}'

        # Verify all tasks completed successfully
        async with broker.session_factory() as session:
            for task_id in task_ids:
                task = await session.get(TaskModel, task_id)
                assert task is not None
                assert task.status == TaskStatus.COMPLETED, (
                    f'Task {task_id} has status {task.status}, expected COMPLETED'
                )


# =============================================================================
# L2.2 Cluster-Wide Cap Enforcement
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_cluster_wide_cap(cluster_cap_broker: PostgresBroker) -> None:
    """L2.2: Validate cluster_wide_cap limits RUNNING tasks across all workers."""
    cluster_wide_cap = instance_cluster_cap.CLUSTER_WIDE_CAP  # 2
    num_workers = 2
    num_tasks = 6
    task_duration_ms = 500

    with run_workers(
        CLUSTER_CAP_INSTANCE,
        count=num_workers,
        processes=2,  # Each worker has 2 processes
        ready_check=_make_ready_check(instance_cluster_cap.healthcheck),
    ):
        # Enqueue slow tasks (use cluster_cap-specific task)
        task_ids = [
            cluster_cap_broker.enqueue(
                task_name='e2e_cluster_cap_slow',
                args=(task_duration_ms,),
                kwargs={},
                queue_name='default',
            ).unwrap()
            for _ in range(num_tasks)
        ]

        # Poll DB during execution to find max RUNNING count
        # Duration should cover at least a few task executions
        max_running = await poll_max_during(
            cluster_cap_broker.session_factory,
            "SELECT COUNT(*) FROM horsies_tasks WHERE status = 'RUNNING'",
            duration_s=4.0,
            poll_interval=0.05,
        )

        # Wait for all to complete
        await wait_for_all_terminal(
            cluster_cap_broker.session_factory, task_ids, timeout_s=30.0
        )

        # Guard against vacuous pass (all tasks completed before first poll sample)
        assert max_running > 0, 'Polling observed 0 RUNNING tasks — test may be vacuous'

        # Verify cap was respected
        assert (
            max_running <= cluster_wide_cap
        ), f'RUNNING count {max_running} exceeded cluster_wide_cap {cluster_wide_cap}'

        # Verify all tasks completed successfully
        async with cluster_cap_broker.session_factory() as session:
            for task_id in task_ids:
                task = await session.get(TaskModel, task_id)
                assert task is not None
                assert task.status == TaskStatus.COMPLETED


# =============================================================================
# L2.3 Per-Queue Concurrency Cap
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_per_queue_concurrency_cap(custom_broker: PostgresBroker) -> None:
    """L2.3: Validate per-queue max_concurrency in CUSTOM mode."""
    # high queue has max_concurrency=5 in instance_custom
    queue_name = 'high'
    max_concurrency = 5
    num_tasks = 10
    task_duration_ms = 400

    with run_workers(
        CUSTOM_INSTANCE,
        count=1,
        processes=8,  # More processes than max_concurrency
        ready_check=_make_ready_check(queues_custom.high_task),
    ):
        # Enqueue slow tasks to the high queue (use custom-specific task)
        task_ids = [
            custom_broker.enqueue(
                task_name='e2e_custom_slow',
                args=(task_duration_ms,),
                kwargs={},
                queue_name=queue_name,
            ).unwrap()
            for _ in range(num_tasks)
        ]

        # Poll DB during execution to find max RUNNING count for this queue
        max_running = await poll_max_during(
            custom_broker.session_factory,
            "SELECT COUNT(*) FROM horsies_tasks WHERE status = 'RUNNING' AND queue_name = :qn",
            duration_s=5.0,
            poll_interval=0.05,
            params={'qn': queue_name},
        )

        # Wait for all to complete
        await wait_for_all_terminal(
            custom_broker.session_factory, task_ids, timeout_s=30.0
        )

        # Guard against vacuous pass (all tasks completed before first poll sample)
        assert max_running > 0, 'Polling observed 0 RUNNING tasks — test may be vacuous'

        # Verify per-queue cap was respected
        assert max_running <= max_concurrency, (
            f"RUNNING count {max_running} in queue '{queue_name}' exceeded "
            f'max_concurrency {max_concurrency}'
        )

        # Verify all tasks completed successfully (prevents silent failures)
        async with custom_broker.session_factory() as session:
            for task_id in task_ids:
                task = await session.get(TaskModel, task_id)
                assert task is not None
                assert (
                    task.status == TaskStatus.COMPLETED
                ), f'Task {task_id} has status {task.status}, expected COMPLETED'


# =============================================================================
# L2.5 Double-Execution Prevention (Multi-Worker)
# =============================================================================


@pytest.mark.e2e
def test_multi_worker_no_double_execution() -> None:
    """L2.5: Prove tasks are not executed twice across multiple workers."""
    num_workers = 2
    num_tasks = 20

    with tempfile.TemporaryDirectory() as log_dir:
        os.environ['E2E_IDEMPOTENT_LOG_DIR'] = log_dir
        try:
            with run_workers(
                DEFAULT_INSTANCE,
                count=num_workers,
                processes=2,
                ready_check=_make_ready_check(basic_tasks.healthcheck),
            ):
                # Use unique tokens for each task
                tokens = [f'multiworker_task_{i}' for i in range(num_tasks)]
                handles = [basic_tasks.idempotent_task.send(token) for token in tokens]
                results = [h.get(timeout_ms=30000) for h in handles]

                # All tasks should succeed (no DOUBLE_EXECUTION errors)
                for i, r in enumerate(results):
                    assert r.is_ok(), f'Task {i} failed: {r.err}'

                # Verify each token file exists exactly once
                for token in tokens:
                    token_file = Path(log_dir) / token
                    assert token_file.exists(), f'Token file {token} not created'

                # Verify no extra files were created (catches double-execution with
                # differently-named artifacts)
                all_files = list(Path(log_dir).iterdir())
                assert len(all_files) == num_tasks, (
                    f'Expected exactly {num_tasks} token files, found {len(all_files)}: '
                    f'{[f.name for f in all_files]}'
                )
        finally:
            os.environ.pop('E2E_IDEMPOTENT_LOG_DIR', None)


# =============================================================================
# L2.16 DB Ledger Race — Exact Single Execution Proof
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_softcap_db_ledger_race_single_execution(
    softcap_broker: PostgresBroker,
) -> None:
    """L2.16: Force reclaim race and prove only one task body enters via DB ledger."""
    await _prepare_execution_ledger_tables(softcap_broker)

    blocker_duration_ms = 4_000
    token = 'softcap_db_ledger_race_token'

    with run_worker(
        SOFTCAP_INSTANCE,
        processes=1,
        timeout=20.0,
        ready_check=_make_ready_check(instance_softcap.healthcheck),
    ):
        blocker_task_id = softcap_broker.enqueue(
            task_name='e2e_softcap_blocker',
            args=(blocker_duration_ms,),
            kwargs={},
            queue_name='default',
        ).unwrap()
        await wait_for_status(
            softcap_broker.session_factory,
            blocker_task_id,
            'RUNNING',
            timeout_s=10.0,
        )

        ledger_task_id = softcap_broker.enqueue(
            task_name='e2e_softcap_db_ledger',
            args=(token,),
            kwargs={},
            queue_name='default',
        ).unwrap()

        await _wait_for_claimed_owner(
            softcap_broker.session_factory,
            ledger_task_id,
            timeout_s=10.0,
        )

        # claim_lease_ms=500 in SOFTCAP_INSTANCE, so 1.2s guarantees expiry before worker B starts
        await asyncio.sleep(1.2)

        with run_worker(
            SOFTCAP_INSTANCE,
            processes=1,
            timeout=20.0,
            ready_check=_make_ready_check(instance_softcap.healthcheck),
        ):
            await wait_for_all_terminal(
                softcap_broker.session_factory,
                [blocker_task_id, ledger_task_id],
                timeout_s=40.0,
            )

    async with softcap_broker.session_factory() as session:
        ledger_task = await session.get(TaskModel, ledger_task_id)
        assert ledger_task is not None
        assert ledger_task.status == TaskStatus.COMPLETED, (
            f'Expected COMPLETED, got {ledger_task.status}'
        )
        # Note: We intentionally do NOT assert which worker ended up running the task.
        # Worker A's claim loop can re-claim its own expired lease before Worker B polls,
        # making the owner non-deterministic. The DB ledger assertions below are the
        # definitive single-execution proof.

        result = await session.execute(
            text("SELECT COUNT(*) FROM e2e_execution_attempts WHERE token = :token"),
            {'token': token},
        )
        attempts = int(result.scalar() or 0)
        assert attempts == 1, f'Expected exactly 1 body-entry attempt, got {attempts}'

        result = await session.execute(
            text("SELECT COUNT(*) FROM e2e_execution_winner WHERE token = :token"),
            {'token': token},
        )
        winners = int(result.scalar() or 0)
        assert winners == 1, f'Expected exactly 1 winner row, got {winners}'

        result = await session.execute(
            text(
                """
                SELECT a.worker_identity, w.worker_identity
                FROM e2e_execution_attempts a
                JOIN e2e_execution_winner w ON w.token = a.token
                WHERE a.token = :token
                """
            ),
            {'token': token},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == row[1], (
            f'Attempt identity {row[0]} does not match winner identity {row[1]}'
        )


# =============================================================================
# L2.6 Stale RUNNING → FAILED on Worker Crash
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_stale_running_marked_failed_on_crash(
    recovery_broker: PostgresBroker,
) -> None:
    """L2.6: Worker crash mid-task marks RUNNING task as FAILED with WORKER_CRASHED."""
    task_duration_ms = 60_000  # 60s — long enough to still be RUNNING when we kill

    with run_worker(
        RECOVERY_INSTANCE,
        processes=1,
        ready_check=_make_ready_check(instance_recovery.healthcheck),
    ) as worker_proc:
        # Enqueue a long-running task
        task_id = recovery_broker.enqueue(
            task_name='e2e_recovery_slow',
            args=(task_duration_ms,),
            kwargs={},
            queue_name='default',
        ).unwrap()

        # Wait until task reaches RUNNING
        await wait_for_status(
            recovery_broker.session_factory,
            task_id,
            'RUNNING',
            timeout_s=15.0,
        )

        # SIGKILL the worker process group (no graceful shutdown)
        os.killpg(worker_proc.pid, signal.SIGKILL)
        worker_proc.wait(timeout=5.0)

    # Start a NEW worker whose reaper will detect the orphaned task
    with run_worker(
        RECOVERY_INSTANCE,
        processes=1,
        ready_check=_make_ready_check(instance_recovery.healthcheck),
    ):
        # Wait for task to reach terminal state (reaper marks it FAILED)
        await wait_for_all_terminal(
            recovery_broker.session_factory,
            [task_id],
            timeout_s=15.0,
        )

    # Verify status and error code
    async with recovery_broker.session_factory() as session:
        task = await session.get(TaskModel, task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED, (
            f'Expected FAILED, got {task.status}'
        )
        assert task.result is not None, 'FAILED task should have a result JSON'
        result_data = json.loads(task.result)
        error_code = result_data.get('err', {}).get('error_code', '')
        assert error_code == 'WORKER_CRASHED', (
            f'Expected error_code WORKER_CRASHED, got {error_code!r}'
        )


# =============================================================================
# L2.7 Stale CLAIMED → Requeued → COMPLETED
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_stale_claimed_requeued_and_completed(
    recovery_broker: PostgresBroker,
) -> None:
    """L2.7: Stale CLAIMED task is requeued to PENDING and completed by another worker."""
    dead_worker_id = 'dead_worker_00000000'

    # Enqueue a simple task
    task_id = recovery_broker.enqueue(
        task_name='e2e_recovery_healthcheck',
        args=(),
        kwargs={},
        queue_name='default',
    ).unwrap()

    # Manually set the row to CLAIMED with a stale timestamp, simulating a
    # worker that died before executing the task
    async with recovery_broker.session_factory() as session:
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'CLAIMED',
                    claimed = TRUE,
                    claimed_at = NOW() - INTERVAL '10 seconds',
                    claimed_by_worker_id = :dead_worker_id
                WHERE id = :task_id
            """),
            {'task_id': task_id, 'dead_worker_id': dead_worker_id},
        )
        await session.commit()

    # Start a worker whose reaper will detect the stale CLAIMED task and requeue it
    with run_worker(
        RECOVERY_INSTANCE,
        processes=1,
        ready_check=_make_ready_check(instance_recovery.healthcheck),
    ):
        await wait_for_all_terminal(
            recovery_broker.session_factory,
            [task_id],
            timeout_s=15.0,
        )

    # Verify the task completed and was re-claimed by a live worker
    async with recovery_broker.session_factory() as session:
        task = await session.get(TaskModel, task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED, (
            f'Expected COMPLETED, got {task.status}'
        )
        assert task.claimed_by_worker_id != dead_worker_id, (
            f'Task was not re-claimed: still assigned to {dead_worker_id}'
        )


# =============================================================================
# L2.8 Cluster Cap Not Leaked on Failure
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_cluster_cap_not_leaked_on_failure(
    cluster_cap_broker: PostgresBroker,
) -> None:
    """L2.8: Failed tasks free their cluster_wide_cap slots for new tasks."""
    num_workers = 2
    num_failing = 4
    num_slow = 4
    task_duration_ms = 300

    with run_workers(
        CLUSTER_CAP_INSTANCE,
        count=num_workers,
        processes=2,
        ready_check=_make_ready_check(instance_cluster_cap.healthcheck),
    ):
        # Phase 1: Enqueue always-failing tasks
        fail_ids = [
            cluster_cap_broker.enqueue(
                task_name='e2e_cluster_cap_fail',
                args=(),
                kwargs={},
                queue_name='default',
            ).unwrap()
            for _ in range(num_failing)
        ]

        # Wait for all failures
        await wait_for_all_terminal(
            cluster_cap_broker.session_factory, fail_ids, timeout_s=15.0
        )

        # Verify all failed
        async with cluster_cap_broker.session_factory() as session:
            for task_id in fail_ids:
                task = await session.get(TaskModel, task_id)
                assert task is not None
                assert task.status == TaskStatus.FAILED, (
                    f'Task {task_id} has status {task.status}, expected FAILED'
                )

        # Phase 2: Enqueue slow tasks — these must be able to run if cap was freed
        slow_ids = [
            cluster_cap_broker.enqueue(
                task_name='e2e_cluster_cap_slow',
                args=(task_duration_ms,),
                kwargs={},
                queue_name='default',
            ).unwrap()
            for _ in range(num_slow)
        ]

        # If cap leaked, these would hang forever as PENDING
        await wait_for_all_terminal(
            cluster_cap_broker.session_factory, slow_ids, timeout_s=15.0
        )

        # Verify all slow tasks completed
        async with cluster_cap_broker.session_factory() as session:
            for task_id in slow_ids:
                task = await session.get(TaskModel, task_id)
                assert task is not None
                assert task.status == TaskStatus.COMPLETED, (
                    f'Task {task_id} has status {task.status}, expected COMPLETED'
                )


# =============================================================================
# L2.9 Retry Works Across Multiple Workers
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_retry_works_across_multiple_workers(broker: PostgresBroker) -> None:
    """L2.9: Retry mechanism works correctly in a multi-worker setup."""
    num_workers = 2
    num_tasks = 10

    with run_workers(
        DEFAULT_INSTANCE,
        count=num_workers,
        processes=2,
        ready_check=_make_ready_check(basic_tasks.healthcheck),
    ):
        # Use .send() so retry policy (task_options) is persisted in DB
        handles = [retry_tasks.retry_exhausted_task.send() for _ in range(num_tasks)]
        task_ids = [h.task_id for h in handles]

        # Wait for all to reach terminal state (3 retries x 1s + execution time)
        await wait_for_all_terminal(
            broker.session_factory, task_ids, timeout_s=30.0
        )

        # Verify all tasks failed with correct retry count
        worker_ids_seen: set[str] = set()
        async with broker.session_factory() as session:
            for task_id in task_ids:
                task = await session.get(TaskModel, task_id)
                assert task is not None
                assert task.status == TaskStatus.FAILED, (
                    f'Task {task_id} has status {task.status}, expected FAILED'
                )
                assert task.retry_count == 3, (
                    f'Task {task_id} has retry_count {task.retry_count}, expected 3'
                )
                if task.claimed_by_worker_id:
                    worker_ids_seen.add(task.claimed_by_worker_id)

        # Prove multiple workers participated
        assert len(worker_ids_seen) >= 2, (
            f'Expected at least 2 distinct worker IDs, got {len(worker_ids_seen)}: '
            f'{worker_ids_seen}'
        )


# =============================================================================
# L2.10 Per-Queue Caps Independent Across Queues
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_per_queue_caps_independent_across_queues(
    custom_broker: PostgresBroker,
) -> None:
    """L2.10: Per-queue max_concurrency is enforced independently across queues."""
    high_max_concurrency = 5
    normal_max_concurrency = 10
    num_tasks_per_queue = 10
    task_duration_ms = 400

    with run_workers(
        CUSTOM_INSTANCE,
        count=1,
        processes=8,
        ready_check=_make_ready_check(queues_custom.high_task),
    ):
        # Enqueue tasks to both queues
        high_ids = [
            custom_broker.enqueue(
                task_name='e2e_custom_slow',
                args=(task_duration_ms,),
                kwargs={},
                queue_name='high',
            ).unwrap()
            for _ in range(num_tasks_per_queue)
        ]
        normal_ids = [
            custom_broker.enqueue(
                task_name='e2e_custom_slow',
                args=(task_duration_ms,),
                kwargs={},
                queue_name='normal',
            ).unwrap()
            for _ in range(num_tasks_per_queue)
        ]

        all_ids = high_ids + normal_ids

        # Poll both queues concurrently for max RUNNING
        max_high, max_normal = await asyncio.gather(
            poll_max_during(
                custom_broker.session_factory,
                "SELECT COUNT(*) FROM horsies_tasks WHERE status = 'RUNNING' AND queue_name = :qn",
                duration_s=5.0,
                poll_interval=0.05,
                params={'qn': 'high'},
            ),
            poll_max_during(
                custom_broker.session_factory,
                "SELECT COUNT(*) FROM horsies_tasks WHERE status = 'RUNNING' AND queue_name = :qn",
                duration_s=5.0,
                poll_interval=0.05,
                params={'qn': 'normal'},
            ),
        )

        await wait_for_all_terminal(
            custom_broker.session_factory, all_ids, timeout_s=30.0
        )

        # Guard against vacuous pass
        assert max_high > 0, 'Polling observed 0 RUNNING tasks in high queue'
        assert max_normal > 0, 'Polling observed 0 RUNNING tasks in normal queue'

        # Per-queue caps respected
        assert max_high <= high_max_concurrency, (
            f'RUNNING in high={max_high} exceeded max_concurrency={high_max_concurrency}'
        )
        assert max_normal <= normal_max_concurrency, (
            f'RUNNING in normal={max_normal} exceeded max_concurrency={normal_max_concurrency}'
        )

        # All tasks completed
        async with custom_broker.session_factory() as session:
            for task_id in all_ids:
                task = await session.get(TaskModel, task_id)
                assert task is not None
                assert task.status == TaskStatus.COMPLETED, (
                    f'Task {task_id} has status {task.status}, expected COMPLETED'
                )


# =============================================================================
# L2.11 Queue Priority Ordering
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_queue_priority_ordering(custom_broker: PostgresBroker) -> None:
    """L2.11: In CUSTOM mode, high-priority queues are processed before low-priority."""
    num_tasks_per_queue = 5
    task_duration_ms = 100

    # Enqueue low-priority tasks FIRST
    low_ids = [
        custom_broker.enqueue(
            task_name='e2e_custom_slow',
            args=(task_duration_ms,),
            kwargs={},
            queue_name='low',
        ).unwrap()
        for _ in range(num_tasks_per_queue)
    ]

    # Enqueue high-priority tasks SECOND
    high_ids = [
        custom_broker.enqueue(
            task_name='e2e_custom_slow',
            args=(task_duration_ms,),
            kwargs={},
            queue_name='high',
        ).unwrap()
        for _ in range(num_tasks_per_queue)
    ]

    all_ids = low_ids + high_ids

    # Start worker AFTER all tasks are enqueued — 1 process for serial execution
    with run_worker(
        CUSTOM_INSTANCE,
        processes=1,
        ready_check=_make_ready_check(queues_custom.high_task),
    ):
        await wait_for_all_terminal(
            custom_broker.session_factory, all_ids, timeout_s=30.0
        )

    # Query completion timestamps
    async with custom_broker.session_factory() as session:
        result = await session.execute(
            text("""
                SELECT id, queue_name, completed_at
                FROM horsies_tasks
                WHERE id = ANY(:ids)
                ORDER BY completed_at ASC
            """),
            {'ids': all_ids},
        )
        rows = result.fetchall()

    # All tasks should be completed
    assert len(rows) == len(all_ids), (
        f'Expected {len(all_ids)} rows, got {len(rows)}'
    )
    for row in rows:
        assert row[2] is not None, f'Task {row[0]} has no completed_at'

    # The first N completed tasks should be from the 'high' queue
    first_batch = rows[:num_tasks_per_queue]
    high_in_first_batch = sum(1 for r in first_batch if r[1] == 'high')
    assert high_in_first_batch >= num_tasks_per_queue - 1, (
        f'Expected at least {num_tasks_per_queue - 1} high-priority tasks in first '
        f'{num_tasks_per_queue} completions, got {high_in_first_batch}. '
        f'Order: {[(r[0][:8], r[1]) for r in rows]}'
    )


# =============================================================================
# L2.12 Single Worker Crash — Remaining Continue
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_single_worker_crash_remaining_continue(
    broker: PostgresBroker,
) -> None:
    """L2.12: Killing 1 of N workers doesn't stop others from completing work."""
    num_workers = 3
    num_tasks = 20
    task_duration_ms = 300

    with run_workers(
        DEFAULT_INSTANCE,
        count=num_workers,
        processes=2,
        ready_check=_make_ready_check(basic_tasks.healthcheck),
    ) as workers:
        task_ids = [
            broker.enqueue(
                task_name='e2e_slow',
                args=(task_duration_ms,),
                kwargs={},
                queue_name='default',
            ).unwrap()
            for _ in range(num_tasks)
        ]

        # Wait until at least one task is RUNNING
        await wait_for_any_status(
            broker.session_factory,
            task_ids,
            target_status='RUNNING',
            timeout_s=15.0,
        )

        # SIGKILL the first worker process (hard crash simulation)
        killed_proc = workers[0]
        killed_proc.kill()
        killed_proc.wait(timeout=5.0)

        # Give remaining workers time to process (tasks are 300ms each)
        await asyncio.sleep(8.0)

    # Query completed tasks and distinct worker IDs
    async with broker.session_factory() as session:
        result = await session.execute(
            text("""
                SELECT COUNT(*) FROM horsies_tasks
                WHERE id = ANY(:ids) AND status = 'COMPLETED'
            """),
            {'ids': task_ids},
        )
        completed_count = result.scalar() or 0

        result = await session.execute(
            text("""
                SELECT DISTINCT claimed_by_worker_id FROM horsies_tasks
                WHERE id = ANY(:ids)
                AND status = 'COMPLETED'
                AND claimed_by_worker_id IS NOT NULL
            """),
            {'ids': task_ids},
        )
        worker_ids = [row[0] for row in result.fetchall()]

    # Remaining 2 workers should complete most tasks
    assert completed_count >= 15, (
        f'Expected at least 15 completed tasks, got {completed_count}'
    )

    # At least 2 distinct workers contributed to completions
    assert len(worker_ids) >= 2, (
        f'Expected at least 2 distinct worker IDs among completed, got '
        f'{len(worker_ids)}: {worker_ids}'
    )


# =============================================================================
# L2.13 Concurrent Enqueue During Processing
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_concurrent_enqueue_during_processing(
    broker: PostgresBroker,
) -> None:
    """L2.13: Enqueueing new tasks while workers are processing doesn't cause issues."""
    num_workers = 2
    batch_size = 5
    task_duration_ms = 500

    with run_workers(
        DEFAULT_INSTANCE,
        count=num_workers,
        processes=2,
        ready_check=_make_ready_check(basic_tasks.healthcheck),
    ):
        # First batch
        batch1_ids = [
            broker.enqueue(
                task_name='e2e_slow',
                args=(task_duration_ms,),
                kwargs={},
                queue_name='default',
            ).unwrap()
            for _ in range(batch_size)
        ]

        # Wait until processing begins
        await wait_for_any_status(
            broker.session_factory,
            batch1_ids,
            target_status='RUNNING',
            timeout_s=15.0,
        )

        # Enqueue second batch mid-flight
        batch2_ids = [
            broker.enqueue(
                task_name='e2e_slow',
                args=(task_duration_ms,),
                kwargs={},
                queue_name='default',
            ).unwrap()
            for _ in range(batch_size)
        ]

        all_ids = batch1_ids + batch2_ids

        # All 10 tasks should complete
        await wait_for_all_terminal(
            broker.session_factory, all_ids, timeout_s=30.0
        )

        # Verify all completed
        async with broker.session_factory() as session:
            for task_id in all_ids:
                task = await session.get(TaskModel, task_id)
                assert task is not None
                assert task.status == TaskStatus.COMPLETED, (
                    f'Task {task_id} has status {task.status}, expected COMPLETED'
                )


# =============================================================================
# L2.14 Soft Cap Lease — No Double Execution
# =============================================================================


@pytest.mark.e2e
def test_softcap_lease_no_double_execution(
    softcap_broker: PostgresBroker,
) -> None:
    """L2.14: Under soft cap with aggressive lease expiry, no task body executes twice.

    Strategy: 2 workers x 1 process each. prefetch_buffer=2 lets each worker
    claim up to 3 tasks (1 running + 2 prefetched). claim_lease_ms=500 means
    prefetched claims expire while waiting in the process pool queue (each task
    takes 800ms). The other worker's claim loop reclaims the expired claims.
    When the first worker's pool finally picks up the expired claim,
    _confirm_ownership_and_set_running detects CLAIM_LOST and aborts — no
    double execution.
    """
    num_workers = 2
    num_tasks = 6

    with tempfile.TemporaryDirectory() as log_dir:
        os.environ['E2E_IDEMPOTENT_LOG_DIR'] = log_dir
        try:
            with run_workers(
                SOFTCAP_INSTANCE,
                count=num_workers,
                processes=1,
                timeout=20.0,
                ready_check=_make_ready_check(instance_softcap.healthcheck),
            ):
                tokens = [f'softcap_task_{i}' for i in range(num_tasks)]
                handles = [
                    instance_softcap.slow_idempotent_task.send(token, 800)
                    for token in tokens
                ]
                results = [h.get(timeout_ms=60_000) for h in handles]

                # All tasks should succeed (no DOUBLE_EXECUTION errors)
                for i, r in enumerate(results):
                    assert r.is_ok(), (
                        f'Task {i} (token=softcap_task_{i}) failed: {r.err}'
                    )

                # Verify each token file exists exactly once
                for token in tokens:
                    token_file = Path(log_dir) / token
                    assert token_file.exists(), f'Token file {token} not created'

                # Verify no extra files (catches double-execution with different artifacts)
                all_files = list(Path(log_dir).iterdir())
                assert len(all_files) == num_tasks, (
                    f'Expected exactly {num_tasks} token files, found {len(all_files)}: '
                    f'{[f.name for f in all_files]}'
                )
        finally:
            os.environ.pop('E2E_IDEMPOTENT_LOG_DIR', None)


# =============================================================================
# L2.15 Soft Cap — Expired Claim Requeued and Completed
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_softcap_expired_claim_requeued(
    softcap_broker: PostgresBroker,
) -> None:
    """L2.15: A manually expired claim is reclaimed and executed exactly once.

    Proves that CLAIM_LOST from the dead worker does not cascade into task
    failure — the reclaiming worker completes the task successfully.
    """
    dead_worker_id = 'dead_softcap_worker_00000000'

    with tempfile.TemporaryDirectory() as log_dir:
        os.environ['E2E_IDEMPOTENT_LOG_DIR'] = log_dir
        try:
            token = 'softcap_expired_claim'

            # Enqueue 1 softcap idempotent task (short duration — speed not needed here)
            task_id = softcap_broker.enqueue(
                task_name='e2e_softcap_slow_idempotent',
                args=(token, 100),
                kwargs={},
                queue_name='default',
            ).unwrap()

            # Manually set to CLAIMED with an already-expired lease and a dead worker
            async with softcap_broker.session_factory() as session:
                await session.execute(
                    text("""
                        UPDATE horsies_tasks
                        SET status = 'CLAIMED',
                            claimed = TRUE,
                            claimed_at = NOW() - INTERVAL '10 seconds',
                            claimed_by_worker_id = :dead_worker_id,
                            claim_expires_at = NOW() - INTERVAL '5 seconds'
                        WHERE id = :task_id
                    """),
                    {'task_id': task_id, 'dead_worker_id': dead_worker_id},
                )
                await session.commit()

            # Start 1 worker — its claim loop will pick up the expired claim
            with run_worker(
                SOFTCAP_INSTANCE,
                processes=1,
                timeout=20.0,
                ready_check=_make_ready_check(instance_softcap.healthcheck),
            ):
                await wait_for_all_terminal(
                    softcap_broker.session_factory,
                    [task_id],
                    timeout_s=15.0,
                )

            # Verify task completed successfully and was reclaimed
            async with softcap_broker.session_factory() as session:
                task = await session.get(TaskModel, task_id)
                assert task is not None
                assert task.status == TaskStatus.COMPLETED, (
                    f'Expected COMPLETED, got {task.status}'
                )
                assert task.claimed_by_worker_id is not None, (
                    'Reclaimed COMPLETED task must record a live owner'
                )
                assert task.claimed_by_worker_id != dead_worker_id, (
                    f'Task was not reclaimed: still assigned to {dead_worker_id}'
                )

                # Verify result has no error code (clean completion)
                assert task.result is not None, 'COMPLETED task should have a result'
                result_data: dict[str, Any] = json.loads(task.result)
                assert 'err' not in result_data or result_data['err'] is None, (
                    f'Expected clean result, got error: {result_data.get("err")}'
                )
                assert result_data.get('ok') == f'executed:{token}', (
                    f'Unexpected completion payload: {result_data}'
                )

            # Verify atomic token file exists exactly once (single execution)
            token_file = Path(log_dir) / token
            assert token_file.exists(), f'Token file {token} not created'
            all_files = list(Path(log_dir).iterdir())
            assert len(all_files) == 1, (
                f'Expected exactly 1 token file, found {len(all_files)}: '
                f'{[f.name for f in all_files]}'
            )
        finally:
            os.environ.pop('E2E_IDEMPOTENT_LOG_DIR', None)


# =============================================================================
# L2.17 Soft Cap — Owner Transition After Worker Crash
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_softcap_owner_transition_after_worker_crash(
    softcap_broker: PostgresBroker,
) -> None:
    """L2.17: A CLAIMED task transitions owner after original worker crashes."""
    blocker_duration_ms = 6_000
    token = 'softcap_owner_transition'

    with tempfile.TemporaryDirectory() as log_dir:
        os.environ['E2E_IDEMPOTENT_LOG_DIR'] = log_dir
        try:
            with run_worker(
                SOFTCAP_INSTANCE,
                processes=1,
                timeout=20.0,
                ready_check=_make_ready_check(instance_softcap.healthcheck),
            ) as worker_a_proc:
                blocker_task_id = softcap_broker.enqueue(
                    task_name='e2e_softcap_blocker',
                    args=(blocker_duration_ms,),
                    kwargs={},
                    queue_name='default',
                ).unwrap()
                await wait_for_status(
                    softcap_broker.session_factory,
                    blocker_task_id,
                    'RUNNING',
                    timeout_s=10.0,
                )

                task_id = softcap_broker.enqueue(
                    task_name='e2e_softcap_slow_idempotent',
                    args=(token, 100),
                    kwargs={},
                    queue_name='default',
                ).unwrap()

                first_owner = await _wait_for_claimed_owner(
                    softcap_broker.session_factory,
                    task_id,
                    timeout_s=10.0,
                )

                # claim_lease_ms=500 in SOFTCAP_INSTANCE; wait to guarantee expiry.
                await asyncio.sleep(1.2)

                os.killpg(worker_a_proc.pid, signal.SIGKILL)
                worker_a_proc.wait(timeout=5.0)

                with run_worker(
                    SOFTCAP_INSTANCE,
                    processes=1,
                    timeout=20.0,
                    ready_check=_make_ready_check(instance_softcap.healthcheck),
                ):
                    await wait_for_all_terminal(
                        softcap_broker.session_factory,
                        [task_id],
                        timeout_s=30.0,
                    )

            async with softcap_broker.session_factory() as session:
                task = await session.get(TaskModel, task_id)
                assert task is not None
                assert task.status == TaskStatus.COMPLETED, (
                    f'Expected COMPLETED, got {task.status}'
                )
                assert task.claimed_by_worker_id is not None, (
                    'Completed task should retain final owner id'
                )
                assert task.claimed_by_worker_id != first_owner, (
                    f'Expected owner transition after crash, still owned by {first_owner}'
                )

                assert task.result is not None, 'COMPLETED task should have a result'
                result_data: dict[str, Any] = json.loads(task.result)
                assert 'err' not in result_data or result_data['err'] is None, (
                    f'Expected clean result, got error: {result_data.get("err")}'
                )
                assert result_data.get('ok') == f'executed:{token}', (
                    f'Unexpected completion payload: {result_data}'
                )

            token_file = Path(log_dir) / token
            assert token_file.exists(), f'Token file {token} not created'
            all_files = list(Path(log_dir).iterdir())
            assert len(all_files) == 1, (
                f'Expected exactly 1 token file, found {len(all_files)}: '
                f'{[f.name for f in all_files]}'
            )
        finally:
            os.environ.pop('E2E_IDEMPOTENT_LOG_DIR', None)
