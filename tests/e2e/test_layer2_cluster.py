"""Layer 2 e2e tests: Multi-Worker / Cluster behaviors."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import tempfile
from pathlib import Path
from typing import Protocol

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


DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'
CUSTOM_INSTANCE = 'tests.e2e.tasks.instance_custom:app'
CLUSTER_CAP_INSTANCE = 'tests.e2e.tasks.instance_cluster_cap:app'
RECOVERY_INSTANCE = 'tests.e2e.tasks.instance_recovery:app'


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
            )
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
            )
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
            )
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
        )

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
    )

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
            )
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
            )
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

        # Wait for all to reach terminal state (3 retries × 1s + execution time)
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
            )
            for _ in range(num_tasks_per_queue)
        ]
        normal_ids = [
            custom_broker.enqueue(
                task_name='e2e_custom_slow',
                args=(task_duration_ms,),
                kwargs={},
                queue_name='normal',
            )
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
        )
        for _ in range(num_tasks_per_queue)
    ]

    # Enqueue high-priority tasks SECOND
    high_ids = [
        custom_broker.enqueue(
            task_name='e2e_custom_slow',
            args=(task_duration_ms,),
            kwargs={},
            queue_name='high',
        )
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
            )
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
            )
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
            )
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
