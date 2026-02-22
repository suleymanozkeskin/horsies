"""Layer 4 e2e tests: workflow structure (DAG patterns).

Tests verify:
- Linear chain execution (A → B → C)
- Fan-out parallelism (root → B, C, D)
- Fan-in aggregation (A, B, C → AGG)
- Diamond pattern (A → B, C → D)
- Complex multi-level DAG
- Output task handling (explicit vs default terminal results)
- Task expiry (good_until) for workflow tasks
- Single-node workflow (boundary)
- Task failure cascade (downstream SKIP)
- Fan-out/fan-in failure isolation
- Workflow cancellation
- WorkflowHandle.get() error and timeout behavior

Critical: Timestamp assertions are strict (>) to validate true dependency ordering.
Tasks have minimum 50ms execution time to ensure distinguishable timestamps.

All tests use DB polling to avoid event loop conflicts with PostgresListener.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, cast

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.workflow import TaskNode

from horsies.core.models.tasks import TaskError, TaskResult

from tests.e2e.helpers.assertions import assert_err, assert_ok, start_ok_sync
from tests.e2e.helpers.worker import run_worker
from tests.e2e.helpers.workflow import (
    get_workflow_tasks,
    poll_max_running_tasks,
    wait_for_workflow_completion,
)
from tests.e2e.tasks import workflows as wf_tasks
from tests.e2e.tasks.basic import healthcheck
from tests.e2e.tasks.instance import app


DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'


def _make_ready_check() -> Callable[[], bool]:
    """Create a ready check that sends healthcheck task."""
    from horsies.core.task_decorator import TaskHandle

    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = healthcheck.send()
        result = handle.get(timeout_ms=2000)
        return result.is_ok()

    return _check


async def _wait_until(
    predicate: Callable[[], Awaitable[bool]],
    *,
    timeout_s: float = 5.0,
    poll_interval: float = 0.05,
) -> bool:
    """Poll an async predicate until true or timeout."""
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(poll_interval)
    return await predicate()


# =============================================================================
# L4.1: Linear Chain (A → B → C)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_linear_workflow(broker: PostgresBroker) -> None:
    """Tasks execute in sequence, each waits for predecessor."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_linear)

        # Use DB polling to wait for completion
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        # Get tasks via DB query
        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 3

        # All tasks completed
        for task in db_tasks:
            assert task['status'] == 'COMPLETED'

        # Verify sequential execution via timestamps (strict ordering)
        a, b, c = db_tasks
        assert a['task_index'] == 0
        assert b['task_index'] == 1
        assert c['task_index'] == 2

        assert a['completed_at'] is not None
        assert b['started_at'] is not None
        assert b['completed_at'] is not None
        assert c['started_at'] is not None

        # B started strictly after A completed
        assert (
            b['started_at'] > a['completed_at']
        ), f"B should start after A completes: B.started_at={b['started_at']}, A.completed_at={a['completed_at']}"

        # C started strictly after B completed
        assert (
            c['started_at'] > b['completed_at']
        ), f"C should start after B completes: C.started_at={c['started_at']}, B.completed_at={b['completed_at']}"


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_linear_status_transitions(broker: PostgresBroker) -> None:
    """Verify workflow_task DB records have correct timestamps."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_linear)

        # Use DB polling to wait for completion
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        # Verify DB state directly
        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 3

        for task in db_tasks:
            assert task['status'] == 'COMPLETED'
            assert task['started_at'] is not None
            assert task['completed_at'] is not None


# =============================================================================
# L4.2: Fan-Out (root → B, C, D)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_fanout_parallel_execution(broker: PostgresBroker) -> None:
    """Parallel tasks execute concurrently after root (DB polling proof)."""
    with run_worker(DEFAULT_INSTANCE, processes=4, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_fanout)

        # Poll for concurrent RUNNING tasks while workflow executes
        # Fan-out tasks take 500ms each, so poll for ~2 seconds
        async def poll_and_wait() -> int:
            max_concurrent = await poll_max_running_tasks(
                broker.session_factory,
                handle.workflow_id,
                duration_s=2.0,
                poll_interval=0.05,
            )
            return max_concurrent

        # Run polling in background while waiting for completion
        poll_task = asyncio.create_task(poll_and_wait())

        # Wait for workflow completion via DB polling
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        max_concurrent = await poll_task

        # Must observe at least 2 concurrent RUNNING tasks to prove parallelism
        assert (
            max_concurrent >= 2
        ), f'Expected at least 2 concurrent RUNNING tasks, observed max {max_concurrent}'

        # Verify all parallel tasks started after root completed
        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        root = db_tasks[0]
        parallel_tasks = db_tasks[1:4]

        assert root['completed_at'] is not None
        for t in parallel_tasks:
            assert t['started_at'] is not None
            assert (
                t['started_at'] > root['completed_at']
            ), f'Parallel task should start after root completes'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_fanout_sequential_with_single_process(broker: PostgresBroker) -> None:
    """Fan-out tasks execute sequentially when only 1 process available."""
    with run_worker(DEFAULT_INSTANCE, processes=1, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_fanout)

        # Wait for workflow completion via DB polling
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=30.0
        )
        assert status == 'COMPLETED'

        # With 1 process, all tasks complete but sequentially
        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert all(t['status'] == 'COMPLETED' for t in db_tasks)


# =============================================================================
# L4.3: Fan-In (A, B, C → AGG)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_fanin_waits_for_all(broker: PostgresBroker) -> None:
    """Aggregator waits for all parallel tasks to complete."""
    with run_worker(DEFAULT_INSTANCE, processes=4, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_fanin)

        # Wait for workflow completion via DB polling
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 4

        # Tasks 0-2 are A, B, C (roots); task 3 is AGG
        agg_task = db_tasks[3]
        root_tasks = db_tasks[:3]

        # Get max completed_at among root tasks
        completed_times = [t['completed_at'] for t in root_tasks if t['completed_at']]
        assert len(completed_times) == 3
        latest_root_completion = max(completed_times)

        # AGG started strictly after ALL roots completed
        assert agg_task['started_at'] is not None
        assert agg_task['started_at'] > latest_root_completion, (
            f"AGG should start after all roots complete: "
            f"AGG.started_at={agg_task['started_at']}, latest_root={latest_root_completion}"
        )


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_fanin_aggregator_pending_during_execution(
    broker: PostgresBroker,
) -> None:
    """Aggregator stays PENDING while dependencies are running.

    Timing-sensitive: relies on fan-in roots having staggered delays
    (200/400/600ms) so there is a window where some roots are completed
    but not all. If delays are too short, the observation window may be missed.
    """
    with run_worker(DEFAULT_INSTANCE, processes=4, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_fanin)

        async def _agg_pending_while_roots_running() -> bool:
            db_tasks = await get_workflow_tasks(
                broker.session_factory, handle.workflow_id
            )
            if len(db_tasks) < 4:
                return False
            root_completed = sum(1 for t in db_tasks[:3] if t['status'] == 'COMPLETED')
            if 0 < root_completed < 3:
                return db_tasks[3]['status'] == 'PENDING'
            return False

        observed_pending = await _wait_until(
            _agg_pending_while_roots_running,
            timeout_s=5.0,
        )
        assert observed_pending, (
            'Did not observe AGG in PENDING while root tasks were still running'
        )

        # Wait for completion via DB polling
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'


# =============================================================================
# L4.4: Diamond (A → B, C → D)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_diamond_structure(broker: PostgresBroker) -> None:
    """Diamond pattern executes correctly with proper dependency ordering."""
    with run_worker(DEFAULT_INSTANCE, processes=4, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_diamond)

        # Wait for workflow completion via DB polling
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert all(t['status'] == 'COMPLETED' for t in db_tasks)

        a, b, c, d = db_tasks
        assert a['task_index'] == 0
        assert b['task_index'] == 1
        assert c['task_index'] == 2
        assert d['task_index'] == 3

        # B and C start after A completes
        assert a['completed_at'] is not None
        assert b['started_at'] is not None
        assert c['started_at'] is not None
        assert b['started_at'] > a['completed_at']
        assert c['started_at'] > a['completed_at']

        # D starts after both B and C complete
        assert b['completed_at'] is not None
        assert c['completed_at'] is not None
        assert d['started_at'] is not None
        assert d['started_at'] > b['completed_at']
        assert d['started_at'] > c['completed_at']


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_diamond_waits_for_slow_branch(broker: PostgresBroker) -> None:
    """D waits for slower branch (C=400ms) even though B (200ms) finishes earlier.

    Intentional overlap with test_diamond_structure: this test focuses on
    the slow-branch timing guarantee specifically.
    """
    with run_worker(DEFAULT_INSTANCE, processes=4, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_diamond)

        # Wait for workflow completion via DB polling
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        _, c, d = db_tasks[1], db_tasks[2], db_tasks[3]

        # C takes longer (400ms vs B's 200ms)
        # D should start after C completes (the slower one)
        assert c['completed_at'] is not None
        assert d['started_at'] is not None
        assert d['started_at'] > c['completed_at'], (
            f"D should wait for slower branch C: "
            f"D.started_at={d['started_at']}, C.completed_at={c['completed_at']}"
        )


# =============================================================================
# L4.5: Complex DAG
#
#     A
#    /|\
#   B C D
#   |X|
#   E F
#    \|
#     G
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_complex_dag(broker: PostgresBroker) -> None:
    """Complex DAG with multiple parallel levels executes correctly."""
    with run_worker(DEFAULT_INSTANCE, processes=4, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_complex)

        # Wait for workflow completion via DB polling
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=30.0
        )
        assert status == 'COMPLETED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 7
        assert all(t['status'] == 'COMPLETED' for t in db_tasks)

        a, b, c, d, e, f, g = db_tasks
        assert a['task_index'] == 0
        assert b['task_index'] == 1
        assert c['task_index'] == 2
        assert d['task_index'] == 3
        assert e['task_index'] == 4
        assert f['task_index'] == 5
        assert g['task_index'] == 6

        # Level 1: A completes first (root)
        assert a['completed_at'] is not None

        # Level 2: B, C, D start after A completes
        assert b['started_at'] is not None and b['started_at'] > a['completed_at']
        assert c['started_at'] is not None and c['started_at'] > a['completed_at']
        assert d['started_at'] is not None and d['started_at'] > a['completed_at']

        # Level 3: E waits for B AND C, F waits for C AND D
        assert b['completed_at'] is not None
        assert c['completed_at'] is not None
        assert d['completed_at'] is not None
        assert e['started_at'] is not None
        assert f['started_at'] is not None

        assert e['started_at'] > b['completed_at'], 'E should start after B completes'
        assert e['started_at'] > c['completed_at'], 'E should start after C completes'
        assert f['started_at'] > c['completed_at'], 'F should start after C completes'
        assert f['started_at'] > d['completed_at'], 'F should start after D completes'

        # Level 4: G waits for E AND F
        assert e['completed_at'] is not None
        assert f['completed_at'] is not None
        assert g['started_at'] is not None

        assert g['started_at'] > e['completed_at'], 'G should start after E completes'
        assert g['started_at'] > f['completed_at'], 'G should start after F completes'


# =============================================================================
# L4.6: Output Task
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_explicit_output_task(broker: PostgresBroker) -> None:
    """WorkflowHandle.get() returns explicit output task result."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_output)

        # Wait for workflow completion via DB polling
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        # Get result via sync handle.get() - should work after workflow completes
        result = handle.get(timeout_ms=1000)
        assert_ok(result)

        # Should return node_output_final's result directly (not dict of terminals)
        output = result.unwrap()
        assert output == {'final': 'result', 'count': 42}


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_default_terminal_results(broker: PostgresBroker) -> None:
    """Without explicit output, get() returns dict of terminal task results."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_fanout has no output= set, so returns dict of terminal results
        handle = start_ok_sync(wf_tasks.spec_fanout)

        # Wait for workflow completion via DB polling
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        # Get result via sync handle.get() - should work after workflow completes
        result = handle.get(timeout_ms=1000)
        assert_ok(result)

        # Returns dict keyed by node_id
        output = cast(dict[str, Any], result.unwrap())
        assert isinstance(output, dict)

        # Terminal tasks are B, C, D (indices 1, 2, 3)
        # Keys should be like "e2e_fanout:1", "e2e_fanout:2", etc.
        assert len(output) == 3, f'Expected 3 terminal results, got {len(output)}'

        # Each key should be in 'workflow_name:task_index' format
        # Each value should be a TaskResult(ok='completed_...')
        for key, value in output.items():
            assert (
                ':' in key
            ), f"Key should be in 'workflow_name:task_index' format: {key}"
            assert isinstance(value, TaskResult), f'Value for {key} should be TaskResult'
            task_result: TaskResult[Any, TaskError] = cast(TaskResult[Any, TaskError], value)
            assert task_result.is_ok(), f'Terminal task {key} should have ok result'
            unwrapped = task_result.unwrap()
            assert isinstance(unwrapped, str) and unwrapped.startswith('completed_'), (
                f'Terminal task {key} value should start with "completed_", got {unwrapped!r}'
            )


# =============================================================================
# L4.7: Task Expiry (good_until)
# =============================================================================


async def _get_expiry_task_status(
    session_factory: Any,
    workflow_id: str,
    task_index: int,
) -> tuple[str, Any]:
    """Fetch (status, good_until) for a workflow task's underlying task row."""
    from sqlalchemy import text

    async with session_factory() as session:
        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        wt_row = wt_result.fetchone()
        assert wt_row is not None, f'Workflow task {task_index} not found'
        task_id = wt_row[0]

        task_result = await session.execute(
            text('SELECT status, good_until FROM horsies_tasks WHERE id = :tid'),
            {'tid': task_id},
        )
        task_row = task_result.fetchone()
        assert task_row is not None, f'Task {task_id} not found'
        return (str(task_row[0]), task_row[1])


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_task_expires_before_claim(broker: PostgresBroker) -> None:
    """L4.7.1: Workflow task with past good_until is never claimed.

    Creates a workflow with a task that expires before worker can claim it.
    Verifies the expired task stays in PENDING status (never claimed).
    """
    # Create workflow spec with short-lived task (expires in 500ms)
    expiry = datetime.now(timezone.utc) + timedelta(milliseconds=500)

    node_expiring = TaskNode(
        fn=wf_tasks.step_task,
        kwargs={'step': 'expiring'},
        good_until=expiry,
    )

    spec_expiring = app.workflow(
        name='e2e_expiring_task',
        tasks=[node_expiring],
    )

    # Start workflow (this enqueues the root task immediately)
    handle = start_ok_sync(spec_expiring)

    # Wait until task has definitely expired before starting worker.
    await _wait_until(
        lambda: asyncio.sleep(0, result=datetime.now(timezone.utc) > expiry),
        timeout_s=2.0,
    )

    # Start worker AFTER task has expired
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # Observe for a short window and ensure worker never claims expired task.
        observe_deadline = asyncio.get_event_loop().time() + 2.0
        while asyncio.get_event_loop().time() < observe_deadline:
            status, _ = await _get_expiry_task_status(
                broker.session_factory, handle.workflow_id, 0,
            )
            assert (
                status == 'PENDING'
            ), f"Expired task should remain PENDING (never claimed), got {status}"
            await asyncio.sleep(0.1)

        # Final verification: task is still PENDING with good_until set
        status, good_until_val = await _get_expiry_task_status(
            broker.session_factory, handle.workflow_id, 0,
        )
        assert good_until_val is not None, 'good_until should be set on task'
        assert (
            status == 'PENDING'
        ), f'Expired task should remain PENDING (never claimed), got {status}'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_task_completes_before_expiry(broker: PostgresBroker) -> None:
    """L4.7.2: Workflow task with future good_until completes successfully."""
    # Create workflow spec with long-lived task (expires in 60s)
    expiry = datetime.now(timezone.utc) + timedelta(seconds=60)

    node_valid = TaskNode(
        fn=wf_tasks.step_task,
        kwargs={'step': 'valid'},
        good_until=expiry,
    )

    spec_valid = app.workflow(
        name='e2e_valid_expiry',
        tasks=[node_valid],
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec_valid)

        # Wait for workflow completion via DB polling
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        # Verify the task completed successfully
        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 1
        assert db_tasks[0]['status'] == 'COMPLETED'


# =============================================================================
# L4.8: Single-Node Workflow (boundary)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_single_node_workflow(broker: PostgresBroker) -> None:
    """Minimal workflow with exactly one task completes and returns output."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_single_node)

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 1

        task = db_tasks[0]
        assert task['status'] == 'COMPLETED'
        assert task['task_index'] == 0
        assert task['task_name'] == 'e2e_wf_step'
        assert task['started_at'] is not None
        assert task['completed_at'] is not None

        result = handle.get(timeout_ms=2000)
        assert_ok(result)
        output = result.unwrap()
        # Default terminal results: dict keyed by node_id
        assert isinstance(output, dict)
        terminal_results: list[Any] = list(output.values())
        assert len(terminal_results) == 1
        inner: Any = terminal_results[0]
        assert isinstance(inner, TaskResult)
        assert inner.is_ok()
        assert inner.unwrap() == 'completed_ONLY'


# =============================================================================
# L4.9: Task Failure Cascade (A → B(fail) → C(skipped))
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_linear_failure_cascades_to_skip(broker: PostgresBroker) -> None:
    """When mid-chain task fails, downstream tasks are SKIPPED."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_linear_fail_mid)

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'FAILED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 3

        a, b, c = db_tasks
        assert a['task_index'] == 0
        assert b['task_index'] == 1
        assert c['task_index'] == 2

        assert a['status'] == 'COMPLETED'
        assert b['status'] == 'FAILED'
        assert c['status'] == 'SKIPPED'

        # Skipped task was never executed
        assert c['started_at'] is None
        assert c['completed_at'] is None


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_handle_get_returns_error_on_failed_workflow(
    broker: PostgresBroker,
) -> None:
    """handle.get() returns err with first failed task's error_code."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_linear_fail_mid)

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'FAILED'

        result = handle.get(timeout_ms=2000)
        assert_err(result, 'MID_FAIL')


# =============================================================================
# L4.10: Fan-Out with One Failing Branch
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_fanout_one_branch_fails(broker: PostgresBroker) -> None:
    """Independent branches are not affected by sibling failure."""
    with run_worker(DEFAULT_INSTANCE, processes=4, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_fanout_one_fail)

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'FAILED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 4

        root, b, c, d = db_tasks
        assert root['task_index'] == 0
        assert b['task_index'] == 1
        assert c['task_index'] == 2
        assert d['task_index'] == 3

        assert root['status'] == 'COMPLETED'
        assert b['status'] == 'FAILED'
        # Independent branches C and D complete despite B failing
        assert c['status'] == 'COMPLETED'
        assert d['status'] == 'COMPLETED'


# =============================================================================
# L4.11: Fan-In with Failing Dep
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_fanin_one_dep_fails_skips_aggregator(broker: PostgresBroker) -> None:
    """Aggregator is SKIPPED when any dependency fails (default join=all)."""
    with run_worker(DEFAULT_INSTANCE, processes=4, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_fanin_one_fail)

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'FAILED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 4

        a, b, c, agg = db_tasks
        assert a['task_index'] == 0
        assert b['task_index'] == 1
        assert c['task_index'] == 2
        assert agg['task_index'] == 3

        assert a['status'] == 'FAILED'
        assert b['status'] == 'COMPLETED'
        assert c['status'] == 'COMPLETED'
        assert agg['status'] == 'SKIPPED'

        # Aggregator was never executed
        assert agg['started_at'] is None
        assert agg['completed_at'] is None


# =============================================================================
# L4.12: Workflow Cancellation
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_cancellation(broker: PostgresBroker) -> None:
    """Cancelling a running workflow SKIPs remaining tasks."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_slow_linear)

        # Poll until first task (A, 200ms) completes
        async def _a_completed() -> bool:
            db_tasks = await get_workflow_tasks(
                broker.session_factory, handle.workflow_id,
            )
            if len(db_tasks) < 1:
                return False
            return db_tasks[0]['status'] == 'COMPLETED'

        a_done = await _wait_until(_a_completed, timeout_s=10.0)
        assert a_done, 'Task A did not complete before cancel'

        # Cancel the workflow while B (1000ms) is running or pending
        handle.cancel()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'CANCELLED'

        # Remaining PENDING/READY tasks should be SKIPPED
        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        for task in db_tasks[1:]:
            assert task['status'] in (
                'SKIPPED', 'COMPLETED', 'RUNNING',
            ), f"Task {task['task_index']} has unexpected status: {task['status']}"

        result = handle.get(timeout_ms=2000)
        assert_err(result, 'WORKFLOW_CANCELLED')


# =============================================================================
# L4.13: Handle Get Timeout
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_handle_get_timeout(broker: PostgresBroker) -> None:
    """handle.get() returns WAIT_TIMEOUT when workflow does not complete."""
    _ = broker  # Fixture ensures DB schema is initialized
    # Start workflow WITHOUT a worker — tasks stay non-terminal
    handle = start_ok_sync(wf_tasks.spec_slow_linear)

    result = handle.get(timeout_ms=200)
    assert_err(result, 'WAIT_TIMEOUT')
