"""Layer 6 e2e tests: workflow advanced semantics.

Tests verify:
- Join=quorum with ctx gating
- run_when/skip_when conditional execution
- Pause/resume workflow semantics
- Success policy evaluation
- Recovery preserves results
- Workflow task retries

All tests use DB polling to avoid event loop conflicts with PostgresListener.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Awaitable, Callable, cast

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskResult

from tests.e2e.helpers.assertions import assert_ok, assert_err
from tests.e2e.helpers.db import wait_for_status
from tests.e2e.helpers.worker import run_worker
from tests.e2e.helpers.workflow import (
    get_workflow_tasks,
    wait_for_workflow_completion,
    get_workflow_status,
)
from tests.e2e.tasks import workflows as wf_tasks
from tests.e2e.tasks.basic import healthcheck


DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'


def _unwrap_task_value(value: Any) -> Any:
    """Unwrap TaskResult value from workflow output."""
    if isinstance(value, TaskResult):
        task_result = cast(TaskResult[Any, Any], value)
        if task_result.is_ok():
            return task_result.unwrap()
        return task_result
    return value


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
# T6.2: Join=quorum with ctx gating
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_quorum_ctx_gating(broker: PostgresBroker) -> None:
    """T6.2: D waits for ctx dep (C) even when quorum (2) is satisfied by A,B."""
    with run_worker(DEFAULT_INSTANCE, processes=3, ready_check=_make_ready_check()):
        # spec_quorum_ctx_gating: A(100ms), B(150ms), C(300ms) -> D
        # D has join=quorum, min_success=2, workflow_ctx_from=[C]
        # A and B complete first (satisfying quorum), but D must wait for C
        handle = wf_tasks.spec_quorum_ctx_gating.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 4

        # All tasks should be COMPLETED
        for task in db_tasks:
            assert (
                task['status'] == 'COMPLETED'
            ), f"Task {task['task_name']} should be COMPLETED, got {task['status']}"

        # Verify D ran after C (checking completed_at timestamps)
        c_completed = db_tasks[2]['completed_at']
        d_completed = db_tasks[3]['completed_at']
        assert c_completed is not None
        assert d_completed is not None
        assert d_completed >= c_completed, 'D should complete after or when C completes'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_quorum_impossible_skips(broker: PostgresBroker) -> None:
    """T6.2b: D is SKIPPED when quorum min_success=3 is unreachable (2 of 3 deps fail)."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_quorum_impossible.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'FAILED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 4

        assert db_tasks[0]['status'] == 'FAILED', f"A should be FAILED, got {db_tasks[0]['status']}"
        assert db_tasks[1]['status'] == 'FAILED', f"B should be FAILED, got {db_tasks[1]['status']}"
        assert db_tasks[2]['status'] == 'COMPLETED', f"C should be COMPLETED, got {db_tasks[2]['status']}"
        assert db_tasks[3]['status'] == 'SKIPPED', f"D should be SKIPPED (quorum impossible), got {db_tasks[3]['status']}"


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_join_any_all_deps_fail_skips(broker: PostgresBroker) -> None:
    """T6.2c: C is SKIPPED when join=any but all deps fail."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_join_any_all_fail.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'FAILED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 3

        assert db_tasks[0]['status'] == 'FAILED', f"A should be FAILED, got {db_tasks[0]['status']}"
        assert db_tasks[1]['status'] == 'FAILED', f"B should be FAILED, got {db_tasks[1]['status']}"
        assert db_tasks[2]['status'] == 'SKIPPED', f"C should be SKIPPED (no dep succeeded), got {db_tasks[2]['status']}"


# =============================================================================
# T6.3: run_when/skip_when precedence
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_skip_when_takes_priority(broker: PostgresBroker) -> None:
    """T6.3a: skip_when=True takes priority over run_when=True, task is SKIPPED."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_skip_when_priority: A produces value, B has skip_when=True, run_when=True
        # B should be SKIPPED, C should receive UPSTREAM_SKIPPED
        handle = wf_tasks.spec_skip_when_priority.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 3

        # A should be COMPLETED
        assert (
            db_tasks[0]['status'] == 'COMPLETED'
        ), f"A should be COMPLETED, got {db_tasks[0]['status']}"

        # B should be SKIPPED (skip_when takes priority)
        assert (
            db_tasks[1]['status'] == 'SKIPPED'
        ), f"B should be SKIPPED due to skip_when=True, got {db_tasks[1]['status']}"

        # C should be COMPLETED
        assert (
            db_tasks[2]['status'] == 'COMPLETED'
        ), f"C should be COMPLETED, got {db_tasks[2]['status']}"

        # Verify C received UPSTREAM_SKIPPED
        result = handle.get(timeout_ms=1000)
        assert_ok(result)
        output = cast(dict[str, Any], result.unwrap())
        for key, value in output.items():
            if 'e2e_wf_check_upstream_skipped' in key:
                unwrapped = _unwrap_task_value(value)
                assert (
                    unwrapped == 'received_upstream_skipped'
                ), f'C should receive UPSTREAM_SKIPPED, got {unwrapped}'
                break
        else:
            pytest.fail('Could not find check_upstream_skipped task result')


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_run_when_false_skips(broker: PostgresBroker) -> None:
    """T6.3b: run_when=False causes task to be SKIPPED."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_run_when_false: A produces value, B has run_when=False
        # B should be SKIPPED, C should receive UPSTREAM_SKIPPED
        handle = wf_tasks.spec_run_when_false.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 3

        # A should be COMPLETED
        assert db_tasks[0]['status'] == 'COMPLETED'

        # B should be SKIPPED (run_when=False)
        assert (
            db_tasks[1]['status'] == 'SKIPPED'
        ), f"B should be SKIPPED due to run_when=False, got {db_tasks[1]['status']}"

        # C should be COMPLETED
        assert db_tasks[2]['status'] == 'COMPLETED'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_condition_exception_fails_task(broker: PostgresBroker) -> None:
    """T6.3c: Exception in run_when lambda causes task failure."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_condition_exception.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'FAILED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 2

        assert db_tasks[0]['status'] == 'COMPLETED', f"A should be COMPLETED, got {db_tasks[0]['status']}"
        assert db_tasks[1]['status'] == 'FAILED', f"B should be FAILED (condition exception), got {db_tasks[1]['status']}"


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_run_when_true_executes(broker: PostgresBroker) -> None:
    """T6.3d: run_when=True allows task to execute normally."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_run_when_true.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 2

        assert db_tasks[0]['status'] == 'COMPLETED', f"A should be COMPLETED, got {db_tasks[0]['status']}"
        assert db_tasks[1]['status'] == 'COMPLETED', f"B should be COMPLETED (run_when=True), got {db_tasks[1]['status']}"


# =============================================================================
# T6.4-T6.5: Pause/Resume semantics
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_pause_blocks_ready_transitions(broker: PostgresBroker) -> None:
    """T6.4: Pausing workflow prevents READY tasks from being enqueued."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_pausable: A(100ms) -> B(200ms) -> C(200ms) -> D(100ms)
        handle = wf_tasks.spec_pausable.start()

        async def _ready_to_pause() -> bool:
            db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
            if len(db_tasks) < 4:
                return False
            # Pause after A completes while workflow is still in progress.
            return db_tasks[0]['status'] == 'COMPLETED' and db_tasks[3]['status'] == 'PENDING'

        ready_to_pause = await _wait_until(_ready_to_pause, timeout_s=5.0)
        assert ready_to_pause, 'Workflow did not reach expected pause point in time'

        # Pause the workflow
        paused = handle.pause()
        assert paused is True, 'Workflow should be pausable'

        # Verify workflow is PAUSED
        wf_status = await get_workflow_status(
            broker.session_factory, handle.workflow_id
        )
        assert wf_status == 'PAUSED', f'Workflow should be PAUSED, got {wf_status}'

        # Ensure workflow remains PAUSED for an observation window.
        observe_deadline = asyncio.get_event_loop().time() + 1.0
        while asyncio.get_event_loop().time() < observe_deadline:
            wf_status = await get_workflow_status(
                broker.session_factory, handle.workflow_id
            )
            assert wf_status == 'PAUSED', 'Workflow should remain PAUSED'
            await asyncio.sleep(0.1)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_resume_continues_workflow(broker: PostgresBroker) -> None:
    """T6.5: Resuming paused workflow allows it to complete."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_pausable: A -> B -> C -> D
        handle = wf_tasks.spec_pausable.start()

        async def _ready_to_pause() -> bool:
            db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
            if len(db_tasks) < 4:
                return False
            return db_tasks[0]['status'] == 'COMPLETED' and db_tasks[3]['status'] == 'PENDING'

        ready_to_pause = await _wait_until(_ready_to_pause, timeout_s=5.0)
        assert ready_to_pause, 'Workflow did not reach expected pause point in time'

        paused = handle.pause()
        assert paused is True

        # Verify PAUSED
        wf_status = await get_workflow_status(
            broker.session_factory, handle.workflow_id
        )
        assert wf_status == 'PAUSED'

        # Resume the workflow
        resumed = handle.resume()
        assert resumed is True, 'Workflow should be resumable'

        # Verify workflow leaves PAUSED after resume.
        # Fast workers may complete the remaining DAG before this read, so COMPLETED
        # is also valid here.
        wf_status = await get_workflow_status(
            broker.session_factory, handle.workflow_id
        )
        assert (
            wf_status in ('RUNNING', 'COMPLETED')
        ), f'Workflow should leave PAUSED after resume, got {wf_status}'

        # Wait for completion
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert (
            status == 'COMPLETED'
        ), f'Workflow should complete after resume, got {status}'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_pause_then_cancel(broker: PostgresBroker) -> None:
    """T6.4b: Paused workflow can be cancelled; remaining tasks become SKIPPED."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_pausable.start()

        async def _ready_to_pause() -> bool:
            db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
            if len(db_tasks) < 4:
                return False
            return db_tasks[0]['status'] == 'COMPLETED' and db_tasks[3]['status'] == 'PENDING'

        ready_to_pause = await _wait_until(_ready_to_pause, timeout_s=5.0)
        assert ready_to_pause, 'Workflow did not reach expected pause point in time'

        paused = handle.pause()
        assert paused is True

        wf_status = await get_workflow_status(broker.session_factory, handle.workflow_id)
        assert wf_status == 'PAUSED'

        handle.cancel()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'CANCELLED'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        # At least D should be SKIPPED (it was PENDING at pause time)
        d_status = db_tasks[3]['status']
        assert d_status == 'SKIPPED', f"D should be SKIPPED after cancel, got {d_status}"


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_pause_idempotent(broker: PostgresBroker) -> None:
    """T6.4c: Second pause() on already-paused workflow returns False."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_pausable.start()

        async def _ready_to_pause() -> bool:
            db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
            if len(db_tasks) < 4:
                return False
            return db_tasks[0]['status'] == 'COMPLETED' and db_tasks[3]['status'] == 'PENDING'

        ready_to_pause = await _wait_until(_ready_to_pause, timeout_s=5.0)
        assert ready_to_pause, 'Workflow did not reach expected pause point in time'

        first_pause = handle.pause()
        assert first_pause is True

        second_pause = handle.pause()
        assert second_pause is False, 'Second pause() should return False (already paused)'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_resume_on_running_noop(broker: PostgresBroker) -> None:
    """T6.5b: resume() on a RUNNING workflow returns False (no-op)."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_pausable.start()

        # Wait briefly to ensure workflow is RUNNING
        async def _is_running() -> bool:
            wf_status = await get_workflow_status(
                broker.session_factory, handle.workflow_id
            )
            return wf_status == 'RUNNING'

        is_running = await _wait_until(_is_running, timeout_s=5.0)
        assert is_running, 'Workflow should be RUNNING'

        resumed = handle.resume()
        assert resumed is False, 'resume() on RUNNING workflow should return False'


# =============================================================================
# T6.6: Success policy satisfied
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_success_policy_satisfied(broker: PostgresBroker) -> None:
    """T6.6: Workflow COMPLETED when success policy case is satisfied."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_success_policy_satisfied: A required, B optional (B fails)
        # Workflow should COMPLETE because A succeeds
        handle = wf_tasks.spec_success_policy_satisfied.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert (
            status == 'COMPLETED'
        ), f'Workflow should COMPLETE with satisfied success policy, got {status}'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 2

        # A should be COMPLETED
        assert db_tasks[0]['status'] == 'COMPLETED'

        # B should be FAILED (but workflow still succeeds because B is optional)
        assert db_tasks[1]['status'] == 'FAILED'


# =============================================================================
# T6.7: Success policy not met
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_success_policy_not_met(broker: PostgresBroker) -> None:
    """T6.7: Workflow FAILED when no success policy case is satisfied."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_success_policy_not_met: A and B required, B fails
        # Workflow should FAIL because B is required and fails
        handle = wf_tasks.spec_success_policy_not_met.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert (
            status == 'FAILED'
        ), f'Workflow should FAIL when success policy not met, got {status}'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 2

        # A should be COMPLETED
        assert db_tasks[0]['status'] == 'COMPLETED'

        # B should be FAILED
        assert db_tasks[1]['status'] == 'FAILED'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_success_policy_not_met_error_content(broker: PostgresBroker) -> None:
    """T6.7b: handle.get() returns error with failed task's error code when policy not met."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_success_policy_not_met.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'FAILED'

        result = handle.get(timeout_ms=1000)
        assert_err(result, expected_code='REQUIRED_FAIL')


# =============================================================================
# T6.8: Multiple success cases
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_success_policy_multiple_cases(broker: PostgresBroker) -> None:
    """T6.8: Workflow COMPLETED when any success case is satisfied."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_success_policy_multi: case1 requires A (fails), case2 requires B (succeeds)
        # Workflow should COMPLETE because case2 is satisfied
        handle = wf_tasks.spec_success_policy_multi.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert (
            status == 'COMPLETED'
        ), f'Workflow should COMPLETE when any success case passes, got {status}'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 2

        # A should be FAILED (case1 fails)
        assert db_tasks[0]['status'] == 'FAILED'

        # B should be COMPLETED (case2 satisfied)
        assert db_tasks[1]['status'] == 'COMPLETED'


# =============================================================================
# T6.9: Recovery preserves results
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_recovery_preserves_results(broker: PostgresBroker) -> None:
    """T6.9: Recovery finalizes stuck workflows with correct results/errors."""
    from horsies.core.workflows.recovery import recover_stuck_workflows
    from sqlalchemy import text

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # Start a workflow and let it complete normally
        handle = wf_tasks.spec_linear.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        # Get the original result to ensure it completed properly
        result = handle.get(timeout_ms=1000)
        assert_ok(result)
        _ = result.unwrap()  # Verify result is accessible

        # Now simulate a "stuck" workflow by artificially setting status back to RUNNING
        # but keeping all tasks terminal
        async with broker.session_factory() as session:
            await session.execute(
                text("""
                    UPDATE horsies_workflows
                    SET status = 'RUNNING', completed_at = NULL
                    WHERE id = :wf_id
                """),
                {'wf_id': handle.workflow_id},
            )
            await session.commit()

        # Run recovery
        async with broker.session_factory() as session:
            recovered = await recover_stuck_workflows(session)
            assert recovered >= 1, 'Should recover at least one workflow'
            await session.commit()

        # Verify workflow is back to COMPLETED
        wf_status = await get_workflow_status(
            broker.session_factory, handle.workflow_id
        )
        assert (
            wf_status == 'COMPLETED'
        ), f'Recovered workflow should be COMPLETED, got {wf_status}'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_recovery_preserves_failed_state(broker: PostgresBroker) -> None:
    """T6.9b: Recovery re-finalizes a failed workflow stuck in RUNNING."""
    from horsies.core.workflows.recovery import recover_stuck_workflows
    from sqlalchemy import text

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # Start a workflow that fails (A ok → B fail → C skipped)
        handle = wf_tasks.spec_linear_fail_mid.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'FAILED'

        # Simulate stuck state: set workflow back to RUNNING
        async with broker.session_factory() as session:
            await session.execute(
                text("""
                    UPDATE horsies_workflows
                    SET status = 'RUNNING', completed_at = NULL
                    WHERE id = :wf_id
                """),
                {'wf_id': handle.workflow_id},
            )
            await session.commit()

        # Verify it's RUNNING now
        wf_status = await get_workflow_status(
            broker.session_factory, handle.workflow_id
        )
        assert wf_status == 'RUNNING'

        # Run recovery
        async with broker.session_factory() as session:
            recovered = await recover_stuck_workflows(session)
            assert recovered >= 1, 'Should recover at least one workflow'
            await session.commit()

        # Verify workflow is back to FAILED (not COMPLETED)
        wf_status = await get_workflow_status(
            broker.session_factory, handle.workflow_id
        )
        assert wf_status == 'FAILED', f'Recovered workflow should be FAILED, got {wf_status}'


# =============================================================================
# T6.10: Workflow task retries
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_task_retries(broker: PostgresBroker, tmp_path: Path) -> None:
    """T6.10: Workflow task respects retry policy and eventually succeeds."""
    from horsies.core.models.workflow import TaskNode
    from tests.e2e.tasks.workflows import retry_then_ok_task

    # Create a counter file for tracking retry attempts
    counter_file = tmp_path / 'retry_counter.txt'

    # Create dynamic spec with counter file
    node_retry = TaskNode(
        fn=retry_then_ok_task,
        kwargs={'counter_file': str(counter_file), 'succeed_on_attempt': 2},
    )

    from tests.e2e.tasks.instance import app

    spec_retry = app.workflow(
        name='e2e_retry_test',
        tasks=[node_retry],
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = spec_retry.start()

        # Allow time for retries (retry policy uses 1s base exponential)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=30.0
        )
        assert (
            status == 'COMPLETED'
        ), f'Workflow should complete after retries, got {status}'

        # Verify the task completed with success
        result = handle.get(timeout_ms=1000)
        assert_ok(result)

        # Verify attempts were made
        assert counter_file.exists(), 'Counter file should exist'
        with open(counter_file) as f:
            final_attempt = int(f.read().strip())
        assert (
            final_attempt >= 2
        ), f'Should have at least 2 attempts, got {final_attempt}'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_task_retries_exhausted(
    broker: PostgresBroker, tmp_path: Path,
) -> None:
    """T6.10b: Workflow FAILS when task exhausts all retries."""
    from horsies.core.models.workflow import TaskNode
    from tests.e2e.tasks.workflows import retry_then_ok_task
    from tests.e2e.tasks.instance import app

    counter_file = tmp_path / 'retry_exhausted_counter.txt'

    # succeed_on_attempt=10 but max_retries=3 → 4 attempts total, never succeeds
    node_retry = TaskNode(
        fn=retry_then_ok_task,
        kwargs={'counter_file': str(counter_file), 'succeed_on_attempt': 10},
    )

    spec_retry_fail = app.workflow(
        name='e2e_retry_exhausted',
        tasks=[node_retry],
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = spec_retry_fail.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=60.0
        )
        assert status == 'FAILED', f'Workflow should FAIL after exhausting retries, got {status}'

        # Verify attempts were made (initial + 3 retries = 4)
        assert counter_file.exists(), 'Counter file should exist'
        with open(counter_file) as f:
            final_attempt = int(f.read().strip())
        assert final_attempt == 4, f'Should have exactly 4 attempts (1 initial + 3 retries), got {final_attempt}'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 1
        assert db_tasks[0]['status'] == 'FAILED', f"Task should be FAILED, got {db_tasks[0]['status']}"


# =============================================================================
# T6.11: on_error=PAUSE semantics
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_on_error_pause_stops_workflow(broker: PostgresBroker) -> None:
    """T6.11a: Workflow auto-pauses when task fails with on_error=PAUSE."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_on_error_pause.start()

        # Wait for workflow to reach PAUSED state (not terminal)
        async def _is_paused() -> bool:
            wf_status = await get_workflow_status(
                broker.session_factory, handle.workflow_id
            )
            return wf_status == 'PAUSED'

        paused = await _wait_until(_is_paused, timeout_s=15.0)
        assert paused, 'Workflow should auto-pause after task failure'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 3

        assert db_tasks[0]['status'] == 'COMPLETED', f"A should be COMPLETED, got {db_tasks[0]['status']}"
        assert db_tasks[1]['status'] == 'FAILED', f"B should be FAILED, got {db_tasks[1]['status']}"
        assert db_tasks[2]['status'] == 'PENDING', f"C should still be PENDING (paused before enqueue), got {db_tasks[2]['status']}"

        # handle.get() should return WORKFLOW_PAUSED error
        result = handle.get(timeout_ms=1000)
        assert_err(result, expected_code='WORKFLOW_PAUSED')


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_on_error_pause_resume_completes(broker: PostgresBroker) -> None:
    """T6.11b: Resume after auto-pause finalizes workflow (C stays SKIPPED since B failed)."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_on_error_pause.start()

        # Wait for auto-pause
        async def _is_paused() -> bool:
            wf_status = await get_workflow_status(
                broker.session_factory, handle.workflow_id
            )
            return wf_status == 'PAUSED'

        paused = await _wait_until(_is_paused, timeout_s=15.0)
        assert paused, 'Workflow should auto-pause after task failure'

        # Resume the workflow
        resumed = handle.resume()
        assert resumed is True, 'Workflow should be resumable after auto-pause'

        # Wait for terminal state
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'FAILED', f'Workflow should FAIL (B failed), got {status}'

        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert db_tasks[0]['status'] == 'COMPLETED', f"A should be COMPLETED, got {db_tasks[0]['status']}"
        assert db_tasks[1]['status'] == 'FAILED', f"B should be FAILED, got {db_tasks[1]['status']}"
        # C depends on B which failed, so C should be SKIPPED
        assert db_tasks[2]['status'] == 'SKIPPED', f"C should be SKIPPED (B failed), got {db_tasks[2]['status']}"


# =============================================================================
# T6.9c: Workflow recovery after worker crash (full chain)
# =============================================================================

RECOVERY_INSTANCE = 'tests.e2e.tasks.instance_recovery:app'


def _make_recovery_ready_check() -> Callable[[], bool]:
    """Ready check for the recovery app instance."""
    from horsies.core.task_decorator import TaskHandle
    from tests.e2e.tasks import instance_recovery

    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = instance_recovery.healthcheck.send()
        result = handle.get(timeout_ms=2000)
        return result.is_ok()

    return _check


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_recovers_after_worker_crash(
    recovery_broker: PostgresBroker,
) -> None:
    """T6.9c: Workflow with in-flight tasks recovers after worker crash.

    DAG: A(50ms) -> B(50ms) -> C(60s) -> E(50ms, recovery queue) -> F(50ms, recovery queue)
                            -> D(60s) -/

    E has allow_failed_deps=True so it runs after C,D are marked FAILED.
    E and F are on "recovery" queue (max_concurrency=1) — sequential execution.

    Kill worker after A,B complete and C,D are RUNNING.
    Restart worker. Recovery chain:
        reaper marks C,D FAILED (WORKER_CRASHED)
        -> recover_stuck_workflows resolves wt mismatch
        -> E runs (allow_failed_deps=True) on recovery queue
        -> F runs after E on recovery queue
        -> workflow finalizes as FAILED (C,D failed despite E,F succeeding)
    """
    import json
    import os
    import signal

    from sqlalchemy import text as sa_text
    from tests.e2e.tasks import instance_recovery

    with run_worker(
        RECOVERY_INSTANCE,
        processes=2,
        ready_check=_make_recovery_ready_check(),
    ) as worker_proc:
        handle = instance_recovery.spec_recovery_crash.start()

        # Wait until A (idx 0) and B (idx 1) are COMPLETED in workflow_tasks
        async def _ab_completed() -> bool:
            tasks = await get_workflow_tasks(
                recovery_broker.session_factory, handle.workflow_id,
            )
            if len(tasks) < 6:
                return False
            return (
                tasks[0]['status'] == 'COMPLETED'
                and tasks[1]['status'] == 'COMPLETED'
            )

        reached = await _wait_until(_ab_completed, timeout_s=15.0)
        assert reached, 'A and B should complete before crash'

        # Get underlying task_ids for C (idx 2) and D (idx 3)
        async with recovery_broker.session_factory() as session:
            result = await session.execute(
                sa_text("""
                    SELECT task_index, task_id FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf_id AND task_index IN (2, 3)
                    ORDER BY task_index
                """),
                {'wf_id': handle.workflow_id},
            )
            rows = result.fetchall()
            assert len(rows) == 2, f'Expected 2 rows for C,D, got {len(rows)}'
            task_id_c = rows[0][1]
            task_id_d = rows[1][1]

        assert task_id_c is not None, 'C should have been enqueued (task_id set)'
        assert task_id_d is not None, 'D should have been enqueued (task_id set)'

        # Wait until both underlying tasks are actually RUNNING
        await wait_for_status(
            recovery_broker.session_factory, task_id_c, 'RUNNING', timeout_s=15.0,
        )
        await wait_for_status(
            recovery_broker.session_factory, task_id_d, 'RUNNING', timeout_s=15.0,
        )

        # Kill the entire worker process group
        os.killpg(worker_proc.pid, signal.SIGKILL)
        worker_proc.wait(timeout=5.0)

    # Start a new worker — its reaper detects stale tasks and recovery kicks in.
    # E and F will be picked up on the "recovery" queue after C,D are resolved.
    with run_worker(
        RECOVERY_INSTANCE,
        processes=2,
        ready_check=_make_recovery_ready_check(),
    ):
        status = await wait_for_workflow_completion(
            recovery_broker.session_factory, handle.workflow_id, timeout_s=30.0,
        )

    # -- Assertions --

    assert status == 'FAILED', f'Workflow should be FAILED, got {status}'

    db_tasks = await get_workflow_tasks(
        recovery_broker.session_factory, handle.workflow_id,
    )
    assert len(db_tasks) == 6, f'Expected 6 workflow tasks, got {len(db_tasks)}'

    # A, B: preserved — completed before crash
    assert db_tasks[0]['status'] == 'COMPLETED', (
        f"A should be COMPLETED, got {db_tasks[0]['status']}"
    )
    assert db_tasks[1]['status'] == 'COMPLETED', (
        f"B should be COMPLETED, got {db_tasks[1]['status']}"
    )

    # C, D: marked FAILED by recovery (in-flight when killed)
    assert db_tasks[2]['status'] == 'FAILED', (
        f"C should be FAILED, got {db_tasks[2]['status']}"
    )
    assert db_tasks[3]['status'] == 'FAILED', (
        f"D should be FAILED, got {db_tasks[3]['status']}"
    )

    # E: COMPLETED — ran after recovery (allow_failed_deps=True, recovery queue)
    assert db_tasks[4]['status'] == 'COMPLETED', (
        f"E should be COMPLETED, got {db_tasks[4]['status']}"
    )

    # F: COMPLETED — ran after E on recovery queue (max_concurrency=1)
    assert db_tasks[5]['status'] == 'COMPLETED', (
        f"F should be COMPLETED, got {db_tasks[5]['status']}"
    )

    # Verify WORKER_CRASHED error code on crashed task C
    async with recovery_broker.session_factory() as session:
        result = await session.execute(
            sa_text("""
                SELECT result FROM horsies_tasks WHERE id = :id
            """),
            {'id': task_id_c},
        )
        row = result.fetchone()
        assert row is not None and row[0] is not None, 'C task should have a result'
        result_data = json.loads(row[0])
        error_code = result_data.get('err', {}).get('error_code', '')
        assert error_code == 'WORKER_CRASHED', (
            f"C should have WORKER_CRASHED error, got {error_code}"
        )

    # Verify E and F ran on the "recovery" queue
    async with recovery_broker.session_factory() as session:
        result = await session.execute(
            sa_text("""
                SELECT wt.task_index, t.queue_name
                FROM horsies_workflow_tasks wt
                JOIN horsies_tasks t ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index IN (4, 5)
                ORDER BY wt.task_index
            """),
            {'wf_id': handle.workflow_id},
        )
        queue_rows = result.fetchall()
        assert len(queue_rows) == 2, f'Expected 2 queue rows for E,F, got {len(queue_rows)}'
        assert queue_rows[0][1] == 'recovery', (
            f"E should be on 'recovery' queue, got {queue_rows[0][1]}"
        )
        assert queue_rows[1][1] == 'recovery', (
            f"F should be on 'recovery' queue, got {queue_rows[1][1]}"
        )
