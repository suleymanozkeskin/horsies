"""Layer 3 e2e tests: scheduler functionality."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import text

from horsies.core.errors import ConfigurationError

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.task_pg import ScheduleStateModel
from horsies.core.models.schedule import (
    ScheduleConfig,
    TaskSchedule,
    IntervalSchedule,
)

from tests.e2e.helpers.worker import run_worker, run_scheduler
from tests.e2e.tasks import instance_scheduler


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEDULER_INSTANCE = 'tests.e2e.tasks.instance_scheduler:app'


@pytest.fixture
def scheduler_broker() -> PostgresBroker:
    """Broker instance for scheduler tests."""
    return instance_scheduler.broker


# =============================================================================
# T3.1 Schedule Persistence
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_schedule_persistence(scheduler_broker: PostgresBroker) -> None:
    """T3.1: Verify schedules are stored in schedule_state table after scheduler starts."""
    # Start scheduler (it will initialize schedule state in DB)
    with run_scheduler(SCHEDULER_INSTANCE, timeout=10.0):
        # Poll for scheduler initialization instead of relying on a fixed delay.
        deadline = time.time() + 5.0
        state: ScheduleStateModel | None = None
        while time.time() < deadline:
            async with scheduler_broker.session_factory() as session:
                state = await session.get(
                    ScheduleStateModel, 'e2e_scheduled_simple_interval'
                )
            if state is not None:
                break
            await asyncio.sleep(0.1)

        assert state is not None, 'Schedule state should exist in DB'
        assert state.next_run_at is not None, 'next_run_at should be set'
        assert state.next_run_at > datetime.now(
            timezone.utc
        ), 'next_run_at should be in the future'
        assert state.run_count == 0, 'run_count should be 0 initially'


# =============================================================================
# T3.2 Schedule Executes at/after Due Time
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_schedule_executes_after_due_time(
    scheduler_broker: PostgresBroker,
) -> None:
    """T3.2: Task runs only after schedule is due."""
    # Record the time before starting
    start_time = datetime.now(timezone.utc)

    # Start scheduler AND worker (scheduler enqueues, worker executes)
    with run_scheduler(SCHEDULER_INSTANCE, timeout=10.0):
        with run_worker(SCHEDULER_INSTANCE, timeout=10.0):
            # The predefined schedule runs every 2 seconds.
            # Wait for first execution (schedule check + interval)
            # Poll DB for task creation
            deadline = time.time() + 10.0
            task_created = False

            while time.time() < deadline:
                async with scheduler_broker.session_factory() as session:
                    result = await session.execute(
                        text(
                            """
                            SELECT created_at FROM horsies_tasks
                            WHERE task_name = 'e2e_scheduled_simple'
                            ORDER BY created_at ASC
                            LIMIT 1
                        """
                        )
                    )
                    row = result.fetchone()
                    if row is not None:
                        created_at = row[0]
                        # Verify task was created after start time
                        # (accounting for timezone awareness)
                        if created_at.tzinfo is None:
                            created_at = created_at.replace(tzinfo=timezone.utc)
                        assert (
                            created_at >= start_time
                        ), f'Task created at {created_at} before start time {start_time}'
                        task_created = True
                        break
                await asyncio.sleep(0.2)

            assert task_created, 'Scheduled task should have been created'


# =============================================================================
# T3.3 Schedule Does Not Execute Early
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_schedule_does_not_execute_early(
    scheduler_broker: PostgresBroker,
) -> None:
    """T3.3: No premature execution for future schedules."""
    # The predefined schedule has a 2-second interval.
    # First verify the scheduler is actually running (state row exists),
    # then assert no tasks were created in the initial window.

    with run_scheduler(SCHEDULER_INSTANCE, timeout=10.0):
        # Wait for scheduler to initialize (state row exists)
        init_deadline = time.time() + 5.0
        scheduler_ready = False
        while time.time() < init_deadline:
            async with scheduler_broker.session_factory() as session:
                state = await session.get(
                    ScheduleStateModel, 'e2e_scheduled_simple_interval'
                )
            if state is not None:
                scheduler_ready = True
                break
            await asyncio.sleep(0.1)

        assert scheduler_ready, 'Scheduler should have initialized state'

        # Now check that no tasks were created yet (next_run_at is ~2s in the future)
        async with scheduler_broker.session_factory() as session:
            result = await session.execute(
                text(
                    """
                    SELECT COUNT(*) FROM horsies_tasks
                    WHERE task_name = 'e2e_scheduled_simple'
                """
                )
            )
            tasks_found = result.scalar() or 0

        assert (
            tasks_found == 0
        ), f'No tasks should be created before next_run_at, found {tasks_found}'


# =============================================================================
# T3.4 Repeating Schedule (Short Interval)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_repeating_schedule(scheduler_broker: PostgresBroker) -> None:
    """T3.4: Verify repeated scheduling creates multiple tasks."""
    # The predefined schedule runs every 2 seconds.
    # Wait for at least 2 cycles (~5 seconds to be safe)

    with run_scheduler(SCHEDULER_INSTANCE, timeout=10.0):
        with run_worker(SCHEDULER_INSTANCE, timeout=10.0):
            # Wait for at least 2 tasks to be created
            deadline = time.time() + 8.0
            task_count = 0
            created_times: list[datetime] = []

            while time.time() < deadline:
                async with scheduler_broker.session_factory() as session:
                    result = await session.execute(
                        text(
                            """
                            SELECT created_at FROM horsies_tasks
                            WHERE task_name = 'e2e_scheduled_simple'
                            ORDER BY created_at ASC
                        """
                        )
                    )
                    rows = result.fetchall()
                    task_count = len(rows)
                    created_times = [row[0] for row in rows]

                    if task_count >= 2:
                        break
                await asyncio.sleep(0.3)

            assert task_count >= 2, f'Expected at least 2 tasks, got {task_count}'

            # Verify tasks have increasing created_at times
            for i in range(1, len(created_times)):
                assert (
                    created_times[i] > created_times[i - 1]
                ), f'Task {i} created_at should be after task {i-1}'

            # Verify schedule_state was updated
            async with scheduler_broker.session_factory() as session:
                state = await session.get(
                    ScheduleStateModel, 'e2e_scheduled_simple_interval'
                )
                assert state is not None
                assert (
                    state.run_count >= 2
                ), f'run_count should be >= 2, got {state.run_count}'
                assert state.last_run_at is not None, 'last_run_at should be set'


# =============================================================================
# T3.5 Invalid Schedule Configuration
# =============================================================================


@pytest.mark.e2e
def test_invalid_schedule_configuration() -> None:
    """T3.5: Invalid schedule configuration is rejected at config time."""
    # Test that IntervalSchedule requires at least one time unit
    with pytest.raises(ConfigurationError, match='at least one time unit'):
        IntervalSchedule()  # No time units specified

    # Test that ScheduleConfig validates unique schedule names
    with pytest.raises(ConfigurationError, match='duplicate schedule names'):
        ScheduleConfig(
            schedules=[
                TaskSchedule(
                    name='duplicate_name',
                    task_name='some_task',
                    pattern=IntervalSchedule(seconds=5),
                ),
                TaskSchedule(
                    name='duplicate_name',  # Duplicate!
                    task_name='another_task',
                    pattern=IntervalSchedule(seconds=10),
                ),
            ]
        )


# =============================================================================
# T3.6 Missing Task Resolution
# =============================================================================

INVALID_SCHEDULER_INSTANCE = str(
    REPO_ROOT / 'tests/e2e/tasks/instance_scheduler_invalid.py'
)


@pytest.mark.e2e
def test_scheduler_fails_with_nonexistent_task() -> None:
    """T3.6: Scheduler should fail to start if scheduled task doesn't exist.

    This is a regression guard: if someone changes the validator or model,
    this test will catch it. The scheduler must fail fast at startup when
    a schedule references a task that isn't registered.
    """
    import subprocess
    import os

    # Build command to start scheduler with invalid instance
    cmd = [
        'uv',
        'run',
        'horsies',
        'scheduler',
        INVALID_SCHEDULER_INSTANCE,
        '--loglevel=warning',
    ]

    env = os.environ.copy()
    env['PYTHONPATH'] = str(REPO_ROOT)

    # Run scheduler and expect it to fail
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )

    # Scheduler should exit with non-zero code
    assert (
        result.returncode != 0
    ), f'Scheduler should have failed but exited with code {result.returncode}'

    # Error message should mention the missing task
    combined_output = result.stdout + result.stderr
    assert (
        'not registered' in combined_output.lower()
        or 'this_task_does_not_exist' in combined_output.lower()
    ), f'Error should mention unregistered task. Output:\n{combined_output}'


# =============================================================================
# T3.7 Disabled Schedule Does Not Fire
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_disabled_schedule_does_not_fire(
    scheduler_broker: PostgresBroker,
) -> None:
    """T3.7: Disabled schedule should not create state or enqueue tasks."""
    with run_scheduler(SCHEDULER_INSTANCE, timeout=10.0):
        # Wait for an enabled schedule to confirm scheduler is running
        deadline = time.time() + 5.0
        scheduler_ready = False
        while time.time() < deadline:
            async with scheduler_broker.session_factory() as session:
                state = await session.get(
                    ScheduleStateModel, 'e2e_scheduled_simple_interval'
                )
            if state is not None:
                scheduler_ready = True
                break
            await asyncio.sleep(0.1)

        assert scheduler_ready, 'Scheduler should be running (enabled schedule has state)'

        # Disabled schedule should have no state row
        async with scheduler_broker.session_factory() as session:
            disabled_state = await session.get(
                ScheduleStateModel, 'e2e_disabled_schedule'
            )

        assert disabled_state is None, (
            'Disabled schedule should not have a state row in the DB'
        )


# =============================================================================
# T3.8 Schedule With Args Passes Arguments
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_schedule_with_args_passes_arguments(
    scheduler_broker: PostgresBroker,
) -> None:
    """T3.8: Scheduled task receives args and produces correct result (42*2=84)."""
    with run_scheduler(SCHEDULER_INSTANCE, timeout=10.0):
        with run_worker(SCHEDULER_INSTANCE, timeout=10.0):
            # Poll for the task to complete with correct result
            deadline = time.time() + 10.0
            found_result = False

            while time.time() < deadline:
                async with scheduler_broker.session_factory() as session:
                    result = await session.execute(
                        text(
                            """
                            SELECT result FROM horsies_tasks
                            WHERE task_name = 'e2e_scheduled_with_args'
                            AND status = 'COMPLETED'
                            LIMIT 1
                        """
                        )
                    )
                    row = result.fetchone()
                    if row is not None and row[0] is not None:
                        result_str = str(row[0])
                        # The result should contain 84 (42 * 2)
                        if '84' in result_str:
                            found_result = True
                            break
                await asyncio.sleep(0.3)

            assert found_result, (
                'Scheduled task with args should complete with result containing 84'
            )


# =============================================================================
# T3.9 Config Hash Change Recalculates Next Run
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_config_hash_change_recalculates_next_run(
    scheduler_broker: PostgresBroker,
) -> None:
    """T3.9: Corrupted config_hash triggers next_run_at recalculation on restart."""
    # Phase 1: Start scheduler, record state
    with run_scheduler(SCHEDULER_INSTANCE, timeout=10.0):
        deadline = time.time() + 5.0
        original_state: ScheduleStateModel | None = None
        while time.time() < deadline:
            async with scheduler_broker.session_factory() as session:
                original_state = await session.get(
                    ScheduleStateModel, 'e2e_scheduled_simple_interval'
                )
            if original_state is not None:
                break
            await asyncio.sleep(0.1)

        assert original_state is not None
        original_hash = original_state.config_hash

    # Phase 2: Corrupt the config_hash in DB
    async with scheduler_broker.session_factory() as session:
        await session.execute(
            text(
                """
                UPDATE horsies_schedule_state
                SET config_hash = 'corrupted_hash'
                WHERE schedule_name = 'e2e_scheduled_simple_interval'
            """
            )
        )
        await session.commit()

    # Phase 3: Restart scheduler and verify hash is corrected
    with run_scheduler(SCHEDULER_INSTANCE, timeout=10.0):
        deadline = time.time() + 5.0
        corrected = False
        while time.time() < deadline:
            async with scheduler_broker.session_factory() as session:
                state = await session.get(
                    ScheduleStateModel, 'e2e_scheduled_simple_interval'
                )
            if state is not None and state.config_hash != 'corrupted_hash':
                corrected = True
                assert state.config_hash == original_hash, (
                    f'Config hash should be restored to {original_hash}, '
                    f'got {state.config_hash}'
                )
                break
            await asyncio.sleep(0.1)

        assert corrected, 'Scheduler should recalculate config_hash on restart'


# =============================================================================
# T3.10 Catch-Up Missed Runs
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_catch_up_missed_enqueues_missed_runs(
    scheduler_broker: PostgresBroker,
) -> None:
    """T3.10: catch_up_missed=True enqueues multiple tasks for missed intervals."""
    # Phase 1: Start scheduler to initialize the catch-up schedule state
    state: ScheduleStateModel | None = None
    with run_scheduler(SCHEDULER_INSTANCE, timeout=10.0):
        deadline = time.time() + 5.0
        while time.time() < deadline:
            async with scheduler_broker.session_factory() as session:
                state = await session.get(
                    ScheduleStateModel, 'e2e_schedule_catch_up'
                )
            if state is not None:
                break
            await asyncio.sleep(0.1)

        assert state is not None, 'Catch-up schedule state should be initialized'

    # Phase 2: Set next_run_at to 5 seconds in the past to simulate downtime
    from datetime import timedelta

    past_time = datetime.now(timezone.utc) - timedelta(seconds=5)
    async with scheduler_broker.session_factory() as session:
        await session.execute(
            text(
                """
                UPDATE horsies_schedule_state
                SET next_run_at = :past_time
                WHERE schedule_name = 'e2e_schedule_catch_up'
            """
            ),
            {'past_time': past_time},
        )
        await session.commit()

    # Count existing catch_up tasks before restart
    async with scheduler_broker.session_factory() as session:
        result = await session.execute(
            text(
                """
                SELECT COUNT(*) FROM horsies_tasks
                WHERE task_name = 'e2e_catch_up_task'
            """
            )
        )
        tasks_before = result.scalar() or 0

    # Phase 3: Restart scheduler with worker - should catch up missed runs
    with run_scheduler(SCHEDULER_INSTANCE, timeout=10.0):
        with run_worker(SCHEDULER_INSTANCE, timeout=10.0):
            deadline = time.time() + 8.0
            tasks_after = 0
            while time.time() < deadline:
                async with scheduler_broker.session_factory() as session:
                    result = await session.execute(
                        text(
                            """
                            SELECT COUNT(*) FROM horsies_tasks
                            WHERE task_name = 'e2e_catch_up_task'
                        """
                        )
                    )
                    tasks_after = result.scalar() or 0
                    # We need at least 2 new tasks from catch-up (5s gap, 1s interval)
                    if (tasks_after - tasks_before) >= 2:
                        break
                await asyncio.sleep(0.3)

            new_tasks = tasks_after - tasks_before
            assert new_tasks >= 2, (
                f'Catch-up should have enqueued at least 2 tasks, '
                f'got {new_tasks} new tasks'
            )


# =============================================================================
# T3.11 Scheduler Restart Reuses Existing State
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_scheduler_restart_reuses_existing_state(
    scheduler_broker: PostgresBroker,
) -> None:
    """T3.11: Restarted scheduler preserves run_count and config_hash."""
    # Phase 1: Start scheduler + worker, let it run at least once
    state: ScheduleStateModel | None = None
    with run_scheduler(SCHEDULER_INSTANCE, timeout=10.0):
        with run_worker(SCHEDULER_INSTANCE, timeout=10.0):
            deadline = time.time() + 8.0
            while time.time() < deadline:
                async with scheduler_broker.session_factory() as session:
                    state = await session.get(
                        ScheduleStateModel, 'e2e_scheduled_simple_interval'
                    )
                if state is not None and state.run_count >= 1:
                    break
                await asyncio.sleep(0.3)

    assert state is not None
    assert state.run_count >= 1, (
        f'Schedule should have run at least once, got run_count={state.run_count}'
    )
    run_count_before = state.run_count
    hash_before = state.config_hash

    # Phase 2: Restart scheduler and verify state is preserved (not reset)
    with run_scheduler(SCHEDULER_INSTANCE, timeout=10.0):
        # Wait for scheduler to re-initialize
        await asyncio.sleep(1.0)

        async with scheduler_broker.session_factory() as session:
            state_after = await session.get(
                ScheduleStateModel, 'e2e_scheduled_simple_interval'
            )

        assert state_after is not None
        assert state_after.run_count >= run_count_before, (
            f'run_count should not decrease after restart: '
            f'before={run_count_before}, after={state_after.run_count}'
        )
        assert state_after.config_hash == hash_before, (
            f'config_hash should be unchanged after restart: '
            f'before={hash_before}, after={state_after.config_hash}'
        )
