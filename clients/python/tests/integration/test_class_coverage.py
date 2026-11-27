"""The fleet check and the class report against a real Postgres.

- A pending (queue, class) is served, served without capacity, or unserved,
  from the live snapshots' queues and budgets.
- A snapshot older than the live window does not serve anything.
- An isolated retry of a shared task is unserved when no live worker has
  isolated children.
- A live snapshot with unreadable budgets fails the read and names the
  worker.
- The report gives the wait of each stream's oldest due row and the load of
  each budget of each live worker, counted as the claim counts it.
- The reaper pass logs an unserved stream: the test enters at
  ``_run_reaper_pass``, above the wiring.
- Both reads use the claim index.
- A worker of a real profile with shared async children writes its snapshot,
  and both reads give its ``SHARED_ASYNC`` budget with the seats of its
  shared children.

Each test empties ``horsies_tasks`` and ``horsies_worker_states``: both reads
cover the whole fleet.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import json
import uuid
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import DatabaseErrorCode
from horsies.core.models.class_coverage import (
    BudgetLoad,
    StreamWaitNoneDue,
    StreamWaitDue,
    StreamCoverage,
    StreamCoverageStateServed,
    StreamCoverageStateUnserved,
    StreamCoverageStateNoCapacity,
    WorkerExecutionClassLoad,
)
from horsies.core.models.health import WORKER_STATE_LIVE_WINDOW_SECONDS
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

ISOLATED = Execution.ISOLATED
ASYNC = Execution.SHARED_ASYNC
THREAD = Execution.SHARED_THREAD
ALL_CLASSES = ['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD']
ISOLATED_CHILDREN = {'ISOLATED': {'capacity': 4, 'serves': ALL_CLASSES}}
ASYNC_CHILDREN_ONLY = {'SHARED_ASYNC': {'capacity': 40, 'serves': ['SHARED_ASYNC']}}


@pytest_asyncio.fixture
async def empty_fleet(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - ensures migrations are applied
) -> AsyncGenerator[None, None]:
    """No task row and no worker snapshot, before and after the test."""
    await session.execute(text('TRUNCATE horsies_tasks CASCADE'))
    await session.execute(text('DELETE FROM horsies_worker_states'))
    await session.commit()
    yield
    await session.execute(text('TRUNCATE horsies_tasks CASCADE'))
    await session.execute(text('DELETE FROM horsies_worker_states'))
    await session.commit()


async def _snapshot(
    session: AsyncSession,
    worker_id: str,
    class_capacity: object,
    *,
    queues: list[str],
    age_s: int = 5,
) -> None:
    """One worker snapshot, ``age_s`` seconds old in database time."""
    await session.execute(
        text("""
            INSERT INTO horsies_worker_states (
                worker_id, snapshot_at, hostname, pid, processes,
                max_claim_batch, max_claim_per_worker, queues,
                queue_priorities, queue_max_concurrency, recovery_config,
                tasks_running, tasks_claimed, worker_started_at,
                slots_down, class_capacity, telemetry
            ) VALUES (
                :worker_id, now() - make_interval(secs => CAST(:age AS integer)),
                'itest-host',
                4242, 4, 0, 4, :queues, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
                0, 0, now() - interval '1 hour',
                '[]'::jsonb, CAST(:class_capacity AS JSONB), jsonb_build_object('endpoint', jsonb_build_object('state', 'off'))
            )
        """),
        {
            'worker_id': worker_id,
            'age': age_s,
            'queues': queues,
            'class_capacity': json.dumps(class_capacity),
        },
    )
    await session.commit()


async def _task(
    session: AsyncSession,
    *,
    queue: str,
    execution: Execution,
    priority: int = 100,
    due_in_s: int = -10,
    status: str = 'PENDING',
    owner: str | None = None,
    lease_s: int | None = None,
) -> None:
    """One live row. ``due_in_s`` sets enqueued_at relative to now();
    ``lease_s`` sets claim_expires_at relative to now() for a CLAIMED row."""
    task_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
              (
                  id, task_name, queue_name, priority, input, status, enqueued_at,
                  claimed_by_worker_id, claimed_at, claim_expires_at, started_at,
                  is_workflow_task, created_at, updated_at, retry_count, max_retries,
                  retention_class_key, command_fingerprint_version,
                  command_fingerprint, retain_rerun_input, execution, sent_at,
                  worker_pid, worker_hostname, worker_process_name
              ) VALUES (
                  :id, 'class_task', :queue, :priority, '{}', CAST(:status AS varchar),
                  now() + make_interval(secs => CAST(:due_in AS integer)),
                  CAST(:owner AS varchar),
                  CASE WHEN CAST(:owner AS varchar) IS NULL THEN NULL ELSE now() END,
                  CASE WHEN CAST(:lease AS integer) IS NULL THEN NULL
                    ELSE now() + make_interval(secs => CAST(:lease AS integer)) END,
                  CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN now() ELSE NULL END,
                  false, now(), now(), 0, 0, 'standard_30d', 1,
                  sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')), false,
                  :execution, NOW(),
                  CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 4242 END,
                  CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 'host-1' END,
                  CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 'child-1' END
              )
        """),
        {
            'id': task_id,
            'queue': queue,
            'priority': priority,
            'status': status,
            'due_in': due_in_s,
            'owner': owner,
            'lease': lease_s,
            'execution': execution.value,
        },
    )
    await session.commit()


@pytest.mark.usefixtures('empty_fleet')
class TestClassCoverage:
    async def test_each_coverage_state_from_the_live_snapshots(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        await _snapshot(session, 'w-iso', ISOLATED_CHILDREN, queues=['a'])
        await _snapshot(
            session,
            'w-down',
            {'ISOLATED': {'capacity': 0, 'serves': ALL_CLASSES}},
            queues=['b'],
        )
        await _task(session, queue='a', execution=ASYNC)
        await _task(session, queue='b', execution=ISOLATED)
        await _task(session, queue='c', execution=THREAD)

        match await broker.get_execution_class_coverage_async():
            case Ok(coverage):
                pass
            case Err(error):
                raise AssertionError(error.message)

        assert coverage.live_workers == 2
        assert coverage.streams == (
            StreamCoverage('a', ASYNC, StreamCoverageStateServed(workers=1, capacity=4)),
            StreamCoverage('b', ISOLATED, StreamCoverageStateNoCapacity(workers=1)),
            StreamCoverage('c', THREAD, StreamCoverageStateUnserved()),
        )

    async def test_a_snapshot_older_than_the_live_window_serves_nothing(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        await _snapshot(
            session,
            'w-gone',
            ISOLATED_CHILDREN,
            queues=['a'],
            age_s=WORKER_STATE_LIVE_WINDOW_SECONDS + 5,
        )
        await _task(session, queue='a', execution=ISOLATED)

        match await broker.get_execution_class_coverage_async():
            case Ok(coverage):
                pass
            case Err(error):
                raise AssertionError(error.message)

        assert coverage.live_workers == 0
        assert coverage.streams == (StreamCoverage('a', ISOLATED, StreamCoverageStateUnserved()),)

    async def test_only_the_latest_snapshot_of_a_worker_counts(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """A worker whose slots went down after an earlier snapshot."""
        await _snapshot(session, 'w1', ISOLATED_CHILDREN, queues=['a'], age_s=60)
        await _snapshot(
            session,
            'w1',
            {'ISOLATED': {'capacity': 0, 'serves': ALL_CLASSES}},
            queues=['a'],
            age_s=1,
        )
        await _task(session, queue='a', execution=ISOLATED)

        match await broker.get_execution_class_coverage_async():
            case Ok(coverage):
                pass
            case Err(error):
                raise AssertionError(error.message)

        assert coverage.live_workers == 1
        assert coverage.streams[0].coverage == StreamCoverageStateNoCapacity(workers=1)

    async def test_an_isolated_retry_of_a_shared_task_is_unserved_without_isolated_children(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """The shared task went back to the queue as ISOLATED; every live
        worker runs only SHARED_ASYNC children."""
        await _snapshot(session, 'w-async', ASYNC_CHILDREN_ONLY, queues=['a'])
        await _task(session, queue='a', execution=ASYNC)
        await _task(session, queue='a', execution=ISOLATED)

        match await broker.get_execution_class_coverage_async():
            case Ok(coverage):
                pass
            case Err(error):
                raise AssertionError(error.message)

        assert coverage.streams == (
            StreamCoverage('a', ISOLATED, StreamCoverageStateUnserved()),
            StreamCoverage('a', ASYNC, StreamCoverageStateServed(workers=1, capacity=40)),
        )

    async def test_a_delayed_row_counts_as_pending(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """A row due later still needs a worker of its class."""
        await _task(session, queue='a', execution=ISOLATED, due_in_s=3600)

        match await broker.get_execution_class_coverage_async():
            case Ok(coverage):
                pass
            case Err(error):
                raise AssertionError(error.message)

        assert coverage.streams == (StreamCoverage('a', ISOLATED, StreamCoverageStateUnserved()),)

    async def test_rows_that_are_not_pending_make_no_stream(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        await _task(
            session,
            queue='a',
            execution=ISOLATED,
            status='RUNNING',
            owner='w-gone',
        )

        match await broker.get_execution_class_coverage_async():
            case Ok(coverage):
                assert coverage.streams == ()
            case Err(error):
                raise AssertionError(error.message)

    async def test_a_live_snapshot_with_unreadable_budgets_fails_the_read(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        await _snapshot(
            session,
            'w-odd',
            {'EXCLUSIVE': {'capacity': 1, 'serves': []}},
            queues=['a'],
        )
        await _task(session, queue='a', execution=ISOLATED)

        match await broker.get_execution_class_coverage_async():
            case Err(error):
                assert error.code == DatabaseErrorCode.MONITORING_QUERY_FAILED
                assert error.retryable is False
                assert 'w-odd' in error.message
                assert 'class_capacity is unreadable' in error.message
            case Ok(coverage):
                raise AssertionError(coverage)


@pytest.mark.usefixtures('empty_fleet')
class TestClassReport:
    async def test_the_oldest_due_row_across_priorities_is_the_wait(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        await _snapshot(session, 'w1', ISOLATED_CHILDREN, queues=['a'])
        await _task(session, queue='a', execution=ISOLATED, priority=1, due_in_s=-30)
        await _task(session, queue='a', execution=ISOLATED, priority=50, due_in_s=-300)
        await _task(session, queue='a', execution=ISOLATED, priority=50, due_in_s=-100)
        await _task(session, queue='a', execution=ISOLATED, priority=90, due_in_s=600)
        await _task(session, queue='a', execution=ASYNC, due_in_s=120)

        match await broker.get_execution_class_report_async():
            case Ok(report):
                pass
            case Err(error):
                raise AssertionError(error.message)

        oldest = (
            await session.execute(
                text(
                    'SELECT min(enqueued_at) FROM horsies_tasks '
                    "WHERE execution = 'ISOLATED'"
                ),
            )
        ).scalar_one()
        next_due = (
            await session.execute(
                text(
                    'SELECT enqueued_at FROM horsies_tasks '
                    "WHERE execution = 'SHARED_ASYNC'"
                ),
            )
        ).scalar_one()
        isolated, shared = report.streams
        assert (isolated.queue, isolated.execution) == ('a', ISOLATED)
        assert isolated.wait == StreamWaitDue(oldest, report.checked_at - oldest)
        assert report.checked_at - oldest >= timedelta(seconds=300)
        assert isolated.coverage == StreamCoverageStateServed(workers=1, capacity=4)
        assert shared.wait == StreamWaitNoneDue(next_due)

    async def test_budget_load_counts_rows_as_the_claim_does(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """RUNNING and live CLAIMED rows count; a CLAIMED row with an expired
        lease and another worker's rows do not."""
        two_budgets = {
            'ISOLATED': {'capacity': 4, 'serves': ['ISOLATED', 'SHARED_THREAD']},
            'SHARED_ASYNC': {'capacity': 40, 'serves': ['SHARED_ASYNC']},
        }
        await _snapshot(session, 'w1', two_budgets, queues=['a'])
        await _task(
            session, queue='a', execution=ISOLATED, status='RUNNING', owner='w1'
        )
        await _task(
            session,
            queue='a',
            execution=THREAD,
            status='CLAIMED',
            owner='w1',
            lease_s=60,
        )
        await _task(
            session,
            queue='a',
            execution=THREAD,
            status='CLAIMED',
            owner='w1',
            lease_s=-60,
        )
        await _task(session, queue='a', execution=ASYNC, status='RUNNING', owner='w1')
        await _task(session, queue='a', execution=ASYNC, status='RUNNING', owner='w9')

        match await broker.get_execution_class_report_async():
            case Ok(report):
                pass
            case Err(error):
                raise AssertionError(error.message)

        (worker,) = report.workers
        assert worker == WorkerExecutionClassLoad(
            worker_id='w1',
            snapshot_at=worker.snapshot_at,
            budgets=(
                BudgetLoad(ISOLATED, 4, (ISOLATED, THREAD), in_flight=2),
                BudgetLoad(ASYNC, 40, (ASYNC,), in_flight=1),
            ),
        )
        assert [budget.free for budget in worker.budgets] == [2, 39]

    async def test_rows_of_a_class_the_worker_does_not_serve_fail_the_read(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        await _snapshot(session, 'w1', ASYNC_CHILDREN_ONLY, queues=['a'])
        await _task(
            session, queue='a', execution=ISOLATED, status='RUNNING', owner='w1'
        )

        match await broker.get_execution_class_report_async():
            case Err(error):
                assert error.code == DatabaseErrorCode.MONITORING_QUERY_FAILED
                assert 'w1' in error.message
                assert 'ISOLATED' in error.message
            case Ok(report):
                raise AssertionError(report)
