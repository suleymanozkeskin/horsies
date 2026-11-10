"""The fleet check and the class report against a real Postgres.

- A pending (queue, class) is served, served without capacity, or unserved,
  from the live snapshots' queues and budgets.
- A snapshot older than the live window does not serve anything.
- An isolated retry of a shared task is unserved when no live worker has
  isolated children.
- A live snapshot without budgets, or with unreadable ones, fails the read
  and names the worker.
- The report gives the wait of each stream's oldest due row and the load of
  each budget of each live worker, counted as the claim counts it.
- The reaper pass logs an unserved stream: the test enters at
  ``_run_reaper_pass``, above the wiring.
- Both reads use the claim index.

Each test empties ``horsies_tasks`` and ``horsies_worker_states``: both reads
cover the whole fleet.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import json
import logging
import uuid
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.class_coverage_reads import (
    _PENDING_HEADS_SQL,
    _PENDING_STREAMS_SQL,
)
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode
from horsies.core.models.class_coverage import (
    BudgetLoad,
    NoneDue,
    OldestWaiting,
    StreamCoverage,
    StreamServed,
    StreamUnserved,
    StreamWithoutCapacity,
    WorkerClassLoad,
)
from horsies.core.models.health import WORKER_STATE_LIVE_WINDOW_S
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.retention import RetentionConfig
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok
from horsies.core.worker.runtime import _ReaperPassState
from horsies.core.worker.worker import Worker, WorkerConfig
from tests.broker_resources_support import current_resources

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
                tasks_running, tasks_claimed, worker_started_at,
                slots_down, class_capacity
            ) VALUES (
                :worker_id, now() - make_interval(secs => CAST(:age AS integer)),
                'itest-host',
                4242, 4, 0, 4, :queues, 0, 0, now() - interval '1 hour',
                '[]'::jsonb, CAST(:class_capacity AS JSONB)
            )
        """),
        {
            'worker_id': worker_id,
            'age': age_s,
            'queues': queues,
            'class_capacity': None
            if class_capacity is None
            else json.dumps(class_capacity),
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
              (id, task_name, queue_name, priority, status, enqueued_at,
               claimed, claimed_by_worker_id, claimed_at, claim_expires_at,
               started_at, is_workflow_task, enqueue_sha, created_at,
               updated_at, retry_count, max_retries, retention_class_key,
               command_fingerprint_version, command_fingerprint,
               retain_rerun_input, prepared_rerun_input_disposition,
               execution)
            VALUES
              (:id, 'class_task', :queue, :priority, CAST(:status AS varchar),
               now() + make_interval(secs => CAST(:due_in AS integer)),
               :claimed, CAST(:owner AS varchar),
               CASE WHEN CAST(:owner AS varchar) IS NULL THEN NULL ELSE now() END,
               CASE WHEN CAST(:lease AS integer) IS NULL THEN NULL
                    ELSE now() + make_interval(secs => CAST(:lease AS integer)) END,
               CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN now() ELSE NULL END,
               false, :sha, now(), now(), 0, 0, 'standard_30d', 1,
               sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
               false, 'DECLINED_BY_POLICY', :execution)
        """),
        {
            'id': task_id,
            'queue': queue,
            'priority': priority,
            'status': status,
            'due_in': due_in_s,
            'claimed': owner is not None,
            'owner': owner,
            'lease': lease_s,
            'sha': '0' * 64,
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

        match await broker.get_class_coverage_async():
            case Ok(coverage):
                pass
            case Err(error):
                raise AssertionError(error.message)

        assert coverage.live_workers == 2
        assert coverage.streams == (
            StreamCoverage('a', ASYNC, StreamServed(workers=1, capacity=4)),
            StreamCoverage('b', ISOLATED, StreamWithoutCapacity(workers=1)),
            StreamCoverage('c', THREAD, StreamUnserved()),
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
            age_s=WORKER_STATE_LIVE_WINDOW_S + 5,
        )
        await _task(session, queue='a', execution=ISOLATED)

        match await broker.get_class_coverage_async():
            case Ok(coverage):
                pass
            case Err(error):
                raise AssertionError(error.message)

        assert coverage.live_workers == 0
        assert coverage.streams == (StreamCoverage('a', ISOLATED, StreamUnserved()),)

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

        match await broker.get_class_coverage_async():
            case Ok(coverage):
                pass
            case Err(error):
                raise AssertionError(error.message)

        assert coverage.live_workers == 1
        assert coverage.streams[0].state == StreamWithoutCapacity(workers=1)

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

        match await broker.get_class_coverage_async():
            case Ok(coverage):
                pass
            case Err(error):
                raise AssertionError(error.message)

        assert coverage.streams == (
            StreamCoverage('a', ISOLATED, StreamUnserved()),
            StreamCoverage('a', ASYNC, StreamServed(workers=1, capacity=40)),
        )

    async def test_a_delayed_row_counts_as_pending(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """A row due later still needs a worker of its class."""
        await _task(session, queue='a', execution=ISOLATED, due_in_s=3600)

        match await broker.get_class_coverage_async():
            case Ok(coverage):
                pass
            case Err(error):
                raise AssertionError(error.message)

        assert coverage.streams == (StreamCoverage('a', ISOLATED, StreamUnserved()),)

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

        match await broker.get_class_coverage_async():
            case Ok(coverage):
                assert coverage.streams == ()
            case Err(error):
                raise AssertionError(error.message)

    @pytest.mark.parametrize(
        ('class_capacity', 'cause'),
        [
            (None, 'has no class_capacity'),
            (
                {'EXCLUSIVE': {'capacity': 1, 'serves': []}},
                'class_capacity is unreadable',
            ),
        ],
        ids=['not-recorded', 'unreadable'],
    )
    async def test_a_live_snapshot_without_readable_budgets_fails_the_read(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        class_capacity: object,
        cause: str,
    ) -> None:
        await _snapshot(session, 'w-odd', class_capacity, queues=['a'])
        await _task(session, queue='a', execution=ISOLATED)

        match await broker.get_class_coverage_async():
            case Err(error):
                assert error.code == BrokerErrorCode.MONITORING_QUERY_FAILED
                assert error.retryable is False
                assert 'w-odd' in error.message
                assert cause in error.message
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

        match await broker.get_class_report_async():
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
        assert (isolated.queue_name, isolated.execution) == ('a', ISOLATED)
        assert isolated.wait == OldestWaiting(oldest, report.checked_at - oldest)
        assert report.checked_at - oldest >= timedelta(seconds=300)
        assert isolated.state == StreamServed(workers=1, capacity=4)
        assert shared.wait == NoneDue(next_due)

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

        match await broker.get_class_report_async():
            case Ok(report):
                pass
            case Err(error):
                raise AssertionError(error.message)

        (worker,) = report.workers
        assert worker == WorkerClassLoad(
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

        match await broker.get_class_report_async():
            case Err(error):
                assert error.code == BrokerErrorCode.MONITORING_QUERY_FAILED
                assert 'w1' in error.message
                assert 'ISOLATED' in error.message
            case Ok(report):
                raise AssertionError(report)


def _reaper_worker(broker: PostgresBroker) -> Worker:
    """A Worker constructed only as far as ``_run_reaper_pass`` requires."""
    config = WorkerConfig(
        dsn='postgresql+psycopg://unused:unused@localhost/unused',
        psycopg_dsn='postgresql://unused:unused@localhost/unused',
        queues=['a'],
    )
    return Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=config,
        broker=broker,
    )


@pytest.mark.usefixtures('empty_fleet')
class TestReaperPassLogsUnservedStreams:
    """Enter at the pass, assert the log line: the wiring, not the mechanism."""

    async def test_the_pass_logs_an_unserved_isolated_retry(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        await _snapshot(session, 'w-async', ASYNC_CHILDREN_ONLY, queues=['a'])
        await _task(session, queue='a', execution=ISOLATED)
        state = _ReaperPassState(
            next_retention_cleanup_at=float('inf'),
            next_partition_maintenance_at=float('inf'),
            next_orphan_task_audit_at=float('inf'),
        )

        with caplog.at_level(logging.ERROR, logger='horsies.worker'):
            await _reaper_worker(broker)._run_reaper_pass(
                broker,
                current_resources(broker),
                RecoveryConfig(),
                RetentionConfig(),
                state,
            )

        lines = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert any(
            "PENDING ISOLATED tasks on queue 'a' wait: no live worker serves this class"
            in line
            for line in lines
        ), lines


async def _plan(session: AsyncSession, statement: Any, params: dict[str, Any]) -> str:
    rows = (
        await session.execute(
            text(f'EXPLAIN (FORMAT JSON) {statement.text}'),
            params,
        )
    ).scalar_one()
    return json.dumps(rows)


@pytest.mark.usefixtures('empty_fleet')
class TestClassReadsUseTheClaimIndex:
    """Both skip scans probe idx_horsies_tasks_claim_pending_class.

    20,000 PENDING rows over 4 queues, 3 classes and 100 priorities, so the
    planner picks on its own, without ``enable_seqscan = off``.
    """

    async def test_the_stream_and_head_scans_probe_the_pending_class_index(
        self,
        session: AsyncSession,
    ) -> None:
        await _task(session, queue='a', execution=ISOLATED)
        await session.execute(
            text("""
                INSERT INTO horsies_tasks
                  (id, task_name, queue_name, priority, status, enqueued_at,
                   claimed, is_workflow_task, enqueue_sha, created_at,
                   updated_at, retry_count, max_retries, retention_class_key,
                   command_fingerprint_version, command_fingerprint,
                   retain_rerun_input, prepared_rerun_input_disposition,
                   execution)
                SELECT gen_random_uuid(), 'class_task', 'q' || (g % 4),
                       1 + (g % 100), 'PENDING', now() - g * interval '1 ms',
                       false, false, repeat('0', 64), now(), now(), 0, 0,
                       'standard_30d', 1,
                       sha256(convert_to(CAST(g AS text), 'UTF8')), false,
                       'DECLINED_BY_POLICY',
                       (ARRAY['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD'])[1 + g % 3]
                FROM generate_series(1, 20000) AS g
            """),
        )
        await session.commit()
        await session.execute(text('ANALYZE horsies_tasks'))
        await session.commit()

        streams = await _plan(session, _PENDING_STREAMS_SQL, {'stream_limit': 10})
        heads = await _plan(session, _PENDING_HEADS_SQL, {'head_limit': 10})

        for plan in (streams, heads):
            assert 'idx_horsies_tasks_claim_pending_class' in plan
            assert '"Seq Scan"' not in plan
