"""The claim per execution class, against the real horsies_claim.

- Each budget claims the classes that the routes give it, in priority order
  within the merge of those classes, and never a class routed elsewhere.
- This worker's RUNNING and live CLAIMED rows count against the budget of
  their class; the worker-wide limits still bound the whole pass.
- The expired arm follows the same routes.
- A claim locks only the rows it takes, and a row that another claim holds
  is skipped, not waited for.
- A row whose class does not fit its registered task goes back to the queue
  as ISOLATED, fenced to its claim generation.
- The worker state publishes the capacity of each budget.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import json
import uuid
from collections import Counter
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import Execution
from horsies.core.worker.claiming import ClaimMixin, ClaimPass
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.row_class import RowClassMismatch
from horsies.core.worker.slots import SlotPool
from horsies.core.worker.sql import HORSIES_CLAIM_SQL
from horsies.core.worker.worker import Worker
from tests.broker_resources_support import current_resources

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

QUEUE = 'class_q'
LEASE_MS = 60_000
ISOLATED_ROUTES = {
    'ISOLATED': 'ISOLATED',
    'SHARED_ASYNC': 'ISOLATED',
    'SHARED_THREAD': 'ISOLATED',
}
ISOLATED_AND_ASYNC_ROUTES = {
    'ISOLATED': 'ISOLATED',
    'SHARED_ASYNC': 'SHARED_ASYNC',
    'SHARED_THREAD': 'ISOLATED',
}


async def _seed(
    session: AsyncSession,
    *,
    execution: str,
    priority: int = 100,
    age_secs: int = 10,
    task_name: str = 'class_task',
    status: str = 'PENDING',
    owner: str | None = None,
    lease: str | None = None,
) -> str:
    """Insert one row. ``lease`` is 'live' or 'expired' for a CLAIMED row."""
    task_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
              (id, task_name, queue_name, priority, status, enqueued_at,
               claimed, claimed_by_worker_id, is_workflow_task, enqueue_sha,
               created_at, updated_at, retry_count, max_retries,
               retention_class_key, command_fingerprint_version,
               command_fingerprint, retain_rerun_input,
               prepared_rerun_input_disposition, execution)
            VALUES
              (:id, :name, :q, :p, :status,
               now() - (:age * interval '1 second'),
               :claimed, :owner, false, :sha, now(), now(), 0, 0,
               'standard_30d', 1,
               sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
               false, 'DECLINED_BY_POLICY', :execution)
        """),
        {
            'id': task_id,
            'name': task_name,
            'q': QUEUE,
            'p': priority,
            'status': status,
            'age': age_secs,
            'claimed': owner is not None,
            'owner': owner,
            'sha': '0' * 64,
            'execution': execution,
        },
    )
    match lease:
        case None:
            pass
        case 'live':
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET claimed_at = now(),
                        claim_expires_at = now() + interval '1 hour'
                    WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        case 'expired':
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET claimed_at = now() - interval '2 minutes',
                        claim_expires_at = now() - interval '1 minute'
                    WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        case _:
            raise AssertionError(lease)
    return task_id


def _params(
    *,
    worker_id: str,
    budgets: dict[str, int],
    routes: dict[str, str],
    prefetch_buffer: int = 0,
    max_claim_per_worker: int = 0,
    max_claim_batch: int = 0,
) -> dict[str, object]:
    return {
        'p_worker_id': worker_id,
        'p_queues': json.dumps([QUEUE]),
        'p_queue_priority': json.dumps({}),
        'p_queue_max_concurrency': json.dumps({}),
        'p_hard_cap_mode': prefetch_buffer == 0,
        'p_budgets': json.dumps(budgets),
        'p_routes': json.dumps(routes),
        'p_prefetch_buffer': prefetch_buffer,
        'p_max_claim_per_worker': max_claim_per_worker,
        'p_max_claim_batch': max_claim_batch,
        'p_cluster_wide_cap': None,
        'p_lease_ms': LEASE_MS,
        'p_lock_keys': '[]',
    }


async def _claim(
    broker: PostgresBroker,
    params: dict[str, object],
) -> list[tuple[str, str]]:
    """One committed claim; (id, execution) in dispatch order."""
    async with current_resources(broker).session_factory() as session:
        rows = (await session.execute(HORSIES_CLAIM_SQL, params)).all()
        await session.commit()
    return [(str(row.id), str(row.execution)) for row in rows]


def _worker_id() -> str:
    return f'class-{uuid.uuid4().hex[:8]}'


class TestBudgets:
    async def test_an_isolated_budget_claims_the_merge_of_every_class(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Rows of three classes interleave in priority; the isolated budget
        reads them as one order and returns each row's class."""
        first = await _seed(session, execution='SHARED_ASYNC', priority=1)
        second = await _seed(session, execution='ISOLATED', priority=2)
        third = await _seed(session, execution='SHARED_THREAD', priority=3)
        await _seed(session, execution='SHARED_ASYNC', priority=4)
        await _seed(session, execution='ISOLATED', priority=5)
        await session.commit()

        claimed = await _claim(
            broker,
            _params(
                worker_id=_worker_id(),
                budgets={'ISOLATED': 3},
                routes=ISOLATED_ROUTES,
            ),
        )

        assert claimed == [
            (first, 'SHARED_ASYNC'),
            (second, 'ISOLATED'),
            (third, 'SHARED_THREAD'),
        ]

    async def test_priority_holds_within_each_budget_not_across(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Async rows of better priority do not take isolated capacity: each
        budget takes its own head, and the isolated children never run a
        class that has children of its own."""
        async_rows = [
            await _seed(session, execution='SHARED_ASYNC', priority=1) for _ in range(3)
        ]
        isolated_rows = [
            await _seed(session, execution='ISOLATED', priority=50) for _ in range(2)
        ]
        thread_row = await _seed(session, execution='SHARED_THREAD', priority=60)
        await session.commit()

        claimed = await _claim(
            broker,
            _params(
                worker_id=_worker_id(),
                budgets={'ISOLATED': 3, 'SHARED_ASYNC': 1},
                routes=ISOLATED_AND_ASYNC_ROUTES,
            ),
        )

        by_class = Counter(execution for _, execution in claimed)
        assert by_class == {'SHARED_ASYNC': 1, 'ISOLATED': 2, 'SHARED_THREAD': 1}
        claimed_ids = {task_id for task_id, _ in claimed}
        assert set(isolated_rows) | {thread_row} <= claimed_ids
        assert len(claimed_ids & set(async_rows)) == 1

    async def test_a_budget_never_takes_more_than_its_capacity_over_classes(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """The isolated budget of 2 claims ISOLATED and SHARED_THREAD rows;
        each stream alone could fill it, and the worker total is 4."""
        for _ in range(3):
            await _seed(session, execution='ISOLATED')
            await _seed(session, execution='SHARED_THREAD')
        await session.commit()

        claimed = await _claim(
            broker,
            _params(
                worker_id=_worker_id(),
                budgets={'ISOLATED': 2, 'SHARED_ASYNC': 2},
                routes=ISOLATED_AND_ASYNC_ROUTES,
            ),
        )

        assert len(claimed) == 2

    async def test_a_worker_without_isolated_children_claims_no_isolated_rows(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        for _ in range(3):
            await _seed(session, execution='ISOLATED', priority=1)
        await _seed(session, execution='SHARED_THREAD', priority=1)
        async_row = await _seed(session, execution='SHARED_ASYNC', priority=90)
        await session.commit()

        claimed = await _claim(
            broker,
            _params(
                worker_id=_worker_id(),
                budgets={'SHARED_ASYNC': 5},
                routes={'SHARED_ASYNC': 'SHARED_ASYNC'},
            ),
        )

        assert claimed == [(async_row, 'SHARED_ASYNC')]

    async def test_this_workers_rows_count_against_their_budget(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """One RUNNING async row and one live CLAIMED isolated row of this
        worker leave one seat in each budget of 2. Rows of another worker do
        not count."""
        worker_id = _worker_id()
        await _seed(
            session,
            execution='SHARED_ASYNC',
            status='RUNNING',
            owner=worker_id,
        )
        await _seed(
            session,
            execution='ISOLATED',
            status='CLAIMED',
            owner=worker_id,
            lease='live',
        )
        await _seed(
            session,
            execution='ISOLATED',
            status='RUNNING',
            owner='someone-else',
        )
        for _ in range(3):
            await _seed(session, execution='SHARED_ASYNC')
            await _seed(session, execution='ISOLATED')
        await session.commit()

        claimed = await _claim(
            broker,
            _params(
                worker_id=worker_id,
                budgets={'ISOLATED': 2, 'SHARED_ASYNC': 2},
                routes=ISOLATED_AND_ASYNC_ROUTES,
            ),
        )

        assert Counter(execution for _, execution in claimed) == {
            'ISOLATED': 1,
            'SHARED_ASYNC': 1,
        }

    async def test_claimed_per_worker_bounds_the_worker_across_budgets(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        for _ in range(3):
            await _seed(session, execution='SHARED_ASYNC')
            await _seed(session, execution='ISOLATED')
        await session.commit()

        claimed = await _claim(
            broker,
            _params(
                worker_id=_worker_id(),
                budgets={'ISOLATED': 3, 'SHARED_ASYNC': 3},
                routes=ISOLATED_AND_ASYNC_ROUTES,
                max_claim_per_worker=2,
            ),
        )

        assert len(claimed) == 2

    async def test_the_batch_limit_bounds_each_queue_across_budgets(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        for _ in range(3):
            await _seed(session, execution='SHARED_ASYNC')
            await _seed(session, execution='ISOLATED')
        await session.commit()

        claimed = await _claim(
            broker,
            _params(
                worker_id=_worker_id(),
                budgets={'ISOLATED': 3, 'SHARED_ASYNC': 3},
                routes=ISOLATED_AND_ASYNC_ROUTES,
                max_claim_batch=1,
            ),
        )

        assert len(claimed) == 1

    async def test_soft_cap_lets_each_budget_prefetch_within_the_worker_total(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Capacity 1 + 1 and prefetch 1: each budget may hold 2, the worker
        3. Isolated rows rank first, so the third row is the async one."""
        for _ in range(4):
            await _seed(session, execution='ISOLATED', priority=1)
            await _seed(session, execution='SHARED_ASYNC', priority=2)
        await session.commit()

        claimed = await _claim(
            broker,
            _params(
                worker_id=_worker_id(),
                budgets={'ISOLATED': 1, 'SHARED_ASYNC': 1},
                routes=ISOLATED_AND_ASYNC_ROUTES,
                prefetch_buffer=1,
            ),
        )

        assert Counter(execution for _, execution in claimed) == {
            'ISOLATED': 2,
            'SHARED_ASYNC': 1,
        }

    async def test_a_route_to_a_budget_that_is_not_passed_is_refused(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
    ) -> None:
        with pytest.raises(DBAPIError) as raised:
            await _claim(
                broker,
                _params(
                    worker_id=_worker_id(),
                    budgets={'ISOLATED': 2},
                    routes={'SHARED_ASYNC': 'SHARED_ASYNC'},
                ),
            )
        diagnostic = getattr(raised.value.orig, 'diag')
        assert diagnostic.sqlstate == '22023'
        assert diagnostic.message_primary == (
            'horsies_claim: p_routes names a budget class that p_budgets '
            'does not hold'
        )


class TestExpiredArm:
    async def test_an_expired_claim_follows_the_routes(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        lapsed = await _seed(
            session,
            execution='SHARED_ASYNC',
            status='CLAIMED',
            owner='lapsed-worker',
            lease='expired',
        )
        await session.commit()

        not_served = await _claim(
            broker,
            _params(
                worker_id=_worker_id(),
                budgets={'ISOLATED': 2},
                routes={'ISOLATED': 'ISOLATED'},
            ),
        )
        served = await _claim(
            broker,
            _params(
                worker_id=_worker_id(),
                budgets={'ISOLATED': 2, 'SHARED_ASYNC': 2},
                routes=ISOLATED_AND_ASYNC_ROUTES,
            ),
        )

        assert not_served == []
        assert served == [(lapsed, 'SHARED_ASYNC')]

    async def test_an_unserved_expired_row_takes_no_budget(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """An expired async row ranks first, and this worker serves only
        ISOLATED: the one seat goes to the pending isolated row."""
        await _seed(
            session,
            execution='SHARED_ASYNC',
            priority=1,
            status='CLAIMED',
            owner='lapsed-worker',
            lease='expired',
        )
        pending = await _seed(session, execution='ISOLATED', priority=5)
        await session.commit()

        claimed = await _claim(
            broker,
            _params(
                worker_id=_worker_id(),
                budgets={'ISOLATED': 1},
                routes={'ISOLATED': 'ISOLATED'},
            ),
        )

        assert claimed == [(pending, 'ISOLATED')]

    async def test_the_expired_arm_takes_only_a_served_class(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Two expired rows; the async one ranks first and is not served."""
        await _seed(
            session,
            execution='SHARED_ASYNC',
            priority=1,
            status='CLAIMED',
            owner='lapsed-worker',
            lease='expired',
        )
        isolated = await _seed(
            session,
            execution='ISOLATED',
            priority=5,
            status='CLAIMED',
            owner='lapsed-worker',
            lease='expired',
        )
        await session.commit()

        claimed = await _claim(
            broker,
            _params(
                worker_id=_worker_id(),
                budgets={'ISOLATED': 1},
                routes={'ISOLATED': 'ISOLATED'},
            ),
        )

        assert claimed == [(isolated, 'ISOLATED')]


class TestLocks:
    async def test_a_claim_locks_only_the_rows_it_takes(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """30 rows of two classes in one isolated budget of 5: while the
        claim is open, another worker can still lock the other 25."""
        for _ in range(15):
            await _seed(session, execution='ISOLATED')
            await _seed(session, execution='SHARED_ASYNC')
        await session.commit()
        params = _params(
            worker_id=_worker_id(),
            budgets={'ISOLATED': 5},
            routes=ISOLATED_ROUTES,
        )

        engine = current_resources(broker).engine
        async with engine.connect() as claimer:
            transaction = await claimer.begin()
            try:
                rows = (await claimer.execute(HORSIES_CLAIM_SQL, params)).all()
                async with engine.connect() as other_worker:
                    async with other_worker.begin():
                        unlocked = (
                            await other_worker.execute(
                                text("""
                                    SELECT count(*) FROM (
                                        SELECT id FROM horsies_tasks
                                        WHERE queue_name = :q
                                          AND status = 'PENDING'
                                        FOR UPDATE SKIP LOCKED
                                    ) free
                                """),
                                {'q': QUEUE},
                            )
                        ).scalar_one()
            finally:
                await transaction.rollback()

        assert len(rows) == 5
        assert unlocked == 25

    async def test_a_row_held_by_another_claim_is_skipped(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """The head of the stream is locked elsewhere: the claim takes the
        next row of the same class instead of none."""
        head = await _seed(session, execution='ISOLATED', priority=1)
        second = await _seed(session, execution='ISOLATED', priority=2)
        await session.commit()

        engine = current_resources(broker).engine
        async with engine.connect() as holder:
            transaction = await holder.begin()
            try:
                await holder.execute(
                    text(
                        'SELECT id FROM horsies_tasks '
                        'WHERE id = CAST(:id AS uuid) FOR UPDATE'
                    ),
                    {'id': head},
                )
                claimed = await _claim(
                    broker,
                    _params(
                        worker_id=_worker_id(),
                        budgets={'ISOLATED': 1},
                        routes=ISOLATED_ROUTES,
                    ),
                )
            finally:
                await transaction.rollback()

        assert claimed == [(second, 'ISOLATED')]


class _Probe(ClaimMixin):
    """A claim pass with no slot pool; records each dispatched task id."""

    def __init__(
        self,
        broker: PostgresBroker,
        registered: dict[str, Execution],
    ) -> None:
        self.sf = current_resources(broker).session_factory
        self.cfg = WorkerConfig(
            dsn='unused',
            psycopg_dsn='unused',
            queues=[QUEUE],
            processes=4,
        )
        self.worker_instance_id = _worker_id()
        self._registered_classes = registered
        self.dispatched: list[str] = []

    def _claim_slot_count(self) -> int:
        return self.cfg.processes

    async def _dispatch_one(self, task_id: str, *args: Any, **kwargs: Any) -> None:
        self.dispatched.append(task_id)


async def _row_state(session: AsyncSession, task_id: str) -> tuple[Any, ...]:
    row = (
        await session.execute(
            text("""
                SELECT status, execution, claimed_by_worker_id, claimed_at
                FROM horsies_tasks WHERE id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).one()
    await session.commit()
    return tuple(row)


class TestClassMismatch:
    async def test_a_mismatched_row_returns_as_isolated_and_runs_next_pass(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """The row says SHARED_ASYNC, the function here is ISOLATED (a def
        after a deploy): the pass writes ISOLATED and returns the row; the
        next pass claims and dispatches it."""
        task_id = await _seed(
            session,
            execution='SHARED_ASYNC',
            task_name='now_def',
        )
        await session.commit()
        probe = _Probe(broker, {'now_def': Execution.ISOLATED})

        first = await probe._claim_and_dispatch_all()

        assert first == ClaimPass(claimed=1, dropped=1)
        assert first.left_unannounced_work(limited=False)
        assert probe.dispatched == []
        assert await _row_state(session, task_id) == (
            'PENDING',
            'ISOLATED',
            None,
            None,
        )

        second = await probe._claim_and_dispatch_all()

        assert second == ClaimPass(claimed=1, dropped=0)
        assert probe.dispatched == [task_id]

    async def test_a_return_with_a_stale_claim_generation_changes_nothing(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        probe = _Probe(broker, {'now_def': Execution.ISOLATED})
        task_id = await _seed(
            session,
            execution='SHARED_ASYNC',
            task_name='now_def',
            status='CLAIMED',
            owner=probe.worker_instance_id,
            lease='live',
        )
        await session.commit()
        before = await _row_state(session, task_id)

        await probe._return_row_as_isolated(
            {
                'id': task_id,
                'task_name': 'now_def',
                'queue_name': QUEUE,
                'claimed_at': before[3].replace(year=2000),
            },
            RowClassMismatch(
                row_class=Execution.SHARED_ASYNC,
                registered_class=Execution.ISOLATED,
            ),
        )

        assert await _row_state(session, task_id) == before
        assert before[:2] == ('CLAIMED', 'SHARED_ASYNC')


class TestWorkerStateCapacity:
    async def test_the_snapshot_stores_each_budget_and_what_it_serves(
        self,
        clean_workflow_tables: None,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        cfg = WorkerConfig(
            dsn='unused',
            psycopg_dsn='unused',
            queues=[QUEUE],
            processes=3,
        )
        worker = Worker(
            session_factory=current_resources(broker).session_factory,
            listener=MagicMock(),
            cfg=cfg,
        )
        worker._slots = SlotPool(worker._slot_pool_config(), pending_limit=1)

        await worker._update_worker_state()

        stored = (
            await session.execute(
                text(
                    'SELECT class_capacity FROM horsies_worker_states '
                    'WHERE worker_id = :wid'
                ),
                {'wid': worker.worker_instance_id},
            )
        ).scalar_one()
        await session.commit()
        assert stored == {
            'ISOLATED': {
                'capacity': 3,
                'serves': ['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD'],
            },
        }
