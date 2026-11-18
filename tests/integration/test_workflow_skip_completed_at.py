"""A node that the progression skips gets ``completed_at``.

Rule of the baseline workflow functions: every write of a node to
COMPLETED, FAILED or SKIPPED sets ``completed_at``. Each test runs one of
the four skip writes of the Python progression:

- the level promotion: a join that can no longer be met
  (``SKIP_PENDING_TASKS_BATCH_SQL``), and a join ``all`` with a failed
  dependency (``SKIP_READY_TASKS_BATCH_SQL``);
- the re-evaluation of one node (``try_make_ready_and_enqueue``): the same
  two cases (``SKIP_WORKFLOW_TASK_SQL``, ``SKIP_READY_WORKFLOW_TASK_SQL``).
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode
from horsies.core.workflows.engine import try_make_ready_and_enqueue

from .conftest import (
    finish_task,
    make_failing_task,
    make_simple_task,
    make_workflow_spec,
    start_ok,
)

pytestmark = [pytest.mark.integration]

_FAILED = TaskResult[int, TaskError](err=TaskError(error_code='FAIL', message='dependency failed'))


async def _node(session: AsyncSession, workflow_id: str, index: int) -> tuple[str, object]:
    row = (
        await session.execute(
            text("""
                SELECT status, completed_at FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': index},
        )
    ).one()
    return row.status, row.completed_at


async def _fail_dependencies_in_place(
    session: AsyncSession, workflow_id: str, indexes: list[int],
) -> None:
    """Write the dependencies FAILED with no progression: the re-evaluation case."""
    await session.execute(
        text("""
            UPDATE horsies_workflow_tasks
            SET status = 'FAILED', completed_at = NOW()
            WHERE workflow_id = :wf_id AND task_index = ANY(:idxs)
        """),
        {'wf_id': workflow_id, 'idxs': indexes},
    )
    await session.commit()


@pytest.mark.asyncio(loop_scope='function')
class TestSkipSetsCompletedAt:
    @pytest_asyncio.fixture
    async def setup(
        self,
        clean_workflow_tables: None,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        return session, broker, app

    async def test_a_join_that_cannot_be_met_skips_the_node_with_completed_at(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup
        failing = make_failing_task(app, 'skip_completed_any_fail')
        task = make_simple_task(app, 'skip_completed_any_task')
        first = TaskNode(fn=failing)
        second = TaskNode(fn=failing)
        joined = TaskNode(fn=task, kwargs={'value': 0}, waits_for=[first, second], join='any')
        handle = await start_ok(
            make_workflow_spec(
                broker=broker, name='skip_completed_any', tasks=[first, second, joined],
            ),
            broker,
        )

        await finish_task(session, broker, str(handle.workflow_id), 0, _FAILED)
        await finish_task(session, broker, str(handle.workflow_id), 1, _FAILED)

        status, completed_at = await _node(session, str(handle.workflow_id), 2)
        assert status == 'SKIPPED'
        assert completed_at is not None

    async def test_a_failed_dependency_of_a_join_all_skips_the_node_with_completed_at(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup
        failing = make_failing_task(app, 'skip_completed_all_fail')
        task = make_simple_task(app, 'skip_completed_all_task')
        first = TaskNode(fn=failing)
        dependent = TaskNode(fn=task, kwargs={'value': 0}, waits_for=[first])
        handle = await start_ok(
            make_workflow_spec(broker=broker, name='skip_completed_all', tasks=[first, dependent]),
            broker,
        )

        await finish_task(session, broker, str(handle.workflow_id), 0, _FAILED)

        status, completed_at = await _node(session, str(handle.workflow_id), 1)
        assert status == 'SKIPPED'
        assert completed_at is not None

    async def test_the_reevaluation_of_a_join_that_cannot_be_met_sets_completed_at(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup
        task = make_simple_task(app, 'skip_completed_reeval_any')
        first = TaskNode(fn=task, kwargs={'value': 1})
        second = TaskNode(fn=task, kwargs={'value': 2})
        joined = TaskNode(fn=task, kwargs={'value': 0}, waits_for=[first, second], join='any')
        handle = await start_ok(
            make_workflow_spec(
                broker=broker, name='skip_completed_reeval_any', tasks=[first, second, joined],
            ),
            broker,
        )
        await _fail_dependencies_in_place(session, str(handle.workflow_id), [0, 1])

        await try_make_ready_and_enqueue(session, broker, str(handle.workflow_id), 2)
        await session.commit()

        status, completed_at = await _node(session, str(handle.workflow_id), 2)
        assert status == 'SKIPPED'
        assert completed_at is not None

    async def test_the_reevaluation_of_a_join_all_with_a_failed_dependency_sets_completed_at(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup
        task = make_simple_task(app, 'skip_completed_reeval_all')
        first = TaskNode(fn=task, kwargs={'value': 1})
        dependent = TaskNode(fn=task, kwargs={'value': 0}, waits_for=[first])
        handle = await start_ok(
            make_workflow_spec(
                broker=broker, name='skip_completed_reeval_all', tasks=[first, dependent],
            ),
            broker,
        )
        await _fail_dependencies_in_place(session, str(handle.workflow_id), [0])

        await try_make_ready_and_enqueue(session, broker, str(handle.workflow_id), 1)
        await session.commit()

        status, completed_at = await _node(session, str(handle.workflow_id), 1)
        assert status == 'SKIPPED'
        assert completed_at is not None
