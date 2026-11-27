"""Integration tests for workflow recovery logic."""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.codec.typed import decode_task_result, encode_task_result
from horsies.core.models.outcome import Failed, Succeeded
from horsies.core.models.tasks import TaskError, OperationalCode
from horsies.core.models.workflow import (
    TaskNode,
)
from horsies.core.types.result import Err, Result

from .conftest import (
    make_simple_task,
    make_workflow_spec,
    start_ok,
)
from tests.integration.history_seeding import force_terminal
from tests.workflow_recovery_support import phase2_recovery_pass


async def _run_phase2_recovery(
    session: AsyncSession,
    broker: PostgresBroker,
    *,
    grace_ms: int = 0,
) -> int:
    """Drive the reaper's phase-2 recovery pass over seeded evidence.

    The crashed-worker case is the outbox consumer's, not the scan's:
    terminalization records the owed progression as it moves the task,
    and this is what the reaper does with those records. The driver owns
    a transaction per record, so the seeded state is committed first and
    the caller's next read starts a fresh snapshot.

    Returns the count of nodes advanced.
    """
    await session.commit()
    return (await phase2_recovery_pass(broker, grace_ms=grace_ms)).applied


def _strict_result_json(result: Result[Any, TaskError]) -> str:
    """Build the strict-serde wire envelope for ``horsies_workflow_tasks.result``.

    Tests in this file seed terminal task rows directly (simulating crash /
    recovery states); they must match what production writes so the engine's
    strict envelope decode of the node finish / recovery finalization
    accepts the payload. ``ok_type=Any`` is sufficient — these
    tests substring-check the serialized output and don't pin the ok-payload
    type-narrowing on read.
    """
    envelope = encode_task_result(result, Any)
    serialized = dumps_json(envelope)
    return serialized.unwrap()


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestWorkflowRecovery:
    """Tests for the global workflow recovery pass."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        """Clean tables and return fixtures."""
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app


    # RETIRED: three cases recovered a terminal task with NO stored
    # result. On any path that owes phase-2 progression the move refuses
    # that state at the source — 'deferred workflow terminalization
    # requires a result payload' — because the outbox carries the
    # result's digest as a NOT NULL column. The four deferring families
    # all produce a payload; the four that do not defer advance their
    # node inline or have no node. The invariant those cases were
    # unknowingly probing is now pinned directly, in
    # test_task_history_terminalization_move.py.


    async def test_recover_crashed_worker_unregistered_task_err_fast_path(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Case 1.7: err-only result for an unregistered task must surface
        via the recovery err-fast-path.

        Pre-fix ``_decode_recovered_task_result`` required the source task's
        ``task_ok_type`` for *any* decode path. When recovery ran in a
        process that hadn't imported the task (cross-process recovery), the
        real failure was discarded in favor of a synthetic
        ``WORKER_CRASHED``. The err-fast-path now uses
        ``decode_task_error`` (no ok_type) so the real err survives.
        """
        session, broker, app = setup
        task_a = make_simple_task(app, 'recover_unregistered_err_a')

        node_a = TaskNode(fn=task_a, kwargs={'value': 5})
        spec = make_workflow_spec(
            broker=broker, name='recover_unregistered_err', tasks=[node_a]
        )

        handle = await start_ok(spec, broker)

        wt_result = await session.execute(
            text("""
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        task_id = wt_result.fetchone()[0]

        # Store a real TASK_EXCEPTION (err-only) result, then simulate
        # crash via workflow_tasks=RUNNING.
        real_err_result: Result[Any, TaskError] = Err(
            TaskError(
                error_code=OperationalCode.TASK_EXCEPTION,
                message='real failure to preserve through recovery',
            ),
        )
        await force_terminal(
            session,
            task_id,
            status='FAILED',
            result_json=_strict_result_json(real_err_result),
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        # Simulate cross-process recovery: the task isn't registered in
        # the recovery worker's app.tasks. Pre-fix this discarded the
        # real err and produced a synthetic WORKER_CRASHED.
        del app.tasks['recover_unregistered_err_a']
        assert app.tasks.get('recover_unregistered_err_a') is None

        recovered = await _run_phase2_recovery(session, broker)
        await session.commit()

        assert recovered == 1

        wt_check = await session.execute(
            text("""
                SELECT status, result FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        status, result_json = wt_check.fetchone()
        assert status == 'FAILED'

        recovered_result = decode_task_result(
            loads_json(result_json).unwrap(), Any,
        )
        match recovered_result:
            case Failed(error=recovered_error):
                # Critical: the real err survives, not the synthetic WORKER_CRASHED.
                assert recovered_error.error_code == (
                    OperationalCode.TASK_EXCEPTION
                )
                assert recovered_error.message == (
                    'real failure to preserve through recovery'
                )
            case Succeeded():
                pytest.fail(f'expected the stored error, got {recovered_result!r}')
