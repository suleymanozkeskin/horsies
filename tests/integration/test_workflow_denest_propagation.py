"""Integration tests for the de-nested parent-propagation driver (S2).

Pins the implementation invariants from the workflow-completion redesign:
- the driver advances a parent from an ALREADY-TERMINAL child — the state a
  phase2 finalize retry cannot reach (the phase-2 record is gone);
- a transient failure folds into the parent_propagation retry stage and the
  bounded retry re-enters the chain and completes it;
- a multi-level ancestor chain is driven iteratively, one fresh transaction
  per level.

The driver runs ``horsies_workflow_child_finish`` per level
(``steps.advance_parents``), with the child starts that a level owes, which
it builds through the worker's broker.

The crash window between levels (child terminal, parent node not updated)
is the recovery case 1.6 state, already pinned by
test_workflow_recovery.py::test_recover_child_completed_parent_not_updated.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.typed import decode_task_result
from horsies.core.types.result import is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker, _FINALIZE_STAGE_PARENT

pytestmark = [pytest.mark.integration]

_CHILD_RESULT_ENVELOPE = '{"__h_task_result__": true, "ok": 7, "err": null}'
_OUTPUTLESS_CHILD_RESULT_ENVELOPE = (
    '{"__h_task_result__":true,"__h_outputless_terminals__":true,'
    '"ok":{"results_by_id":{"terminal":{"__h_task_result__":true,'
    '"ok":7,"err":null}},"task_name_by_id":{"terminal":"leaf_task"},'
    '"definition_key_by_id":{"terminal":null}},"err":null}'
)


def _make_worker(engine: AsyncEngine, broker: PostgresBroker) -> Worker:
    sf = async_sessionmaker(engine, expire_on_commit=False)
    cfg = WorkerConfig(
        dsn='postgresql://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    return Worker(session_factory=sf, listener=MagicMock(), cfg=cfg, broker=broker)


async def _insert_workflow(
    session: AsyncSession,
    *,
    status: str,
    parent_workflow_id: str | None = None,
    parent_task_index: int | None = None,
    result: str | None = None,
) -> str:
    wf_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 parent_workflow_id, parent_task_index, result,
                 sent_at, created_at, started_at, updated_at, completed_at)
            VALUES (:id, 'denest_wf', :status, 'fail', :depth, :id,
                    :pwf, :pidx, :result,
                    NOW(), NOW(), NOW(), NOW(),
                    CASE WHEN :is_completed THEN NOW() END)
        """),
        {
            'id': wf_id,
            'status': status,
            'depth': 0 if parent_workflow_id is None else 1,
            'pwf': parent_workflow_id,
            'pidx': parent_task_index,
            'result': result,
            'is_completed': status == 'COMPLETED',
        },
    )
    return wf_id


async def _insert_subworkflow_node(
    session: AsyncSession,
    *,
    workflow_id: str,
    task_index: int,
    sub_workflow_id: str | None,
) -> None:
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (downstream_subworkflow, id, workflow_id, task_index, node_id, task_name,
                 input, queue_name, priority,
                 dependencies, allow_failed_deps, join_type,
                 is_subworkflow, sub_workflow_id, sub_workflow_params,
                 status, created_at)
            VALUES ('NONE', :id, :wf, :idx, :node, 'denest_sub', '{}',
                    'default', 100, '{}', FALSE, 'all',
                    TRUE, :sub_id, 'NOT_GIVEN', 'RUNNING', NOW())
        """),
        {
            'id': str(uuid.uuid4()),
            'wf': workflow_id,
            'idx': task_index,
            'node': f'sub{task_index}',
            'sub_id': sub_workflow_id,
        },
    )


async def _statuses(
    session: AsyncSession, workflow_id: str
) -> tuple[str, str]:
    """(parent node status at idx 0, workflow status)."""
    node = (
        await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf AND task_index = 0
            """),
            {'wf': workflow_id},
        )
    ).scalar()
    wf = (
        await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf'),
            {'wf': workflow_id},
        )
    ).scalar()
    return str(node), str(wf)


def _load_json(raw: str | None) -> Any:
    assert raw is not None
    loaded = loads_json(raw)
    assert is_ok(loaded), f'JSON decode failed: {loaded}'
    return loaded.ok_value


async def _cleanup(session: AsyncSession, workflow_ids: list[str]) -> None:
    for wf_id in workflow_ids:
        await session.execute(
            text('DELETE FROM horsies_workflow_tasks WHERE workflow_id = :wf'),
            {'wf': wf_id},
        )
        await session.execute(
            text('DELETE FROM horsies_workflows WHERE id = :wf'), {'wf': wf_id},
        )
    await session.commit()


@pytest.mark.asyncio
async def test_driver_advances_parent_from_already_terminal_child(
    engine: AsyncEngine, broker: PostgresBroker, session: AsyncSession,
) -> None:
    """The past-CAS invariant: the child is already COMPLETED (a phase2
    retry would CAS-miss and never reach propagation); the driver alone
    must advance and finalize the parent."""
    parent_id = await _insert_workflow(session, status='RUNNING')
    child_id = await _insert_workflow(
        session,
        status='COMPLETED',
        parent_workflow_id=parent_id,
        parent_task_index=0,
        result=_CHILD_RESULT_ENVELOPE,
    )
    await _insert_subworkflow_node(
        session, workflow_id=parent_id, task_index=0, sub_workflow_id=child_id,
    )
    await session.commit()

    worker = _make_worker(engine, broker)
    try:
        await worker._drive_parent_propagations([child_id])

        node_status, wf_status = await _statuses(session, parent_id)
        assert node_status == 'COMPLETED'
        assert wf_status == 'COMPLETED'
        assert worker._finalize_retry_attempts == {}
    finally:
        await _cleanup(session, [child_id, parent_id])


@pytest.mark.asyncio
async def test_driver_propagates_expired_child_as_parent_failure(
    engine: AsyncEngine, broker: PostgresBroker, session: AsyncSession,
) -> None:
    parent_id = await _insert_workflow(session, status='RUNNING')
    child_id = await _insert_workflow(
        session,
        status='EXPIRED',
        parent_workflow_id=parent_id,
        parent_task_index=0,
    )
    await _insert_subworkflow_node(
        session,
        workflow_id=parent_id,
        task_index=0,
        sub_workflow_id=child_id,
    )
    await session.commit()

    worker = _make_worker(engine, broker)
    try:
        await worker._drive_parent_propagations([child_id])

        assert await _statuses(session, parent_id) == ('FAILED', 'FAILED')
        summary = (
            await session.execute(
                text(
                    'SELECT sub_workflow_summary '
                    'FROM horsies_workflow_tasks '
                    'WHERE workflow_id = :wf AND task_index = 0'
                ),
                {'wf': parent_id},
            )
        ).scalar_one()
        assert _load_json(summary)['status'] == 'EXPIRED'
    finally:
        await _cleanup(session, [child_id, parent_id])


@pytest.mark.asyncio
async def test_outputless_child_propagates_as_none_output(
    engine: AsyncEngine, broker: PostgresBroker, session: AsyncSession,
) -> None:
    """A completed outputless child must not expose nested TaskResult
    envelopes as the parent SubWorkflowNode output or summary output.

    The child's outputless final result is a workflow-level handle payload.
    Parent propagation stores a one-level TaskResult(ok=None) for the
    SubWorkflowNode instead, so later strict-serde paths never see nested
    ``__h_task_result__`` markers as user-originated data.
    """
    parent_id = await _insert_workflow(session, status='RUNNING')
    child_id = await _insert_workflow(
        session,
        status='COMPLETED',
        parent_workflow_id=parent_id,
        parent_task_index=0,
        result=_OUTPUTLESS_CHILD_RESULT_ENVELOPE,
    )
    await _insert_subworkflow_node(
        session, workflow_id=parent_id, task_index=0, sub_workflow_id=child_id,
    )
    await session.commit()

    worker = _make_worker(engine, broker)
    try:
        await worker._drive_parent_propagations([child_id])

        node_status, wf_status = await _statuses(session, parent_id)
        assert (node_status, wf_status) == ('COMPLETED', 'COMPLETED')

        row = (
            await session.execute(
                text("""
                    SELECT result, sub_workflow_summary
                    FROM horsies_workflow_tasks
                    WHERE workflow_id = :wf AND task_index = 0
                """),
                {'wf': parent_id},
            )
        ).one()
        stored_result = decode_task_result(_load_json(row.result), Any)
        assert stored_result.is_ok()
        assert stored_result.unwrap() is None

        summary = _load_json(row.sub_workflow_summary)
        assert isinstance(summary, dict)
        assert summary['output'] is None
    finally:
        await _cleanup(session, [child_id, parent_id])


@pytest.mark.asyncio
async def test_recovery_isolates_poison_child_and_continues(
    session: AsyncSession,
    broker: PostgresBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A case 1.6 candidate that raises rolls back alone; the next candidate
    of the pass advances its parent."""
    from horsies.core.workflows import recovery as recovery_mod
    from tests.broker_resources_support import current_resources

    poison_parent_id = await _insert_workflow(session, status='RUNNING')
    poison_child_id = await _insert_workflow(
        session,
        status='COMPLETED',
        parent_workflow_id=poison_parent_id,
        parent_task_index=0,
        result=_CHILD_RESULT_ENVELOPE,
    )
    await _insert_subworkflow_node(
        session,
        workflow_id=poison_parent_id,
        task_index=0,
        sub_workflow_id=poison_child_id,
    )

    ok_parent_id = await _insert_workflow(session, status='RUNNING')
    ok_child_id = await _insert_workflow(
        session,
        status='COMPLETED',
        parent_workflow_id=ok_parent_id,
        parent_task_index=0,
        result=_CHILD_RESULT_ENVELOPE,
    )
    await _insert_subworkflow_node(
        session,
        workflow_id=ok_parent_id,
        task_index=0,
        sub_workflow_id=ok_child_id,
    )
    await session.commit()

    real_run_in_transaction = recovery_mod.run_in_transaction

    async def _poison_first_child(*args: Any, **kwargs: Any) -> Any:
        parameters = args[3]
        if parameters.get('child_workflow_id') == poison_child_id:
            raise RuntimeError(
                "reserved key '__h_task_result__' in user-originated data",
            )
        return await real_run_in_transaction(*args, **kwargs)

    monkeypatch.setattr(recovery_mod, 'run_in_transaction', _poison_first_child)
    try:
        report = await recovery_mod.recover_stuck_workflows_global(
            current_resources(broker).session_factory,
            broker,
        )
        assert report.recovered == 1
        assert await _statuses(session, poison_parent_id) == ('RUNNING', 'RUNNING')
        assert await _statuses(session, ok_parent_id) == (
            'COMPLETED',
            'COMPLETED',
        )
    finally:
        await _cleanup(
            session,
            [poison_child_id, poison_parent_id, ok_child_id, ok_parent_id],
        )


@pytest.mark.asyncio
async def test_transient_failure_retries_via_parent_stage_and_completes(
    engine: AsyncEngine,
    broker: PostgresBroker,
    session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bounded-retry invariant: first propagation attempt fails with a
    transient connection error; the parent_propagation stage schedules the
    retry, which re-enters the chain and completes the parent.

    The advance runs in the core (``advance_workflow_parents_async``). A
    trigger on the parent node's change to COMPLETED raises a retryable
    SQLSTATE (57P01) on its first firing only: a sequence counts the
    firings, and a sequence is not rolled back with the failed level."""
    from horsies.core.worker import finalize as finalize_mod

    parent_id = await _insert_workflow(session, status='RUNNING')
    child_id = await _insert_workflow(
        session,
        status='COMPLETED',
        parent_workflow_id=parent_id,
        parent_task_index=0,
        result=_CHILD_RESULT_ENVELOPE,
    )
    await _insert_subworkflow_node(
        session, workflow_id=parent_id, task_index=0, sub_workflow_id=child_id,
    )
    await session.execute(text('CREATE SEQUENCE it_parent_advance_firings'))
    await session.execute(
        text("""
            CREATE FUNCTION it_fail_the_first_advance() RETURNS trigger
            LANGUAGE plpgsql AS $body$
            BEGIN
                IF nextval('it_parent_advance_firings') = 1 THEN
                    RAISE EXCEPTION 'connection dropped' USING ERRCODE = '57P01';
                END IF;
                RETURN NEW;
            END
            $body$
        """),
    )
    await session.execute(
        text(f"""
            CREATE TRIGGER it_fail_the_first_advance
            BEFORE UPDATE ON horsies_workflow_tasks
            FOR EACH ROW
            WHEN (
                NEW.workflow_id = '{parent_id}'::uuid
                AND NEW.status = 'COMPLETED' AND OLD.status IS DISTINCT FROM 'COMPLETED'
            )
            EXECUTE FUNCTION it_fail_the_first_advance()
        """),
    )
    await session.commit()
    # Shrink the backoff so the retry fires fast. Patch the FINALIZE module's
    # binding — the constant is imported at module load, so patching runtime
    # would be a no-op (import-time binding).
    monkeypatch.setattr(finalize_mod, '_FINALIZE_RETRY_BASE_DELAY_S', 0.05)

    worker = _make_worker(engine, broker)
    try:
        await worker._drive_parent_propagations([child_id])

        # First attempt failed: the stage must have recorded the attempt.
        assert worker._finalize_retry_attempts.get(
            (child_id, _FINALIZE_STAGE_PARENT)
        ) == 1

        # The spawned retry re-enters the chain and completes the parent.
        for _ in range(80):
            node_status, wf_status = await _statuses(session, parent_id)
            if wf_status == 'COMPLETED':
                break
            await asyncio.sleep(0.05)
        assert node_status == 'COMPLETED'
        assert wf_status == 'COMPLETED'
        firings = (
            await session.execute(text('SELECT last_value FROM it_parent_advance_firings'))
        ).scalar_one()
        await session.commit()
        assert firings == 2
    finally:
        await worker.stop(force=True)
        await session.execute(
            text('DROP TRIGGER it_fail_the_first_advance ON horsies_workflow_tasks'),
        )
        await session.execute(text('DROP FUNCTION it_fail_the_first_advance()'))
        await session.execute(text('DROP SEQUENCE it_parent_advance_firings'))
        await session.commit()
        await _cleanup(session, [child_id, parent_id])


@pytest.mark.asyncio
async def test_chain_drives_every_ancestor_level(
    engine: AsyncEngine, broker: PostgresBroker, session: AsyncSession,
) -> None:
    """Grandparent <- parent <- child: the worklist iterates each level in
    its own transaction; both ancestors finalize."""
    grandparent_id = await _insert_workflow(session, status='RUNNING')
    parent_id = await _insert_workflow(
        session,
        status='RUNNING',
        parent_workflow_id=grandparent_id,
        parent_task_index=0,
    )
    child_id = await _insert_workflow(
        session,
        status='COMPLETED',
        parent_workflow_id=parent_id,
        parent_task_index=0,
        result=_CHILD_RESULT_ENVELOPE,
    )
    await _insert_subworkflow_node(
        session, workflow_id=grandparent_id, task_index=0,
        sub_workflow_id=parent_id,
    )
    await _insert_subworkflow_node(
        session, workflow_id=parent_id, task_index=0, sub_workflow_id=child_id,
    )
    await session.commit()

    worker = _make_worker(engine, broker)
    try:
        await worker._drive_parent_propagations([child_id])

        parent_node, parent_wf = await _statuses(session, parent_id)
        grand_node, grand_wf = await _statuses(session, grandparent_id)
        assert (parent_node, parent_wf) == ('COMPLETED', 'COMPLETED')
        assert (grand_node, grand_wf) == ('COMPLETED', 'COMPLETED')
    finally:
        await _cleanup(session, [child_id, parent_id, grandparent_id])
