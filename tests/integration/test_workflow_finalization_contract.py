"""Workflow finalization: stored output and the workflow_done notification.

A COMPLETED workflow stores its explicit output node's result, or the
outputless envelope of its terminal nodes. A FAILED workflow stores no
output. The status trigger delivers exactly one workflow_done
notification, after commit and never before it.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator

import psycopg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from horsies.core.utils.url import to_psycopg_url
from horsies.core.workflows.engine import check_workflow_completion
from tests.integration.conftest import DB_URL
from tests.integration.test_workflow_denest_propagation import (
    _insert_subworkflow_node,
    _insert_workflow,
    _make_worker,
)

pytestmark = [pytest.mark.integration]

NOTIFICATION_WAIT_SECONDS = 0.5
NODE_COUNT = 3


def _ok_envelope(index: int) -> str:
    return json.dumps({'__h_task_result__': True, 'ok': f'node-{index}', 'err': None})


def _err_envelope() -> str:
    return json.dumps({
        '__h_task_result__': True,
        'ok': None,
        'err': {'error_code': 'TASK_EXCEPTION', 'message': 'node failed'},
    })


@pytest_asyncio.fixture
async def workflow_done_listener() -> AsyncGenerator[psycopg.AsyncConnection, None]:
    connection = await psycopg.AsyncConnection.connect(
        to_psycopg_url(DB_URL), autocommit=True,
    )
    await connection.execute('LISTEN workflow_done')
    yield connection
    await connection.close()


async def _received(
    listener: psycopg.AsyncConnection,
) -> list[str]:
    payloads: list[str] = []
    try:
        async with asyncio.timeout(NOTIFICATION_WAIT_SECONDS):
            async for notification in listener.notifies():
                payloads.append(notification.payload)
    except TimeoutError:
        pass
    return payloads


async def _seed_finished_workflow(
    session: AsyncSession,
    *,
    succeeded: bool,
    output_task_index: int | None,
    parent_workflow_id: str | None = None,
) -> str:
    """A RUNNING workflow whose nodes are all terminal. A failed workflow's
    node 0 failed; every other node completed with its own result."""
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, output_task_index, depth,
                 root_workflow_id, parent_workflow_id, parent_task_index,
                 sent_at, created_at, started_at, updated_at)
            VALUES
                (:id, 'finalization_contract', 'RUNNING', 'fail', :output,
                 :depth, :root, :parent, :parent_index,
                 NOW(), NOW(), NOW(), NOW())
        """),
        {
            'id': workflow_id,
            'output': output_task_index,
            'depth': 0 if parent_workflow_id is None else 1,
            'root': parent_workflow_id or workflow_id,
            'parent': parent_workflow_id,
            'parent_index': None if parent_workflow_id is None else 0,
        },
    )
    for index in range(NODE_COUNT):
        failed = not succeeded and index == 0
        await session.execute(
            text("""
                INSERT INTO horsies_workflow_tasks
                    (id, workflow_id, task_index, node_id, task_name, task_args,
                     task_kwargs, queue_name, priority, dependencies,
                     allow_failed_deps, join_type, is_subworkflow, status,
                     result, created_at)
                VALUES
                    (:id, :workflow_id, :index, :node_id, 'contract_task',
                     '[]', '{}', 'default', 100, '{}', FALSE, 'all', FALSE,
                     :status, :result, NOW())
            """),
            {
                'id': str(uuid.uuid4()),
                'workflow_id': workflow_id,
                'index': index,
                'node_id': f'node_{index}',
                'status': 'FAILED' if failed else 'COMPLETED',
                'result': _err_envelope() if failed else _ok_envelope(index),
            },
        )
    await session.commit()
    return workflow_id


@pytest.mark.asyncio(loop_scope='function')
@pytest.mark.parametrize('output_task_index', [None, 1])
@pytest.mark.parametrize('succeeded', [True, False])
async def test_finalization_stores_policy_output_and_notifies_once_after_commit(
    clean_workflow_tables: None,
    session: AsyncSession,
    workflow_done_listener: psycopg.AsyncConnection,
    succeeded: bool,
    output_task_index: int | None,
) -> None:
    _ = clean_workflow_tables
    workflow_id = await _seed_finished_workflow(
        session, succeeded=succeeded, output_task_index=output_task_index,
    )

    await check_workflow_completion(session, workflow_id)
    assert await _received(workflow_done_listener) == []
    await session.commit()
    assert await _received(workflow_done_listener) == [workflow_id]

    row = (await session.execute(
        text('SELECT status, result FROM horsies_workflows WHERE id = :id'),
        {'id': workflow_id},
    )).one()
    match (succeeded, output_task_index):
        case (False, _):
            assert row.status == 'FAILED'
            assert row.result is None
        case (True, None):
            assert row.status == 'COMPLETED'
            stored = json.loads(row.result)
            assert stored['__h_outputless_terminals__'] is True
            assert set(stored['ok']['results_by_id']) == {
                f'node_{index}' for index in range(NODE_COUNT)
            }
        case (True, int() as index):
            assert row.status == 'COMPLETED'
            assert json.loads(row.result) == json.loads(_ok_envelope(index))


@pytest.mark.asyncio(loop_scope='function')
async def test_rolled_back_finalization_sends_no_notification(
    clean_workflow_tables: None,
    session: AsyncSession,
    workflow_done_listener: psycopg.AsyncConnection,
) -> None:
    _ = clean_workflow_tables
    workflow_id = await _seed_finished_workflow(
        session, succeeded=True, output_task_index=None,
    )

    await check_workflow_completion(session, workflow_id)
    await session.rollback()

    assert await _received(workflow_done_listener) == []
    status = (await session.execute(
        text('SELECT status FROM horsies_workflows WHERE id = :id'),
        {'id': workflow_id},
    )).scalar_one()
    assert status == 'RUNNING'


@pytest.mark.asyncio(loop_scope='function')
async def test_a_failed_child_gives_its_parent_a_summary_without_output(
    clean_workflow_tables: None,
    engine: AsyncEngine,
    session: AsyncSession,
) -> None:
    """The child's output node completed, but the child FAILED: the parent's
    SubWorkflowSummary has no output."""
    _ = clean_workflow_tables
    parent_id = await _insert_workflow(session, status='RUNNING')
    child_id = await _seed_finished_workflow(
        session,
        succeeded=False,
        output_task_index=1,
        parent_workflow_id=parent_id,
    )
    await _insert_subworkflow_node(
        session, workflow_id=parent_id, task_index=0, sub_workflow_id=child_id,
    )
    await session.commit()

    await check_workflow_completion(session, child_id)
    await session.commit()
    await _make_worker(engine)._drive_parent_propagations([child_id])

    summary_text = (await session.execute(
        text("""
            SELECT sub_workflow_summary FROM horsies_workflow_tasks
            WHERE workflow_id = :wf AND task_index = 0
        """),
        {'wf': parent_id},
    )).scalar_one()
    summary = json.loads(summary_text)
    assert summary['status'] == 'FAILED'
    assert summary['output'] is None
