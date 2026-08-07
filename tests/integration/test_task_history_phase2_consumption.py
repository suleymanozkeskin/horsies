"""Phase-2 consumption against real PostgreSQL.

The full disposition matrix from real pending rows created by the real
failure move: applied-to-node with the result recorded from history,
idempotent replay after the evidence is gone, supersession by a terminal
workflow, and every evidence-retaining verdict proven to actually retain
— pending intact after digest mismatches and absent sources. Node
recording is proven independent of workflow RUNNING: pause gates
propagation, not durable recording.
"""

from __future__ import annotations

from hashlib import sha256
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.phase2.consumption import (
    Phase2Disposition,
    consume_phase2,
)

from tests.integration.task_history_harness import (
    HistorySchema,
    create_workflow,
    insert_live_task,
    link_workflow_node,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

CLASS_KEY = 'it_phase2'
WORKER = 'worker-p2-1'
FAILURE_RESULT = '{"error":{"code":"BOOM"}}'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_phase2'
)


async def seed_pending(
    connection: AsyncConnection,
    *,
    workflow_status: str = 'RUNNING',
) -> tuple[str, str, str]:
    """Fail a workflow-backing task for real; returns (task, workflow, node)."""
    task_id = await insert_live_task(
        connection,
        class_key=CLASS_KEY,
        worker=WORKER,
        is_workflow_task=True,
    )
    workflow_id = await create_workflow(connection, status=workflow_status)
    node_id = await link_workflow_node(
        connection, task_id, workflow_id=workflow_id, node_status='RUNNING'
    )
    outcome = (
        await connection.execute(
            text(
                'SELECT outcome FROM horsies_fail_locked_task('
                'CAST(:task_id AS uuid), :worker, :result, '
                "'BOOM', 'exploded')"
            ),
            {'task_id': task_id, 'worker': WORKER, 'result': FAILURE_RESULT},
        )
    ).one()
    assert outcome.outcome == 'APPLIED'
    return task_id, workflow_id, node_id


async def pending_exists(connection: AsyncConnection, task_id: str) -> bool:
    return bool(
        (
            await connection.execute(
                text(
                    'SELECT count(*) FROM horsies_workflow_phase2_pending '
                    'WHERE task_id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
        ).scalar_one()
    )


async def node_row(connection: AsyncConnection, node_id: str) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT status, result FROM horsies_workflow_tasks '
                'WHERE id = CAST(:node_id AS uuid)'
            ),
            {'node_id': node_id},
        )
    ).one()


class TestDurableDispositions:
    @pytest.mark.asyncio
    async def test_applied_to_node_records_the_history_result(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, workflow_id, node_id = await seed_pending(connection)
            verdict = await consume_phase2(
                connection, task_id=task_id, terminal_node_status='FAILED'
            )
            assert isinstance(verdict, Phase2Disposition)
            assert verdict.disposition == 'APPLIED_TO_NODE'
            assert verdict.workflow_id == workflow_id
            assert verdict.node_status == 'FAILED'
            assert verdict.terminal_status == 'FAILED'
            assert verdict.workflow_status == 'RUNNING'
            node = await node_row(connection, node_id)
            assert node.status == 'FAILED'
            assert node.result == FAILURE_RESULT
            assert not await pending_exists(connection, task_id)

    @pytest.mark.asyncio
    async def test_replay_after_evidence_is_gone_is_already_applied(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, _ = await seed_pending(connection)
            await consume_phase2(
                connection, task_id=task_id, terminal_node_status='FAILED'
            )
            replay = await consume_phase2(
                connection, task_id=task_id, terminal_node_status='FAILED'
            )
            assert replay.disposition == 'ALREADY_APPLIED'
            assert replay.node_status == 'FAILED'

    @pytest.mark.asyncio
    async def test_terminal_workflow_supersedes_without_touching_the_node(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, node_id = await seed_pending(
                connection, workflow_status='CANCELLED'
            )
            verdict = await consume_phase2(
                connection, task_id=task_id, terminal_node_status='FAILED'
            )
            assert verdict.disposition == 'SUPERSEDED_BY_WORKFLOW_TERMINAL'
            node = await node_row(connection, node_id)
            assert node.status == 'RUNNING'
            assert node.result is None
            assert not await pending_exists(connection, task_id)

    @pytest.mark.asyncio
    async def test_paused_workflow_still_records_the_node(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, node_id = await seed_pending(
                connection, workflow_status='PAUSED'
            )
            verdict = await consume_phase2(
                connection, task_id=task_id, terminal_node_status='FAILED'
            )
            assert verdict.disposition == 'APPLIED_TO_NODE'
            assert verdict.workflow_status == 'PAUSED'
            node = await node_row(connection, node_id)
            assert node.status == 'FAILED'


class TestEvidenceRetention:
    @pytest.mark.asyncio
    async def test_digest_mismatch_retains_pending(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, node_id = await seed_pending(connection)
            await connection.execute(
                text(
                    'UPDATE horsies_workflow_phase2_pending '
                    'SET result_digest = :bad '
                    'WHERE task_id = CAST(:task_id AS uuid)'
                ),
                {'bad': sha256(b'not the result').digest(), 'task_id': task_id},
            )
            verdict = await consume_phase2(
                connection, task_id=task_id, terminal_node_status='FAILED'
            )
            assert verdict.disposition == 'SOURCE_DIGEST_MISMATCH'
            assert await pending_exists(connection, task_id)
            node = await node_row(connection, node_id)
            assert node.status == 'RUNNING'

    @pytest.mark.asyncio
    async def test_absent_source_retains_pending(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, _ = await seed_pending(connection)
            await connection.execute(
                text(
                    'UPDATE horsies_workflow_phase2_pending '
                    "SET history_anchor = history_anchor + interval '90 days' "
                    'WHERE task_id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
            verdict = await consume_phase2(
                connection, task_id=task_id, terminal_node_status='FAILED'
            )
            assert verdict.disposition == 'SOURCE_ABSENT'
            assert await pending_exists(connection, task_id)

    @pytest.mark.asyncio
    async def test_unknown_task_is_pending_absent(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            verdict = await consume_phase2(
                connection,
                task_id=str(uuid4()),
                terminal_node_status='FAILED',
            )
            assert verdict.disposition == 'PENDING_ABSENT'

    @pytest.mark.asyncio
    async def test_invalid_node_status_raises_typed(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.connect() as connection:
            with pytest.raises(DBAPIError, match='terminal node status'):
                await consume_phase2(
                    connection,
                    task_id=str(uuid4()),
                    terminal_node_status='RUNNING',
                )
