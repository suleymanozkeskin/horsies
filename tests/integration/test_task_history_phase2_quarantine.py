"""Phase-2 quarantine function against real PostgreSQL.

The protocol from real pending rows created by the real failure move: a
locator is copied, verified, and repointed; the pending row swaps to the
quarantine source with its history locator cleared; consumption then serves
from quarantine and deletes both rows at disposition. A refusal is proven to
retain evidence: a broken locator keeps its history source untouched.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.phase2.consumption import consume_phase2

from tests.integration.task_history_harness import (
    HistorySchema,
    create_workflow,
    insert_live_task,
    link_workflow_node,
    prepare_move_storage,
    terminalization_schema_fixture,
)
from tests.integration.terminal_writes import fail_running_task, task_failure

pytestmark = [pytest.mark.integration]

CLASS_KEY = 'standard_30d'
WORKER = 'worker-p2q-1'
FAILURE_RESULT = '{"error":{"code":"BOOM"}}'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_p2_quarantine'
)


async def seed_pending(
    connection: AsyncConnection,
) -> tuple[str, str, str]:
    """Fail a workflow-backing task for real; returns (task, workflow, node)."""
    task_id = await insert_live_task(
        connection,
        class_key=CLASS_KEY,
        worker=WORKER,
        is_workflow_task=True,
    )
    workflow_id = await create_workflow(connection, status='RUNNING')
    node_id = await link_workflow_node(
        connection,
        task_id,
        workflow_id=workflow_id,
        node_status='RUNNING',
    )
    outcome = await fail_running_task(
        connection,
        task_id,
        worker_id=WORKER,
        failure=task_failure(
            result_json=FAILURE_RESULT, error_code='BOOM', failed_reason='exploded'
        ),
    )
    assert outcome == 'APPLIED'
    return task_id, workflow_id, node_id


async def quarantine_one(connection: AsyncConnection, task_id: str) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_phase2_quarantine_one('
                'CAST(:task_id AS uuid))'
            ),
            {'task_id': task_id},
        )
    ).one()


async def pending_row(connection: AsyncConnection, task_id: str) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT recovery_source, history_class, history_anchor, '
                'quarantine_task_id '
                'FROM horsies_workflow_phase2_pending '
                'WHERE task_id = CAST(:task_id AS uuid)'
            ),
            {'task_id': task_id},
        )
    ).one()


class TestRepoint:
    @pytest.mark.asyncio
    async def test_locator_is_copied_verified_repointed(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, _ = await seed_pending(connection)

            verdict = await quarantine_one(connection, task_id)
            assert verdict.verdict == 'REPOINTED'

            pending = await pending_row(connection, task_id)
            assert pending.recovery_source == 'QUARANTINE'
            assert str(pending.quarantine_task_id) == task_id
            assert pending.history_class is None
            assert pending.history_anchor is None

            copy = (
                await connection.execute(
                    text(
                        'SELECT terminalization_kind, retention_class_key, '
                        "convert_from(result_payload, 'UTF8') AS payload "
                        'FROM horsies_workflow_phase2_quarantine '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
            assert copy.terminalization_kind == 'FAIL_RUNNING'
            assert copy.retention_class_key == CLASS_KEY
            assert copy.payload == FAILURE_RESULT

    @pytest.mark.asyncio
    async def test_consumption_serves_from_quarantine_and_deletes_both(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, node_id = await seed_pending(connection)
            repointed = await quarantine_one(connection, task_id)
            assert repointed.verdict == 'REPOINTED'

            verdict = await consume_phase2(
                connection, task_id=task_id, terminal_node_status='FAILED'
            )
            assert verdict.disposition == 'APPLIED_TO_NODE'
            node = (
                await connection.execute(
                    text(
                        'SELECT status, result FROM horsies_workflow_tasks '
                        'WHERE id = CAST(:node_id AS uuid)'
                    ),
                    {'node_id': node_id},
                )
            ).one()
            assert node.status == 'FAILED'
            assert node.result == FAILURE_RESULT
            for table in (
                'horsies_workflow_phase2_pending',
                'horsies_workflow_phase2_quarantine',
            ):
                count = (
                    await connection.execute(
                        text(
                            f'SELECT count(*) FROM {table} '
                            'WHERE task_id = CAST(:task_id AS uuid)'
                        ),
                        {'task_id': task_id},
                    )
                ).scalar_one()
                assert count == 0, table

    @pytest.mark.asyncio
    async def test_copy_goes_with_its_node(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """The copy refers to its node, so the retention delete of the
        workflow removes the copy with the pending row that points at it."""
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, workflow_id, _ = await seed_pending(connection)
            repointed = await quarantine_one(connection, task_id)
            assert repointed.verdict == 'REPOINTED'

            await connection.execute(
                text(
                    'DELETE FROM horsies_workflows '
                    'WHERE id = CAST(:workflow_id AS uuid)'
                ),
                {'workflow_id': workflow_id},
            )
            for table in (
                'horsies_workflow_phase2_pending',
                'horsies_workflow_phase2_quarantine',
            ):
                count = (
                    await connection.execute(
                        text(
                            f'SELECT count(*) FROM {table} '
                            'WHERE task_id = CAST(:task_id AS uuid)'
                        ),
                        {'task_id': task_id},
                    )
                ).scalar_one()
                assert count == 0, table


class TestRefusal:
    @pytest.mark.asyncio
    async def test_broken_locator_refuses_and_retains_history_source(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, _ = await seed_pending(connection)
            # A locator that names no history row: the anchor moves by one
            # second, so the one-leaf probe finds nothing.
            await connection.execute(
                text(
                    'UPDATE horsies_workflow_phase2_pending '
                    "SET history_anchor = history_anchor + interval '1 second' "
                    'WHERE task_id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
            verdict = await quarantine_one(connection, task_id)
            assert verdict.verdict == 'SOURCE_ABSENT'
            pending = await pending_row(connection, task_id)
            assert pending.recovery_source == 'HISTORY'
            assert pending.history_class == CLASS_KEY
            quarantine_count = (
                await connection.execute(
                    text(
                        'SELECT count(*) '
                        'FROM horsies_workflow_phase2_quarantine '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert quarantine_count == 0

    @pytest.mark.asyncio
    async def test_unknown_task_is_pending_gone(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            row = await quarantine_one(connection, str(uuid4()))
            assert row.verdict == 'PENDING_GONE'
