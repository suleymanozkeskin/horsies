"""Workflow-node status migration refusal and database enforcement."""
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from horsies.core.history.phase2.consumption import PHASE2_CONSUME_FUNCTION_DDL
from horsies.core.models.workflow.enums import WorkflowTaskStatus
from horsies.core.schemas.node_status import (
    WorkflowNodeStatusMigrationError,
    close_workflow_node_status_set,
)
from tests.integration.task_history_harness import (
    HistorySchema, create_workflow, link_workflow_node, terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]
terminalization_schema = terminalization_schema_fixture('node_status_migration')


async def test_node_status_upgrade_refuses_invalid_rows_then_retries(
    terminalization_schema: HistorySchema,
) -> None:
    async with terminalization_schema.engine.begin() as conn:
        workflow = await create_workflow(conn, status="RUNNING")
        node = await link_workflow_node(
            conn, str(uuid4()), workflow_id=workflow, node_status='CANCELLED'
        )
        old_function = PHASE2_CONSUME_FUNCTION_DDL.replace(
            'CREATE FUNCTION', 'CREATE OR REPLACE FUNCTION', 1
        ).replace(
            "p_terminal_node_status IS NULL OR p_terminal_node_status NOT IN ('COMPLETED', 'FAILED')",
            "p_terminal_node_status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')",
        )
        await conn.execute(text(old_function))
        function_sql = text(
            "SELECT pg_get_functiondef('horsies_phase2_consume(uuid,text)'::regprocedure)"
        )
        before = (await conn.execute(function_sql)).scalar_one()
        with pytest.raises(WorkflowNodeStatusMigrationError) as refusal:
            async with conn.begin_nested():
                await close_workflow_node_status_set(conn)
        assert refusal.value.status == 'CANCELLED'
        assert (await conn.execute(function_sql)).scalar_one() == before
        assert (await conn.execute(text(
            'SELECT status FROM horsies_workflow_tasks WHERE id=CAST(:id AS uuid)'
        ), {'id': node})).scalar_one() == 'CANCELLED'
        assert not (await conn.execute(text("""
            SELECT EXISTS (SELECT 1 FROM pg_constraint
            WHERE conrelid='horsies_workflow_tasks'::regclass
            AND conname='horsies_workflow_tasks_status_check')
        """))).scalar_one()
        await conn.execute(text(
            "UPDATE horsies_workflow_tasks SET status='FAILED' WHERE id=CAST(:id AS uuid)"
        ), {'id': node})
        await close_workflow_node_status_set(conn)
        await close_workflow_node_status_set(conn)
        for status in WorkflowTaskStatus:
            await conn.execute(text(
                'UPDATE horsies_workflow_tasks SET status=:status WHERE id=CAST(:id AS uuid)'
            ), {'id': node, 'status': status.value})
        for status in ['CANCELLED', 'EXPIRED', 'UNKNOWN', '']:
            with pytest.raises(DBAPIError) as error:
                async with conn.begin_nested():
                    await conn.execute(text(
                        'UPDATE horsies_workflow_tasks SET status=:status WHERE id=CAST(:id AS uuid)'
                    ), {'id': node, 'status': status})
            assert error.value.orig.sqlstate == '23514'
        assert (await conn.execute(text("""
            SELECT convalidated FROM pg_constraint
            WHERE conrelid='horsies_workflow_tasks'::regclass
            AND conname='horsies_workflow_tasks_status_check'
        """))).scalar_one()
        for status in ['CANCELLED', 'SKIPPED', 'UNKNOWN', None]:
            with pytest.raises(DBAPIError) as error:
                async with conn.begin_nested():
                    await conn.execute(text(
                        'SELECT horsies_phase2_consume(CAST(:id AS uuid), :status)'
                    ), {'id': str(uuid4()), 'status': status})
            assert error.value.orig.sqlstate == '22023'
        for status in ['COMPLETED', 'FAILED']:
            disposition = (await conn.execute(text(
                'SELECT (horsies_phase2_consume(CAST(:id AS uuid), :status)).disposition'
            ), {'id': str(uuid4()), 'status': status})).scalar_one()
            assert disposition == 'PENDING_ABSENT'
