"""Close the persisted workflow-node status set."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.phase2.consumption import PHASE2_CONSUME_FUNCTION_DDL


class WorkflowNodeStatusMigrationError(RuntimeError):
    """Existing nodes contain a status outside the typed node enum."""

    def __init__(self, status: str) -> None:
        self.status = status
        super().__init__(
            f'unsupported workflow-node status {status!r}; '
            'repair these rows before retrying the migration'
        )


async def close_workflow_node_status_set(conn: AsyncConnection) -> None:
    """Refuse invalid rows before changing the constraint or function."""
    await conn.execute(text(
        'LOCK TABLE horsies_workflow_tasks IN SHARE ROW EXCLUSIVE MODE'
    ))
    invalid_status = (await conn.execute(text("""
        SELECT status FROM horsies_workflow_tasks
        WHERE status NOT IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING',
                             'COMPLETED', 'FAILED', 'SKIPPED')
        LIMIT 1
    """))).scalar_one_or_none()
    if invalid_status is not None:
        raise WorkflowNodeStatusMigrationError(invalid_status)
    await conn.execute(text("""
        DO $migration$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'horsies_workflow_tasks'::regclass
                  AND conname = 'horsies_workflow_tasks_status_check'
            ) THEN
                ALTER TABLE horsies_workflow_tasks
                    ADD CONSTRAINT horsies_workflow_tasks_status_check CHECK (
                        status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING',
                                   'COMPLETED', 'FAILED', 'SKIPPED')
                    );
            END IF;
        END
        $migration$
    """))
    installed = (await conn.execute(text(
        "SELECT to_regprocedure('horsies_phase2_consume(uuid,text)') IS NOT NULL"
    ))).scalar_one()
    if installed:
        await conn.execute(text(PHASE2_CONSUME_FUNCTION_DDL.replace(
            'CREATE FUNCTION', 'CREATE OR REPLACE FUNCTION', 1
        )))
