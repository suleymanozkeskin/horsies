"""Durable distinction between migrated and fully cut-over databases.

The integer schema version records which idempotent migration chain ran.
It cannot also represent completion of the separately operated offline
cutover.  This singleton marker is written only after the database reaches
the frozen structural posture.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


CUTOVER_STATE_TABLE = 'horsies_cutover_state'
CUTOVER_NAME = 'task_history_v1'

CREATE_CUTOVER_STATE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {CUTOVER_STATE_TABLE} (
    cutover_name text PRIMARY KEY,
    completed_at timestamptz NOT NULL DEFAULT NOW()
)
"""

MARK_CUTOVER_COMPLETE_DDL = f"""
INSERT INTO {CUTOVER_STATE_TABLE} (cutover_name)
VALUES ('{CUTOVER_NAME}')
ON CONFLICT (cutover_name) DO NOTHING
"""

CREATE_CUTOVER_STATE_TABLE_SQL = text(CREATE_CUTOVER_STATE_TABLE_DDL)
MARK_CUTOVER_COMPLETE_SQL = text(MARK_CUTOVER_COMPLETE_DDL)
CUTOVER_STATE_TABLE_EXISTS_SQL = text(
    f"SELECT to_regclass('{CUTOVER_STATE_TABLE}') IS NOT NULL"
)
READ_CUTOVER_COMPLETE_SQL = text(
    f'SELECT EXISTS (SELECT 1 FROM {CUTOVER_STATE_TABLE} '
    'WHERE cutover_name = :cutover_name)'
)


async def cutover_complete(connection: AsyncConnection) -> bool:
    """Whether the durable completion marker exists.

    The table check is separate because a version-corrupt or pre-v35 database
    must produce ``False`` rather than an undefined-table error.
    """
    table_exists = bool(
        (await connection.execute(CUTOVER_STATE_TABLE_EXISTS_SQL)).scalar_one()
    )
    if not table_exists:
        return False
    return bool(
        (
            await connection.execute(
                READ_CUTOVER_COMPLETE_SQL,
                {'cutover_name': CUTOVER_NAME},
            )
        ).scalar_one()
    )
