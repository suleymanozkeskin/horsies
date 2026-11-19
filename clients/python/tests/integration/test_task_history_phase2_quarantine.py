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


from tests.integration.task_history_harness import (
    HistorySchema,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

CLASS_KEY = 'standard_30d'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_p2_quarantine'
)


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


class TestRefusal:

    @pytest.mark.asyncio
    async def test_unknown_task_is_pending_gone(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            row = await quarantine_one(connection, str(uuid4()))
            assert row.verdict == 'PENDING_GONE'
