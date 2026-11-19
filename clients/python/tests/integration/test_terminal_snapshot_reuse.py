"""The single-row deferred move writes the history row's result digest to
the deferred record.

The batch moves encode each attempt snapshot once; the core tests that
(``core/src/broker/terminalization_matrix.rs``,
``batch_terminalization_encodes_each_attempt_snapshot_once``).
"""

from __future__ import annotations

from hashlib import sha256

import pytest
from sqlalchemy import text

from tests.integration.task_history_harness import (
    HistorySchema,
    create_workflow,
    insert_live_task,
    link_workflow_node,
    prepare_move_storage,
    terminalization_schema_fixture,
)
from tests.integration.terminal_writes import complete_workflow_task

pytestmark = [pytest.mark.integration]

CLASS_KEY = 'standard_30d'
WORKER = 'worker-snapshot-reuse'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_snapshot_reuse'
)


class TestDeferredDigestReuse:
    @pytest.mark.asyncio
    async def test_deferred_record_carries_the_history_result_digest(
        self, terminalization_schema: HistorySchema,
    ) -> None:
        result = '{"ok":true}'
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                is_workflow_task=True,
            )
            await link_workflow_node(
                connection,
                task_id,
                workflow_id=await create_workflow(connection, status='RUNNING'),
                node_status='RUNNING',
            )
            outcome = await complete_workflow_task(
                connection, task_id, worker_id=WORKER, result_json=result
            )
            assert outcome == 'APPLIED'
            digests = (await connection.execute(
                text("""
                    SELECT h.result_digest AS history_digest,
                           p.result_digest AS deferred_digest
                    FROM horsies_task_history h
                    JOIN horsies_workflow_phase2_pending p
                        ON p.task_id = h.task_id
                    WHERE h.task_id = CAST(:task_id AS uuid)
                """),
                {'task_id': task_id},
            )).one()
            expected = sha256(result.encode()).digest()
            assert bytes(digests.history_digest) == expected
            assert bytes(digests.deferred_digest) == expected
