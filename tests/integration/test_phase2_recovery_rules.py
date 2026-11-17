"""Phase-2 recovery rules against the baseline schema: rollback and diagnostics.

`horsies_phase2_quarantine_one` copies the history result of a pending row
into the quarantine table and repoints the row; `horsies_phase2_consume`
writes the node and deletes the recovery rows. Each runs in its caller's
transaction, so a fault at its last write rolls back every write before it.

The recovery pass records each retaining disposition on the pending row
(`attempt_count`, `last_failure_class`) and attempts the quarantine only at
its attempt bound.
"""

from __future__ import annotations

from typing import Final

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, async_sessionmaker

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.workflows.phase2_recovery import drive_phase2_recovery
from tests.integration.task_history_harness import (
    HistorySchema,
    create_workflow,
    insert_live_task,
    link_workflow_node,
    prepare_move_storage,
    terminalization_schema_fixture,
)
from tests.integration.terminal_writes import fail_running_task, task_failure

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CLASS_KEY: Final = 'standard_30d'
WORKER: Final = 'worker-phase2-rules'
MAX_ROWS: Final = 10
ATTEMPT_BOUND: Final = 3
"""The smallest attempt bound that ``horsies_workflow_phase2_recover`` takes (3 to 1000)."""

terminalization_schema = terminalization_schema_fixture('task_history_it_phase2')


async def seed_failed_workflow_task(connection: AsyncConnection) -> str:
    """A workflow task whose failure moved to history with a HISTORY pending row."""
    task_id = await insert_live_task(
        connection,
        class_key=CLASS_KEY,
        worker=WORKER,
        is_workflow_task=True,
    )
    workflow_id = await create_workflow(connection, status='RUNNING')
    await link_workflow_node(
        connection, task_id, workflow_id=workflow_id, node_status='RUNNING'
    )
    outcome = await fail_running_task(
        connection,
        task_id,
        worker_id=WORKER,
        failure=task_failure(
            result_json='{"error":"boom"}', error_code='BOOM', failed_reason='exploded'
        ),
    )
    assert outcome == 'APPLIED'
    return task_id


async def quarantine(connection: AsyncConnection, task_id: str) -> str:
    return str(
        (
            await connection.execute(
                text(
                    'SELECT verdict FROM horsies_phase2_quarantine_one('
                    'CAST(:task_id AS uuid))'
                ),
                {'task_id': task_id},
            )
        ).scalar_one()
    )


async def recovery_rows(connection: AsyncConnection, task_id: str) -> tuple[object, ...]:
    """The pending row's source and the counts of pending and quarantine rows."""
    row = (
        await connection.execute(
            text(
                'SELECT '
                '(SELECT recovery_source FROM horsies_workflow_phase2_pending '
                ' WHERE task_id = CAST(:task_id AS uuid)) AS source, '
                '(SELECT count(*) FROM horsies_workflow_phase2_pending '
                ' WHERE task_id = CAST(:task_id AS uuid)) AS pending, '
                '(SELECT count(*) FROM horsies_workflow_phase2_quarantine '
                ' WHERE task_id = CAST(:task_id AS uuid)) AS quarantined, '
                '(SELECT status FROM horsies_workflow_tasks '
                ' WHERE task_id = CAST(:task_id AS uuid)) AS node_status'
            ),
            {'task_id': task_id},
        )
    ).one()
    return tuple(row)


async def test_a_failed_repoint_rolls_back_the_quarantine_copy(
    terminalization_schema: HistorySchema,
) -> None:
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY)
        task_id = await seed_failed_workflow_task(connection)
        await connection.execute(
            text(
                """
                CREATE FUNCTION it_refuse_pending_repoint() RETURNS trigger
                LANGUAGE plpgsql AS $function$
                BEGIN
                    IF NEW.recovery_source = 'QUARANTINE' THEN
                        RAISE EXCEPTION 'repoint refused by the test';
                    END IF;
                    RETURN NEW;
                END
                $function$
                """
            )
        )
        await connection.execute(
            text(
                'CREATE TRIGGER it_refuse_pending_repoint '
                'BEFORE UPDATE ON horsies_workflow_phase2_pending '
                'FOR EACH ROW EXECUTE FUNCTION it_refuse_pending_repoint()'
            )
        )

        with pytest.raises(DBAPIError) as raised:
            async with connection.begin_nested():
                await quarantine(connection, task_id)
        assert 'repoint refused by the test' in str(raised.value.orig)

        assert await recovery_rows(connection, task_id) == (
            'HISTORY',
            1,
            0,
            'RUNNING',
        )


async def test_a_failed_pending_delete_rolls_back_the_node_write(
    terminalization_schema: HistorySchema,
) -> None:
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY)
        task_id = await seed_failed_workflow_task(connection)
        assert await quarantine(connection, task_id) == 'REPOINTED'
        await connection.execute(
            text(
                """
                CREATE FUNCTION it_refuse_pending_delete() RETURNS trigger
                LANGUAGE plpgsql AS $function$
                BEGIN
                    RAISE EXCEPTION 'pending delete refused by the test';
                END
                $function$
                """
            )
        )
        await connection.execute(
            text(
                'CREATE TRIGGER it_refuse_pending_delete '
                'BEFORE DELETE ON horsies_workflow_phase2_pending '
                'FOR EACH ROW EXECUTE FUNCTION it_refuse_pending_delete()'
            )
        )

        with pytest.raises(DBAPIError) as raised:
            async with connection.begin_nested():
                await connection.execute(
                    text(
                        'SELECT disposition FROM horsies_phase2_consume('
                        "CAST(:task_id AS uuid), 'FAILED')"
                    ),
                    {'task_id': task_id},
                )
        assert 'pending delete refused by the test' in str(raised.value.orig)

        assert await recovery_rows(connection, task_id) == (
            'QUARANTINE',
            1,
            1,
            'RUNNING',
        )


@pytest.mark.parametrize(
    ('fault', 'failure_class'),
    [
        (
            "UPDATE horsies_workflow_phase2_pending "
            "SET result_digest = decode(repeat('ab', 32), 'hex') "
            'WHERE task_id = CAST(:task_id AS uuid)',
            'SOURCE_DIGEST_MISMATCH',
        ),
        (
            'UPDATE horsies_workflow_phase2_pending '
            'SET history_schema_version = 2 '
            'WHERE task_id = CAST(:task_id AS uuid)',
            'SOURCE_VERSION_CONFLICT',
        ),
        (
            'UPDATE horsies_workflow_phase2_pending '
            "SET history_anchor = history_anchor + interval '1 second' "
            'WHERE task_id = CAST(:task_id AS uuid)',
            'SOURCE_ABSENT',
        ),
    ],
    ids=['digest-mismatch', 'unknown-version', 'absent-source'],
)
async def test_a_retained_row_records_its_failure_class_and_keeps_its_evidence(
    terminalization_schema: HistorySchema,
    broker: PostgresBroker,
    fault: str,
    failure_class: str,
) -> None:
    """Each pass under the bound records the disposition; the pass at the
    bound attempts the quarantine, which refuses this evidence and leaves
    the row on its history locator."""
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY)
        task_id = await seed_failed_workflow_task(connection)
        await connection.execute(text(fault), {'task_id': task_id})

    sessions = async_sessionmaker(
        terminalization_schema.engine, expire_on_commit=False
    )
    for attempt in range(1, ATTEMPT_BOUND):
        under = await drive_phase2_recovery(
            sessions,
            broker,
            grace_ms=0,
            max_rows=MAX_ROWS,
            quarantine_after_attempts=ATTEMPT_BOUND,
        )
        assert (under.retained, under.quarantined) == (1, 0), under
        assert under.quarantine_refusals == ()
        async with terminalization_schema.engine.connect() as connection:
            recorded = (
                await connection.execute(
                    text(
                        'SELECT attempt_count, last_failure_class, recovery_source '
                        'FROM horsies_workflow_phase2_pending '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
        assert tuple(recorded) == (attempt, failure_class, 'HISTORY')

    at_bound = await drive_phase2_recovery(
        sessions,
        broker,
        grace_ms=0,
        max_rows=MAX_ROWS,
        quarantine_after_attempts=ATTEMPT_BOUND,
    )
    assert (at_bound.retained, at_bound.quarantined) == (1, 0)
    assert len(at_bound.quarantine_refusals) == 1
    async with terminalization_schema.engine.connect() as connection:
        assert await recovery_rows(connection, task_id) == (
            'HISTORY',
            1,
            0,
            'RUNNING',
        )
        recorded = (
            await connection.execute(
                text(
                    'SELECT attempt_count, last_failure_class '
                    'FROM horsies_workflow_phase2_pending '
                    'WHERE task_id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
        ).one()
    assert tuple(recorded) == (ATTEMPT_BOUND, failure_class)
