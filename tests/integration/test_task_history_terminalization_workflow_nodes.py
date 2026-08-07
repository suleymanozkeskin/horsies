"""The workflow-node family against real PostgreSQL.

The final six operations: pause-abandon and workflow-cancel singles with
their contract fences (including the single-only requeued-PENDING
carve-out), the id-keyed pairwise batches with ordinality-complete
answers and typed precondition raises, and the two workflow-scoped
sweeps — proven against the per-workflow-STATE matrix: a RUNNING
workflow's tasks are untouched by both sweeps, a PAUSED workflow yields
only CLAIMED tasks behind ENQUEUED|RUNNING nodes, and a CANCELLED
workflow yields only tasks behind still-ENQUEUED nodes, with the
RUNNING-node survivor left for the owned path. The workflow-cancel
kinds archive NULL terminal summaries per the M8-E ruling.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.integration.task_history_harness import (
    HistorySchema,
    create_workflow,
    insert_live_task,
    link_workflow_node,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

CLASS_KEY = 'it_wfnode'
WORKER = 'worker-node-1'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_wfnode'
)


async def history_row(connection: AsyncConnection, task_id: str) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT status, terminalization_kind, error_code, '
                'final_failed_reason, workflow_id, rerun_input_disposition '
                'FROM horsies_task_history '
                'WHERE task_id = CAST(:task_id AS uuid)'
            ),
            {'task_id': task_id},
        )
    ).one()


async def seeded_workflow_task(
    connection: AsyncConnection,
    *,
    workflow_status: str,
    node_status: str,
    task_status: str = 'CLAIMED',
    worker: str | None = WORKER,
) -> tuple[str, str]:
    task_id = await insert_live_task(
        connection,
        class_key=CLASS_KEY,
        status=task_status,
        worker=worker,
        is_workflow_task=True,
    )
    workflow_id = await create_workflow(connection, status=workflow_status)
    await link_workflow_node(
        connection, task_id, workflow_id=workflow_id, node_status=node_status
    )
    return task_id, workflow_id


class TestPauseAbandonSingle:
    @pytest.mark.asyncio
    async def test_applied_with_pause_literals_and_linkage(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, workflow_id = await seeded_workflow_task(
                connection, workflow_status='PAUSED', node_status='RUNNING'
            )
            outcome = (
                await connection.execute(
                    text(
                        'SELECT * FROM horsies_abandon_owned_node('
                        'CAST(:task_id AS uuid), :worker, NULL)'
                    ),
                    {'task_id': task_id, 'worker': WORKER},
                )
            ).one()
            assert outcome.outcome == 'APPLIED'
            row = await history_row(connection, task_id)
            assert row.terminalization_kind == 'PAUSE_ABANDON_CLAIM'
            assert row.error_code == 'TASK_CANCELLED'
            assert row.final_failed_reason == (
                'Workflow paused before task start'
            )
            assert str(row.workflow_id) == workflow_id
            assert row.rerun_input_disposition == 'NEVER_ELIGIBLE'


class TestWorkflowCancelSingle:
    @pytest.mark.asyncio
    async def test_requeued_pending_carve_out_both_ways(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _ = await seeded_workflow_task(
                connection,
                workflow_status='CANCELLED',
                node_status='CANCELLED',
                task_status='PENDING',
                worker=None,
            )
            refused = (
                await connection.execute(
                    text(
                        'SELECT * FROM horsies_cancel_owned_node('
                        'CAST(:task_id AS uuid), :worker, NULL, FALSE)'
                    ),
                    {'task_id': task_id, 'worker': WORKER},
                )
            ).one()
            # Without the carve-out the ownership fence misses, and the
            # one-owner classifier answers claim-first: a requeued task's
            # claim is gone, and the observed status carries the rest.
            assert refused.outcome == 'LOST_CLAIM'
            assert refused.observed_status == 'PENDING'
            accepted = (
                await connection.execute(
                    text(
                        'SELECT * FROM horsies_cancel_owned_node('
                        'CAST(:task_id AS uuid), :worker, NULL, TRUE)'
                    ),
                    {'task_id': task_id, 'worker': WORKER},
                )
            ).one()
            assert accepted.outcome == 'APPLIED'
            row = await history_row(connection, task_id)
            assert row.status == 'CANCELLED'
            assert row.error_code is None
            assert row.final_failed_reason is None


class TestIdKeyedBatch:
    @pytest.mark.asyncio
    async def test_every_input_answers_at_its_ordinality(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            applied_id, _ = await seeded_workflow_task(
                connection, workflow_status='PAUSED', node_status='RUNNING'
            )
            foreign_id, _ = await seeded_workflow_task(
                connection,
                workflow_status='PAUSED',
                node_status='RUNNING',
                worker='worker-other',
            )
            absent_id = str(uuid4())
            outcomes = list(
                (
                    await connection.execute(
                        text(
                            'SELECT * FROM horsies_abandon_owned_nodes('
                            'ARRAY[CAST(:a AS uuid), CAST(:b AS uuid), '
                            'CAST(:c AS uuid)], '
                            'ARRAY[NULL, NULL, NULL]::timestamptz[], '
                            ':worker)'
                        ),
                        {
                            'a': applied_id,
                            'b': foreign_id,
                            'c': absent_id,
                            'worker': WORKER,
                        },
                    )
                ).all()
            )
            by_ordinality = {row.ordinality: row for row in outcomes}
            assert set(by_ordinality) == {1, 2, 3}
            assert by_ordinality[1].outcome == 'APPLIED'
            assert by_ordinality[1].terminalization_kind == (
                'PAUSE_ABANDON_CLAIM_BATCH'
            )
            assert by_ordinality[2].outcome == 'LOST_CLAIM'
            assert by_ordinality[2].observed_worker_id == 'worker-other'
            assert by_ordinality[3].outcome == 'TASK_ABSENT'
            row = await history_row(connection, applied_id)
            assert row.error_code == 'TASK_CANCELLED'

    @pytest.mark.asyncio
    async def test_precondition_raises_are_typed(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.connect() as connection:
            with pytest.raises(DBAPIError, match='lengths differ'):
                await connection.execute(
                    text(
                        'SELECT * FROM horsies_cancel_owned_nodes('
                        'ARRAY[CAST(:a AS uuid)], '
                        'ARRAY[]::timestamptz[], :worker)'
                    ),
                    {'a': str(uuid4()), 'worker': WORKER},
                )
        async with terminalization_schema.engine.connect() as connection:
            duplicate = str(uuid4())
            with pytest.raises(DBAPIError, match='must be distinct'):
                await connection.execute(
                    text(
                        'SELECT * FROM horsies_cancel_owned_nodes('
                        'ARRAY[CAST(:a AS uuid), CAST(:a AS uuid)], '
                        'ARRAY[NULL, NULL]::timestamptz[], :worker)'
                    ),
                    {'a': duplicate, 'worker': WORKER},
                )


class TestWorkflowStateMatrix:
    @pytest.mark.asyncio
    async def test_running_workflow_is_untouched_by_both_sweeps(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, workflow_id = await seeded_workflow_task(
                connection, workflow_status='RUNNING', node_status='ENQUEUED'
            )
            for sweep in (
                'horsies_abandon_nodes_of_paused_workflows',
                'horsies_cancel_nodes_of_cancelled_workflow',
            ):
                outcomes = (
                    await connection.execute(
                        text(
                            f'SELECT * FROM {sweep}('
                            'ARRAY[CAST(:workflow_id AS uuid)])'
                        ),
                        {'workflow_id': workflow_id},
                    )
                ).all()
                assert outcomes == []
            survivor = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert survivor == 1

    @pytest.mark.asyncio
    async def test_paused_sweep_takes_claimed_behind_runnable_nodes_only(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            workflow_id = await create_workflow(connection, status='PAUSED')
            swept = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                is_workflow_task=True,
            )
            await link_workflow_node(
                connection, swept,
                workflow_id=workflow_id, node_status='RUNNING',
            )
            pending_survivor = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='PENDING',
                worker=None,
                is_workflow_task=True,
            )
            await link_workflow_node(
                connection, pending_survivor,
                workflow_id=workflow_id, node_status='ENQUEUED', task_index=1,
            )
            outcomes = list(
                (
                    await connection.execute(
                        text(
                            'SELECT * FROM '
                            'horsies_abandon_nodes_of_paused_workflows('
                            'ARRAY[CAST(:workflow_id AS uuid)])'
                        ),
                        {'workflow_id': workflow_id},
                    )
                ).all()
            )
            assert {str(row.task_id) for row in outcomes} == {swept}
            assert outcomes[0].terminalization_kind == (
                'PAUSE_ABANDON_WORKFLOW'
            )
            row = await history_row(connection, swept)
            assert row.error_code == 'TASK_CANCELLED'
            survivors = {
                str(r.id)
                for r in (
                    await connection.execute(
                        text('SELECT id FROM horsies_tasks')
                    )
                ).all()
            }
            assert survivors == {pending_survivor}

    @pytest.mark.asyncio
    async def test_paused_sweep_judges_from_the_capture_not_a_reread(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """The sweep-vs-workflow-progress race lives here: a node moving
        ENQUEUED -> RUNNING mid-judgment. Discovery captures node state in
        its one snapshot and no later stage re-reads it, so the outcome is
        consistent with the captured read — the concurrent transition,
        committed after discovery, cannot flip the judgment mid-flight."""
        engine = terminalization_schema.engine
        async with engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            workflow_id = await create_workflow(connection, status='PAUSED')
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                is_workflow_task=True,
            )
            node_id = await link_workflow_node(
                connection, task_id,
                workflow_id=workflow_id, node_status='ENQUEUED',
            )
        async with engine.connect() as progress:
            await progress.execute(text('BEGIN'))
            # The concurrent workflow-progress writer: node ENQUEUED ->
            # RUNNING, uncommitted while the sweep judges. It locks only
            # the node row; the sweep locks only task rows, so neither
            # waits on the other.
            await progress.execute(
                text(
                    "UPDATE horsies_workflow_tasks SET status = 'RUNNING' "
                    'WHERE id = CAST(:node_id AS uuid)'
                ),
                {'node_id': node_id},
            )
            async with engine.begin() as connection:
                outcomes = list(
                    (
                        await connection.execute(
                            text(
                                'SELECT * FROM '
                                'horsies_abandon_nodes_of_paused_workflows('
                                'ARRAY[CAST(:workflow_id AS uuid)])'
                            ),
                            {'workflow_id': workflow_id},
                        )
                    ).all()
                )
                # Judged from the captured committed state (ENQUEUED —
                # eligible), and the concurrent transition to RUNNING is
                # ALSO eligible: the judgment cannot differ whichever side
                # of the transition the capture saw, and the swept outcome
                # proves no blocked re-read occurred.
                assert {str(row.task_id) for row in outcomes} == {task_id}
            await progress.execute(text('ROLLBACK'))
        async with engine.begin() as connection:
            moved = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert moved == 1

    @pytest.mark.asyncio
    async def test_cancelled_sweep_takes_enqueued_links_only(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            workflow_id = await create_workflow(connection, status='CANCELLED')
            swept = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='PENDING',
                worker=None,
                is_workflow_task=True,
            )
            await link_workflow_node(
                connection, swept,
                workflow_id=workflow_id, node_status='ENQUEUED',
            )
            running_survivor = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                is_workflow_task=True,
            )
            await link_workflow_node(
                connection, running_survivor,
                workflow_id=workflow_id, node_status='RUNNING', task_index=1,
            )
            outcomes = list(
                (
                    await connection.execute(
                        text(
                            'SELECT * FROM '
                            'horsies_cancel_nodes_of_cancelled_workflow('
                            'ARRAY[CAST(:workflow_id AS uuid)])'
                        ),
                        {'workflow_id': workflow_id},
                    )
                ).all()
            )
            assert {str(row.task_id) for row in outcomes} == {swept}
            row = await history_row(connection, swept)
            assert row.terminalization_kind == 'WORKFLOW_CANCEL_WORKFLOW'
            assert row.error_code is None
            assert row.final_failed_reason is None
            survivors = {
                str(r.id)
                for r in (
                    await connection.execute(
                        text('SELECT id FROM horsies_tasks')
                    )
                ).all()
            }
            assert survivors == {running_survivor}

    @pytest.mark.asyncio
    async def test_sweeps_respect_the_workflow_scope(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            named_workflow = await create_workflow(connection, status='PAUSED')
            named_task = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                is_workflow_task=True,
            )
            await link_workflow_node(
                connection, named_task,
                workflow_id=named_workflow, node_status='RUNNING',
            )
            other_task, _ = await seeded_workflow_task(
                connection, workflow_status='PAUSED', node_status='RUNNING'
            )
            outcomes = list(
                (
                    await connection.execute(
                        text(
                            'SELECT * FROM '
                            'horsies_abandon_nodes_of_paused_workflows('
                            'ARRAY[CAST(:workflow_id AS uuid)])'
                        ),
                        {'workflow_id': named_workflow},
                    )
                ).all()
            )
            assert {str(row.task_id) for row in outcomes} == {named_task}
            other_alive = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': other_task},
                )
            ).scalar_one()
            assert other_alive == 1
