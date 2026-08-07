"""The cancellation family against real PostgreSQL.

Administrative cancellation proves the ratified projection: the canonical
result is null, the pre-cancellation output survives only as the
separately named prior payload with the digest computed over it, and the
operation's literals are database-owned. The orphan single proves the
kind-aware linkage lookup in both directions — no link and terminal-only
link — plus the fence-before-guard classification and the fail-closed
distinct-workflows guard. The sweep proves the builder-rendered batch
moving only orphans among mixed candidates.
"""

from __future__ import annotations

from hashlib import sha256
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.integration.task_history_harness import (
    HistorySchema,
    insert_live_task,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

CLASS_KEY = 'it_cancel'
WORKER = 'worker-cancel-1'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_cancel'
)


async def cancel_admin(
    connection: AsyncConnection,
    task_id: str,
    *,
    permitted: tuple[str, ...] = ('PENDING', 'CLAIMED', 'RUNNING'),
) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_cancel_locked_task('
                'CAST(:task_id AS uuid), :permitted)'
            ),
            {'task_id': task_id, 'permitted': list(permitted)},
        )
    ).one()


async def cancel_orphan(
    connection: AsyncConnection,
    task_id: str,
    *,
    worker: str = WORKER,
) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_cancel_owned_orphan('
                'CAST(:task_id AS uuid), :worker, NULL)'
            ),
            {'task_id': task_id, 'worker': worker},
        )
    ).one()


async def link_node(
    connection: AsyncConnection,
    task_id: str,
    *,
    status: str,
    workflow_id: str | None = None,
    task_index: int = 0,
) -> str:
    node_workflow = workflow_id if workflow_id is not None else str(uuid4())
    await connection.execute(
        text(
            'INSERT INTO horsies_workflow_tasks '
            '(id, workflow_id, task_id, task_index, status) VALUES '
            '(CAST(:node_id AS uuid), CAST(:workflow_id AS uuid), '
            'CAST(:task_id AS uuid), :task_index, :status)'
        ),
        {
            'node_id': str(uuid4()),
            'workflow_id': node_workflow,
            'task_id': task_id,
            'task_index': task_index,
            'status': status,
        },
    )
    return node_workflow


class TestAdministrativeCancel:
    @pytest.mark.asyncio
    async def test_prior_result_swap_archives_the_ratified_projection(
        self, terminalization_schema: HistorySchema
    ) -> None:
        prior = '{"ok":"the pre-cancellation output"}'
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                live_result=prior,
            )
            outcome = await cancel_admin(connection, task_id)
            assert outcome.outcome == 'APPLIED'
            assert outcome.terminalization_kind == 'CANCEL_ADMIN'
            row = (
                await connection.execute(
                    text(
                        'SELECT status, terminalization_kind, '
                        'result_payload, '
                        'prior_result_payload, result_digest, error_code, '
                        'final_failed_reason '
                        'FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
            assert row.status == 'CANCELLED'
            # The frozen CHECK pair: a prior payload exists only beside the
            # administrative kind, and the digest column serves the prior.
            assert row.terminalization_kind == 'CANCEL_ADMIN'
            assert row.result_payload is None
            assert bytes(row.prior_result_payload) == prior.encode()
            assert bytes(row.result_digest) == sha256(prior.encode()).digest()
            assert row.error_code == 'TASK_CANCELLED'
            assert row.final_failed_reason == 'Cancelled via monitoring API'

    @pytest.mark.asyncio
    async def test_no_prior_output_archives_all_null_result_domain(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            await cancel_admin(connection, task_id)
            row = (
                await connection.execute(
                    text(
                        'SELECT result_payload, prior_result_payload, '
                        'result_digest FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
            assert row.result_payload is None
            assert row.prior_result_payload is None
            assert row.result_digest is None

    @pytest.mark.asyncio
    async def test_permitted_source_set_is_the_callers(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            refused = await cancel_admin(
                connection, task_id, permitted=('PENDING', 'CLAIMED')
            )
            assert refused.outcome == 'SOURCE_STATE_CONFLICT'
            applied = await cancel_admin(connection, task_id)
            assert applied.outcome == 'APPLIED'

    @pytest.mark.asyncio
    async def test_workflow_tasks_are_refused(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                is_workflow_task=True,
            )
            refused = await cancel_admin(connection, task_id)
            assert refused.outcome == 'SOURCE_STATE_CONFLICT'
            live = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert live == 1


class TestOrphanSingle:
    @pytest.mark.asyncio
    async def test_no_linkage_cancels_with_null_workflow_id(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                is_workflow_task=True,
            )
            outcome = await cancel_orphan(connection, task_id)
            assert outcome.outcome == 'APPLIED'
            row = (
                await connection.execute(
                    text(
                        'SELECT workflow_id, is_workflow_task, '
                        'rerun_input_disposition '
                        'FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
            assert row.workflow_id is None
            assert row.is_workflow_task is True
            assert row.rerun_input_disposition == 'NEVER_ELIGIBLE'
            pending = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM '
                        'horsies_workflow_phase2_pending'
                    )
                )
            ).scalar_one()
            assert pending == 0

    @pytest.mark.asyncio
    async def test_terminal_only_link_still_orphans_and_archives_linkage(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                is_workflow_task=True,
            )
            workflow_id = await link_node(
                connection, task_id, status='COMPLETED'
            )
            outcome = await cancel_orphan(connection, task_id)
            assert outcome.outcome == 'APPLIED'
            archived = (
                await connection.execute(
                    text(
                        'SELECT workflow_id FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert str(archived) == workflow_id

    @pytest.mark.asyncio
    async def test_runnable_link_refuses_with_the_link_state(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                is_workflow_task=True,
            )
            await link_node(connection, task_id, status='RUNNING')
            refusal = await cancel_orphan(connection, task_id)
            assert refusal.outcome == 'SOURCE_STATE_CONFLICT'
            assert refusal.guard_kind == 'WORKFLOW_LINK_STATE'
            assert refusal.observed_guard == {'node_status': 'RUNNING'}

    @pytest.mark.asyncio
    async def test_fence_is_classified_before_the_guard(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker='worker-other',
                is_workflow_task=True,
            )
            await link_node(connection, task_id, status='RUNNING')
            outcome = await cancel_orphan(connection, task_id)
            assert outcome.outcome == 'LOST_CLAIM'
            assert outcome.guard_kind is None

    @pytest.mark.asyncio
    async def test_links_to_distinct_workflows_fail_closed(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                is_workflow_task=True,
            )
            await link_node(connection, task_id, status='COMPLETED')
            await link_node(
                connection, task_id, status='CANCELLED', task_index=1
            )
            with pytest.raises(DBAPIError, match='multiple workflows'):
                await cancel_orphan(connection, task_id)


class TestOrphanSweep:
    @pytest.mark.asyncio
    async def test_sweep_moves_only_orphans_among_mixed_candidates(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            claimed_orphan = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                is_workflow_task=True,
            )
            pending_orphan = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='PENDING',
                worker=None,
                is_workflow_task=True,
            )
            linked = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                status='CLAIMED',
                worker=WORKER,
                is_workflow_task=True,
            )
            await link_node(connection, linked, status='RUNNING')
            plain = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )

            outcomes = list(
                (
                    await connection.execute(
                        text(
                            'SELECT * FROM horsies_cancel_orphaned_tasks(10)'
                        )
                    )
                ).all()
            )
            moved = {str(row.task_id) for row in outcomes}
            assert moved == {claimed_orphan, pending_orphan}
            assert all(
                row.terminalization_kind == 'CANCEL_ORPHAN_SWEEP'
                for row in outcomes
            )
            observed = {
                str(row.task_id): row.observed_status for row in outcomes
            }
            assert observed[claimed_orphan] == 'CLAIMED'
            assert observed[pending_orphan] == 'PENDING'

            survivors = {
                str(row.id)
                for row in (
                    await connection.execute(
                        text('SELECT id FROM horsies_tasks')
                    )
                ).all()
            }
            assert survivors == {linked, plain}
            dispositions = {
                row.rerun_input_disposition
                for row in (
                    await connection.execute(
                        text(
                            'SELECT rerun_input_disposition '
                            'FROM horsies_task_history'
                        )
                    )
                ).all()
            }
            assert dispositions == {'NEVER_ELIGIBLE'}
