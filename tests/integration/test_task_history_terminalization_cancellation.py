"""The cancellation family against real PostgreSQL.

Administrative cancellation proves the ratified projection: the result
domain is null and the operation's literals are database-owned. The
orphan single proves the
kind-aware linkage lookup in both directions — no link and terminal-only
link — plus the fence-before-guard classification and the fail-closed
distinct-workflows guard. The sweep proves the builder-rendered batch
moving only orphans among mixed candidates.
"""

from __future__ import annotations

from typing import Any

import pytest
from psycopg.errors import UniqueViolation
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
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

CLASS_KEY = 'standard_30d'
WORKER = 'worker-cancel-1'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_cancel'
)


async def cancel_admin(
    connection: AsyncConnection,
    task_id: str,
    *,
    include_running: bool,
) -> Any:
    """The administrative cancel (``horsies_cancel_task``); a RUNNING row
    only with ``include_running``."""
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_cancel_task('
                'CAST(:task_id AS uuid), CAST(:include_running AS BOOLEAN))'
            ),
            {'task_id': task_id, 'include_running': include_running},
        )
    ).one()


async def cancel_orphan(
    connection: AsyncConnection,
    task_id: str,
    *,
    worker: str = WORKER,
) -> Any:
    """Cancel with the claim generation that the stored row holds."""
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_cancel_owned_orphan('
                'CAST(:task_id AS uuid), :worker, '
                '(SELECT claimed_at FROM horsies_tasks '
                'WHERE id = CAST(:task_id AS uuid)))'
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
    node_workflow = (
        workflow_id
        if workflow_id is not None
        else await create_workflow(connection, status='RUNNING')
    )
    _ = await link_workflow_node(
        connection,
        task_id,
        workflow_id=node_workflow,
        node_status=status,
        task_index=task_index,
    )
    return node_workflow


class TestAdministrativeCancel:
    @pytest.mark.asyncio
    async def test_no_prior_output_archives_all_null_result_domain(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            outcome = await cancel_admin(connection, task_id, include_running=True)
            assert outcome.outcome == 'APPLIED'
            assert outcome.terminalization_kind == 'CANCEL_ADMIN'
            row = (
                await connection.execute(
                    text(
                        'SELECT status, terminalization_kind, '
                        'result_payload, result_digest, error_code, '
                        'final_failed_reason '
                        'FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
            assert row.status == 'CANCELLED'
            assert row.terminalization_kind == 'CANCEL_ADMIN'
            assert row.result_payload is None
            assert row.result_digest is None
            assert row.error_code == 'TASK_CANCELLED'
            assert row.final_failed_reason == 'Cancelled via monitoring API'

    @pytest.mark.asyncio
    async def test_a_running_task_is_cancelled_only_when_the_caller_permits_it(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """Whether a RUNNING row may be cancelled is the caller's choice."""
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            refused = await cancel_admin(connection, task_id, include_running=False)
            assert refused.outcome == 'SOURCE_STATE_CONFLICT'
            applied = await cancel_admin(connection, task_id, include_running=True)
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
            refused = await cancel_admin(connection, task_id, include_running=True)
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
    async def test_the_database_refuses_a_second_node_for_one_task(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """A task backs at most one node: the unique index on the node's
        task_id refuses the second link, so no cancel can find a task
        linked to two workflows."""
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
            with pytest.raises(IntegrityError) as refused:
                async with connection.begin_nested():
                    await link_node(
                        connection, task_id, status='FAILED', task_index=1
                    )
            cause = refused.value.orig
            assert isinstance(cause, UniqueViolation)
            assert cause.diag.constraint_name == (
                'ix_horsies_workflow_tasks_task_id'
            )


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
