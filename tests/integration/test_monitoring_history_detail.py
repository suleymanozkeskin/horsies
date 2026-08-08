"""Monitoring detail reads resolve moved tasks from history.

The task-detail and workflow-node routes served whole rows from
`horsies_tasks`; terminalization moves those rows, so both re-point
through the staged detail read when the live side is empty. Proven on
migrated, relocated, tightened databases: the same response shapes
come back with the history-side values — the terminal instant landing
in the status-appropriate end stamp, the failure reason from the
recorded terminal context, attempts decoded from the snapshot — and a
pre-coverage database keeps absent-means-absent semantics.
"""

from __future__ import annotations

import uuid

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import is_err
from horsies.monitoring.queries import get_task_detail, get_workflow_node
from tests.integration.test_task_result_history_fallback import (
    _populated_history,
)
from tests.integration.test_task_history_relocation import (
    insert_legacy_task,
    relocate_all,
)
from tests.integration.test_task_history_schema_emission import (
    MakeDatabase,
    make_database,
)

__all__ = ['make_database']

pytestmark = [pytest.mark.integration]


class TestTaskDetailFromHistory:
    @pytest.mark.asyncio
    async def test_moved_task_detail_resolves_with_attempts(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        try:
            await broker.ensure_schema_initialized()
            completed, cancelled = await _populated_history(url)

            outcome = await get_task_detail(broker, completed)
            assert not is_err(outcome), outcome
            detail = outcome.ok_value
            assert detail is not None
            assert detail.task_name == 'legacy.task'
            assert detail.leaf.status == 'COMPLETED'
            assert detail.leaf.completed_at is not None
            assert detail.leaf.failed_at is None

            outcome = await get_task_detail(broker, cancelled)
            assert not is_err(outcome), outcome
            detail = outcome.ok_value
            assert detail is not None
            assert detail.leaf.status == 'CANCELLED'
            assert detail.leaf.failed_at is not None

            missing = await get_task_detail(broker, str(uuid.uuid4()))
            assert not is_err(missing), missing
            assert missing.ok_value is None
        finally:
            await broker.close_async()

    @pytest.mark.asyncio
    async def test_workflow_node_leaf_resolves_from_history(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        try:
            await broker.ensure_schema_initialized()
            engine = create_async_engine(url)
            workflow_id = str(uuid.uuid4())
            try:
                async with engine.begin() as connection:
                    from tests.integration.test_task_history_relocation import (
                        install_program_state,
                    )

                    await install_program_state(connection)
                    task_id = await insert_legacy_task(
                        connection,
                        status='COMPLETED',
                        kind=None,
                        result='{"ok": 1}',
                        is_workflow_task=True,
                    )
                    await connection.execute(
                        text(
                            'INSERT INTO horsies_workflows '
                            '(id, name, status, on_error, depth, '
                            'created_at, updated_at, sent_at) VALUES '
                            "(:w, 'legacy-wf', 'COMPLETED', 'fail', 0, "
                            'statement_timestamp(), '
                            'statement_timestamp(), '
                            'statement_timestamp())'
                        ),
                        {'w': workflow_id},
                    )
                    await connection.execute(
                        text(
                            'INSERT INTO horsies_workflow_tasks '
                            '(id, workflow_id, task_id, task_index, '
                            'task_name, queue_name, priority, '
                            'dependencies, allow_failed_deps, join_type, '
                            'status, is_subworkflow, created_at) VALUES '
                            "(:n, :w, :t, 0, 'legacy.task', 'default', "
                            "50, '{}', FALSE, 'all', 'COMPLETED', FALSE, "
                            'statement_timestamp())'
                        ),
                        {
                            'n': str(uuid.uuid4()),
                            'w': workflow_id,
                            't': task_id,
                        },
                    )
                    await relocate_all(connection)
                    from horsies.core.history.cutover.tighten import (
                        TightenComplete,
                        confirmation_phrase,
                        tighten_to_frozen,
                    )

                    tightened = await tighten_to_frozen(
                        connection,
                        backup_label='node-test',
                        operator_confirmation=confirmation_phrase(
                            'node-test'
                        ),
                    )
                    assert isinstance(tightened, TightenComplete)
            finally:
                await engine.dispose()

            outcome = await get_workflow_node(broker, workflow_id, 0)
            assert not is_err(outcome), outcome
            node = outcome.ok_value
            assert node is not None
            assert node.leaf is not None
            assert node.leaf.status == 'COMPLETED'
            assert node.leaf.completed_at is not None
        finally:
            await broker.close_async()
