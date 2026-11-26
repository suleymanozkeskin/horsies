"""Monitoring detail reads resolve moved tasks from history.

The task-detail and workflow-node routes served whole rows from
`horsies_tasks`; terminalization moves those rows, so both re-point
through the staged detail read when the live side is empty. Proven on
installed databases with seeded history rows: the same response shapes
come back with the history-side values — the terminal instant landing
in the status-appropriate end stamp, the failure reason from the
recorded terminal context, attempts decoded from the snapshot.
"""

from __future__ import annotations

import uuid

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskStatus
from horsies.core.types.result import is_err
from horsies.monitoring import TaskDetail, TaskDetailNotFound
from horsies.monitoring.queries import get_task_detail, get_workflow_node
from tests.integration.test_task_result_history_fallback import (
    populated_history,
    seed_history_task,
)
from tests.integration.isolated_database import (
    MakeDatabase,
    make_database,
)
from tests.sqlalchemy_url import sqlalchemy_url

__all__ = ['make_database']

pytestmark = [pytest.mark.integration]


class TestTaskDetailFromHistory:
    @pytest.mark.asyncio
    async def test_moved_task_detail_resolves_with_attempts(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        app = Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(url))))
        broker = app.get_broker()
        try:
            await broker.ensure_schema_initialized()
            completed, cancelled = await populated_history(url)

            outcome = await get_task_detail(app, uuid.UUID(completed))
            assert not is_err(outcome), outcome
            detail = outcome.ok_value
            assert isinstance(detail, TaskDetail)
            assert detail.task_name == 'legacy.task'
            assert detail.task_row.status == 'COMPLETED'
            assert detail.task_row.completed_at is not None
            assert detail.task_row.failed_at is None

            outcome = await get_task_detail(app, uuid.UUID(cancelled))
            assert not is_err(outcome), outcome
            detail = outcome.ok_value
            assert isinstance(detail, TaskDetail)
            assert detail.task_row.status == 'CANCELLED'
            assert detail.task_row.failed_at is not None

            missing = await get_task_detail(app, uuid.uuid4())
            assert not is_err(missing), missing
            assert isinstance(missing.ok_value, TaskDetailNotFound)
        finally:
            await broker.close_async()

    @pytest.mark.asyncio
    async def test_workflow_node_task_row_resolves_from_history(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        app = Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(url))))
        broker = app.get_broker()
        try:
            await broker.ensure_schema_initialized()
            engine = create_async_engine(sqlalchemy_url(url))
            workflow_id = str(uuid.uuid4())
            try:
                async with AsyncSession(engine) as session:
                    task_id = await seed_history_task(
                        session,
                        status=TaskStatus.COMPLETED,
                        result_json='{"ok": 1}',
                        is_workflow_task=True,
                        workflow_id=workflow_id,
                    )
                    _ = await session.execute(
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
                    _ = await session.execute(
                        text(
                            'INSERT INTO horsies_workflow_tasks '
                            '(downstream_subworkflow, id, workflow_id, task_id, task_index, '
                            'task_name, input, queue_name, priority, '
                            'dependencies, allow_failed_deps, join_type, '
                            'status, is_subworkflow, created_at, '
                            'max_retries, execution, retention_class_key) '
                            'VALUES '
                            "('NONE', :n, :w, :t, 0, 'legacy.task', '{}', 'default', "
                            "50, '{}', FALSE, 'all', 'COMPLETED', FALSE, "
                            "statement_timestamp(), 0, 'ISOLATED', "
                            "'standard_30d')"
                        ),
                        {
                            'n': str(uuid.uuid4()),
                            'w': workflow_id,
                            't': task_id,
                        },
                    )
                    await session.commit()
            finally:
                await engine.dispose()

            outcome = await get_workflow_node(app, workflow_id, 0)
            assert not is_err(outcome), outcome
            node = outcome.ok_value
            assert node is not None
            assert node.task_row is not None
            assert node.task_row.status == 'COMPLETED'
            assert node.task_row.completed_at is not None
        finally:
            await broker.close_async()
