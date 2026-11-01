"""The workflow recovery limit against real raising rows.

Each test enters from the pass that the reaper calls and asserts on stored
rows. No test patches the function under test. Three stored rows raise on
every pass:

- ``BAD_GOOD_UNTIL`` on a node: the INSERT of the task refuses it (DataError).
- ``BAD_POLICY`` on a workflow: the success evaluation raises AttributeError.
- An output index with no node row: the final result raises AssertionError.

Each test has its own database, because a global pass reads every workflow.
"""

# pyright: reportPrivateUsage=false, reportUnusedFunction=false

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, PendingRollbackError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import loads_json
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import TaskNode, WorkflowDefinition
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.types.result import is_err
from horsies.core.worker.sql import (
    DELETE_EXPIRED_WORKFLOWS_SQL,
    DELETE_ORPHANED_RECOVERY_FAILURES_SQL,
)
from horsies.core.workflows.engine import (
    on_workflow_task_complete,
    start_workflow_async,
)
from horsies.core.workflows.phase2_recovery import (
    Phase2RecoverySummary,
    _RaisedRowNotRecorded,
    _record_raised_row,
    drive_phase2_recovery,
)
from horsies.core.workflows.recovery import (
    GLOBAL_SCAN_ROW_CAP,
    RecoveryPassFailure,
    RecoveryReport,
    recover_stuck_workflow_tree,
    recover_stuck_workflows_global,
)
from horsies.core.workflows.recovery_limit import (
    RecoveryCase,
    RecoveryLimitOutcome,
    RecoveryRaise,
    RecoveryRaiseClass,
    apply_recovery_limit,
    classify_recovery_raise,
)
from tests.broker_resources_support import current_resources
from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import isolated_test_database

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

LIMIT = 3
TASK_NAME = 'limit_task'
BAD_GOOD_UNTIL = '{"good_until":"not-a-date"}'
BAD_POLICY = '{"cases":[1]}'
FIRST_FAILURE = '{"error_code":"TASK_EXCEPTION","message":"first failure"}'
OK_ENVELOPE = '{"__h_task_result__":true,"ok":1,"err":null}'

INSERT_WORKFLOW_SQL = text("""
    INSERT INTO horsies_workflows (
        id, name, status, on_error, output_task_index, success_policy,
        error, result, definition_key, parent_workflow_id, parent_task_index,
        depth, root_workflow_id, sent_at, created_at, started_at, updated_at,
        completed_at
    ) VALUES (
        :id, :name, :status, :on_error, :output_index,
        CAST(:policy AS jsonb), :error, :result, :name, :parent, :parent_index,
        :depth, :root, NOW(), NOW(), NOW(), NOW(), :completed_at
    )
""")

INSERT_NODE_SQL = text("""
    INSERT INTO horsies_workflow_tasks (
        id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
        queue_name, priority, dependencies, allow_failed_deps, join_type,
        task_options, status, is_subworkflow, sub_workflow_id,
        sub_definition_key, task_id, created_at
    ) VALUES (
        :id, :workflow_id, :index, :node_id, :task_name, '[]', '{}',
        'default', 100, :dependencies, FALSE, 'all',
        :options, :status, :is_subworkflow, :sub_workflow_id,
        :sub_definition_key, :task_id, NOW()
    )
""")


@dataclass(frozen=True, slots=True)
class Harness:
    """One isolated database with a broker and an app on it."""

    app: Horsies
    broker: PostgresBroker
    url: str

    @property
    def sessions(self) -> async_sessionmaker[AsyncSession]:
        return current_resources(self.broker).session_factory


@pytest_asyncio.fixture
async def harness() -> AsyncIterator[Harness]:
    async with isolated_test_database(DB_URL) as url:
        app = Horsies(
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=PostgresConfig(database_url=SecretStr(url)),
            ),
        )
        broker = app.get_broker()
        initialized = await broker.ensure_schema_initialized()
        assert initialized.is_ok(), initialized
        from tests.integration.history_seeding import ensure_history_seedable

        async with current_resources(broker).engine.begin() as connection:
            await ensure_history_seedable(connection)

        @app.task(task_name=TASK_NAME)
        def limit_task(*, value: int = 1) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        yield Harness(app=app, broker=broker, url=url)
        await broker.close_async()


async def _seed_workflow(
    harness: Harness,
    *,
    status: str = 'RUNNING',
    on_error: str = 'fail',
    policy: str | None = None,
    error: str | None = None,
    result: str | None = None,
    output_index: int | None = None,
    parent: str | None = None,
    parent_index: int | None = None,
) -> str:
    workflow_id = str(uuid.uuid4())
    async with harness.sessions() as session:
        await session.execute(
            INSERT_WORKFLOW_SQL,
            {
                'id': workflow_id,
                'name': f'limit_{workflow_id[:8]}',
                'status': status,
                'on_error': on_error,
                'output_index': output_index,
                'policy': policy,
                'error': error,
                'result': result,
                'parent': parent,
                'parent_index': parent_index,
                'depth': 0 if parent is None else 1,
                'root': parent or workflow_id,
                'completed_at': (
                    None if status in ('RUNNING', 'PAUSED') else datetime.now(UTC)
                ),
            },
        )
        await session.commit()
    return workflow_id


async def _seed_node(
    harness: Harness,
    workflow_id: str,
    index: int,
    *,
    status: str,
    dependencies: tuple[int, ...] = (),
    options: str | None = None,
    sub_workflow_id: str | None = None,
    sub_definition_key: str | None = None,
    is_subworkflow: bool = False,
    task_id: str | None = None,
) -> None:
    async with harness.sessions() as session:
        await session.execute(
            INSERT_NODE_SQL,
            {
                'id': str(uuid.uuid4()),
                'workflow_id': workflow_id,
                'index': index,
                'node_id': f'node_{index}',
                'task_name': TASK_NAME,
                'dependencies': list(dependencies),
                'options': options,
                'status': status,
                'is_subworkflow': is_subworkflow or sub_workflow_id is not None,
                'sub_workflow_id': sub_workflow_id,
                'sub_definition_key': sub_definition_key,
                'task_id': task_id,
            },
        )
        await session.commit()


async def _execute(harness: Harness, statement: str, **params: Any) -> None:
    async with harness.sessions() as session:
        await session.execute(text(statement), params)
        await session.commit()


async def _run_pass(harness: Harness) -> RecoveryReport:
    return await recover_stuck_workflows_global(
        harness.sessions,
        harness.broker,
        failure_limit=LIMIT,
    )


async def _run_passes(harness: Harness, count: int) -> list[RecoveryReport]:
    return [await _run_pass(harness) for _ in range(count)]


async def _run_phase2(harness: Harness) -> Phase2RecoverySummary:
    return await drive_phase2_recovery(
        harness.sessions,
        harness.broker,
        grace_ms=0,
        max_rows=GLOBAL_SCAN_ROW_CAP,
        quarantine_after_attempts=LIMIT,
    )


async def _workflow_status(harness: Harness, workflow_id: str) -> str:
    async with harness.sessions() as session:
        return str(
            (
                await session.execute(
                    text('SELECT status FROM horsies_workflows WHERE id = :id'),
                    {'id': workflow_id},
                )
            ).scalar_one()
        )


async def _workflow_error(harness: Harness, workflow_id: str) -> str | None:
    async with harness.sessions() as session:
        return (
            await session.execute(
                text('SELECT error FROM horsies_workflows WHERE id = :id'),
                {'id': workflow_id},
            )
        ).scalar_one()


async def _node_status(harness: Harness, workflow_id: str, index: int) -> str:
    async with harness.sessions() as session:
        return str(
            (
                await session.execute(
                    text(
                        'SELECT status FROM horsies_workflow_tasks '
                        'WHERE workflow_id = :id AND task_index = :index',
                    ),
                    {'id': workflow_id, 'index': index},
                )
            ).scalar_one()
        )


async def _node_result(harness: Harness, workflow_id: str, index: int) -> str:
    async with harness.sessions() as session:
        return str(
            (
                await session.execute(
                    text(
                        'SELECT result FROM horsies_workflow_tasks '
                        'WHERE workflow_id = :id AND task_index = :index',
                    ),
                    {'id': workflow_id, 'index': index},
                )
            ).scalar_one()
        )


async def _failed_passes(harness: Harness, workflow_id: str) -> int | None:
    """The stored count, or None when the workflow has no row."""
    async with harness.sessions() as session:
        return (
            await session.execute(
                text(
                    'SELECT failed_passes '
                    'FROM horsies_workflow_recovery_failures '
                    'WHERE workflow_id = :id',
                ),
                {'id': workflow_id},
            )
        ).scalar_one_or_none()


async def _failure_rows(harness: Harness) -> int:
    async with harness.sessions() as session:
        return int(
            (
                await session.execute(
                    text('SELECT count(*) FROM horsies_workflow_recovery_failures'),
                )
            ).scalar_one()
        )


async def _raise_on_update(
    harness: Harness,
    *,
    table: str,
    id_column: str,
    workflow_id: str,
    sqlstate: str,
) -> None:
    """Make every update of this workflow's rows refuse with ``sqlstate``."""
    await _execute(
        harness,
        f"""
        CREATE FUNCTION limit_test_refuse() RETURNS trigger
        LANGUAGE plpgsql AS $body$
        BEGIN
            IF NEW.{id_column} = '{workflow_id}'::uuid THEN
                RAISE EXCEPTION 'refused by the test' USING ERRCODE = '{sqlstate}';
            END IF;
            RETURN NEW;
        END
        $body$
        """,
    )
    await _execute(
        harness,
        f"""
        CREATE TRIGGER limit_test_refuse BEFORE UPDATE ON {table}
        FOR EACH ROW EXECUTE FUNCTION limit_test_refuse()
        """,
    )


async def _stop_raising_on_update(harness: Harness, *, table: str) -> None:
    await _execute(harness, f'DROP TRIGGER limit_test_refuse ON {table}')
    await _execute(harness, 'DROP FUNCTION limit_test_refuse()')


async def _start_a_then_b(harness: Harness) -> tuple[str, str]:
    """Start A then B for real, make B poison, and put A's task in history.

    Returns the workflow id and the task id of A. Phase-2 recovery then owes
    the progression of A, and that progression raises on B.
    """
    from tests.integration.history_seeding import force_terminal

    task = harness.app.tasks[TASK_NAME]
    first: TaskNode[int] = TaskNode(
        fn=task, kwargs={'value': 1}, node_id='a', queue='default', priority=100,
    )
    second: TaskNode[int] = TaskNode(
        fn=task,
        kwargs={'value': 2},
        node_id='b',
        waits_for=[first],
        queue='default',
        priority=100,
    )
    spec = harness.app.workflow(
        name=f'phase2_{uuid.uuid4().hex[:8]}',
        tasks=[first, second],
        definition_key=f'tests.limit.phase2.{uuid.uuid4().hex[:8]}.v1',
    )
    started = await start_workflow_async(spec, harness.broker)
    assert not is_err(started), started
    workflow_id = started.ok_value.workflow_id
    async with harness.sessions() as session:
        await session.execute(
            text(
                'UPDATE horsies_workflow_tasks SET task_options = :options '
                'WHERE workflow_id = :id AND task_index = 1',
            ),
            {'options': BAD_GOOD_UNTIL, 'id': workflow_id},
        )
        task_id = (
            await session.execute(
                text(
                    'SELECT task_id FROM horsies_workflow_tasks '
                    'WHERE workflow_id = :id AND task_index = 0',
                ),
                {'id': workflow_id},
            )
        ).scalar_one()
        await force_terminal(
            session,
            str(task_id),
            status='COMPLETED',
            result_json=OK_ENVELOPE,
        )
        await session.commit()
    return workflow_id, str(task_id)


class PoisonOptionsChild(WorkflowDefinition[int]):
    """A child whose task options are not a JSON object.

    `_child_task_options_json` raises `SerializationError` for it on each
    child start. The test sets the attribute on its own task: this is an
    input of the code under test, not a patch of it.
    """

    name = 'limit_poison_options_child'
    definition_key = 'tests.limit.poison_options_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        @app.task(task_name='limit_child_task')
        def child_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        child_task.task_options_json = '[]'
        node: TaskNode[int] = TaskNode(
            fn=child_task,
            good_until=datetime.now(UTC) + timedelta(hours=1),
        )
        return app.workflow(name=cls.name, tasks=[node], output=node)


class TestCountsUnderTheLimit:
    async def test_each_pass_counts_once_and_changes_nothing_else(
        self, harness: Harness,
    ) -> None:
        workflow_id = await _seed_workflow(harness)
        await _seed_node(harness, workflow_id, 0, status='READY', options=BAD_GOOD_UNTIL)

        reports = await _run_passes(harness, LIMIT - 1)

        assert await _failed_passes(harness, workflow_id) == LIMIT - 1
        assert await _workflow_status(harness, workflow_id) == 'RUNNING'
        assert await _node_status(harness, workflow_id, 0) == 'READY'
        assert [r.metrics['case_1'].failures_counted for r in reports] == [1, 1]
        assert [r.metrics['case_1'].limit_reached for r in reports] == [0, 0]
        assert [r.health()['errors'] for r in reports] == [1, 1]

    async def test_two_cases_of_one_workflow_count_once_in_one_pass(
        self, harness: Harness,
    ) -> None:
        """One pass id serves every case, so a fan-out cannot reach the limit
        in one pass."""
        workflow_id = await _seed_workflow(harness)
        await _seed_node(harness, workflow_id, 0, status='COMPLETED')
        await _seed_node(
            harness, workflow_id, 1, status='PENDING',
            dependencies=(0,), options=BAD_GOOD_UNTIL,
        )
        await _seed_node(harness, workflow_id, 2, status='READY', options=BAD_GOOD_UNTIL)

        report = await _run_pass(harness)

        assert await _failed_passes(harness, workflow_id) == 1
        assert report.metrics['case_0'].errors == 1
        assert report.metrics['case_1'].errors == 1
        assert await _workflow_status(harness, workflow_id) == 'RUNNING'

    async def test_a_new_process_continues_the_count(
        self, harness: Harness,
    ) -> None:
        """The holder of the reaper gate changes, and a worker restarts: the
        stored count goes on."""
        workflow_id = await _seed_workflow(harness)
        await _seed_node(harness, workflow_id, 0, status='READY', options=BAD_GOOD_UNTIL)
        await _run_passes(harness, LIMIT - 1)

        other = PostgresBroker(
            PostgresConfig(database_url=SecretStr(harness.url)),
        )
        other.app = harness.app
        try:
            await recover_stuck_workflows_global(
                current_resources(other).session_factory,
                other,
                failure_limit=LIMIT,
            )
        finally:
            await other.close_async()

        assert await _failed_passes(harness, workflow_id) == LIMIT
        assert await _workflow_status(harness, workflow_id) == 'FAILED'

    async def test_a_repair_before_the_limit_saves_the_workflow(
        self, harness: Harness,
    ) -> None:
        workflow_id = await _seed_workflow(harness)
        await _seed_node(harness, workflow_id, 0, status='READY', options=BAD_GOOD_UNTIL)
        await _run_passes(harness, LIMIT - 1)

        await _execute(
            harness,
            'UPDATE horsies_workflow_tasks SET task_options = NULL '
            'WHERE workflow_id = :id',
            id=workflow_id,
        )
        report = await _run_pass(harness)

        assert report.recovered == 1
        assert await _node_status(harness, workflow_id, 0) == 'ENQUEUED'
        assert await _workflow_status(harness, workflow_id) == 'RUNNING'
        # No reset: the row stays as evidence.
        assert await _failed_passes(harness, workflow_id) == LIMIT - 1


class TestHealthyPath:
    async def test_a_healthy_pass_writes_nothing(self, harness: Harness) -> None:
        """A recovered candidate and an idle workflow cost no count write."""
        recovered = await _seed_workflow(harness)
        await _seed_node(harness, recovered, 0, status='READY')
        idle = await _seed_workflow(harness)
        await _seed_node(harness, idle, 0, status='RUNNING')

        report = await _run_pass(harness)

        assert report.recovered == 1
        assert await _node_status(harness, recovered, 0) == 'ENQUEUED'
        assert await _failure_rows(harness) == 0
        assert report.over_failure_limit == 0


class TestTransientRaises:
    @pytest.mark.parametrize(
        'sqlstate',
        ['55P03', '40P01', '57014', '53300'],
        ids=['lock', 'deadlock', 'statement_timeout', 'too_many_connections'],
    )
    async def test_a_transient_raise_never_counts(
        self, harness: Harness, sqlstate: str,
    ) -> None:
        workflow_id = await _seed_workflow(harness)
        await _seed_node(harness, workflow_id, 0, status='READY')
        await _raise_on_update(
            harness,
            table='horsies_workflow_tasks',
            id_column='workflow_id',
            workflow_id=workflow_id,
            sqlstate=sqlstate,
        )

        reports = await _run_passes(harness, LIMIT + 1)

        assert await _failed_passes(harness, workflow_id) is None
        assert await _workflow_status(harness, workflow_id) == 'RUNNING'
        assert all(r.metrics['case_1'].errors == 1 for r in reports)

        await _stop_raising_on_update(harness, table='horsies_workflow_tasks')
        await _run_pass(harness)
        assert await _node_status(harness, workflow_id, 0) == 'ENQUEUED'

    async def test_a_connection_loss_counts_no_candidate_of_the_pass(
        self, harness: Harness,
    ) -> None:
        """The backend dies in the first candidate. The next candidate gets
        `PendingRollbackError`, and that is not a verdict on its workflow."""
        first = await _seed_workflow(harness)
        second = await _seed_workflow(harness)
        await _seed_node(harness, first, 0, status='READY', options=BAD_GOOD_UNTIL)
        await _seed_node(harness, second, 0, status='READY', options=BAD_GOOD_UNTIL)
        await _execute(
            harness,
            """
            CREATE FUNCTION limit_test_kill() RETURNS trigger
            LANGUAGE plpgsql AS $body$
            BEGIN
                PERFORM pg_terminate_backend(pg_backend_pid());
                RETURN NEW;
            END
            $body$
            """,
        )
        await _execute(
            harness,
            """
            CREATE TRIGGER limit_test_kill BEFORE UPDATE ON horsies_workflow_tasks
            FOR EACH ROW EXECUTE FUNCTION limit_test_kill()
            """,
        )

        with pytest.raises(RecoveryPassFailure):
            await _run_pass(harness)

        await _execute(
            harness, 'DROP TRIGGER limit_test_kill ON horsies_workflow_tasks',
        )
        await _execute(harness, 'DROP FUNCTION limit_test_kill()')
        assert await _failure_rows(harness) == 0
        assert await _workflow_status(harness, first) == 'RUNNING'
        assert await _workflow_status(harness, second) == 'RUNNING'


    async def test_the_real_raise_after_a_connection_loss_is_transient(
        self, harness: Harness,
    ) -> None:
        """The classifier rule rests on what SQLAlchemy raises after a loss.
        A dead session also refuses the count write, so a pass cannot show
        this rule: only the real exception can."""
        async with harness.sessions() as session:
            pid = (
                await session.execute(text('SELECT pg_backend_pid()'))
            ).scalar_one()
            async with harness.sessions() as admin:
                await admin.execute(
                    text('SELECT pg_terminate_backend(:pid)'), {'pid': pid},
                )
                await admin.commit()
            # Each block has the shape of one candidate: a SAVEPOINT around
            # its statements.
            with pytest.raises(OperationalError):
                async with session.begin_nested():
                    await session.execute(text('SELECT 1'))
            with pytest.raises(PendingRollbackError) as raised:
                async with session.begin_nested():
                    await session.execute(text('SELECT 1'))
            await session.rollback()

        assert classify_recovery_raise(raised.value) is RecoveryRaiseClass.TRANSIENT


class TestResumePass:
    async def test_the_resume_pass_logs_and_does_not_count(
        self, harness: Harness,
    ) -> None:
        workflow_id = await _seed_workflow(harness)
        await _seed_node(harness, workflow_id, 0, status='READY', options=BAD_GOOD_UNTIL)

        async with harness.sessions() as session:
            await recover_stuck_workflow_tree(
                session, harness.broker, [workflow_id],
            )
            await session.commit()

        assert await _failed_passes(harness, workflow_id) is None
        assert await _node_status(harness, workflow_id, 0) == 'READY'


class TestLimitOnEachCase:
    async def test_case_0_pending_node(self, harness: Harness) -> None:
        workflow_id = await _seed_workflow(harness)
        await _seed_node(harness, workflow_id, 0, status='COMPLETED')
        await _seed_node(
            harness, workflow_id, 1, status='PENDING',
            dependencies=(0,), options=BAD_GOOD_UNTIL,
        )

        await _run_passes(harness, LIMIT)

        assert await _node_status(harness, workflow_id, 1) == 'FAILED'
        assert await _workflow_status(harness, workflow_id) == 'FAILED'

    async def test_case_1_ready_node_with_other_nodes(
        self, harness: Harness,
    ) -> None:
        workflow_id = await _seed_workflow(harness)
        await _seed_node(harness, workflow_id, 0, status='READY', options=BAD_GOOD_UNTIL)
        await _seed_node(
            harness, workflow_id, 1, status='PENDING', dependencies=(0,),
        )
        await _seed_node(harness, workflow_id, 2, status='COMPLETED')

        reports = await _run_passes(harness, LIMIT)

        assert await _workflow_status(harness, workflow_id) == 'FAILED'
        assert await _node_status(harness, workflow_id, 0) == 'FAILED'
        assert await _node_status(harness, workflow_id, 1) == 'SKIPPED'
        assert await _node_status(harness, workflow_id, 2) == 'COMPLETED'
        assert reports[-1].metrics['case_1'].limit_reached == 1

        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=workflow_id, broker=harness.broker,
        )
        outcome = await handle.get_async(timeout_ms=0)
        assert outcome.is_err()
        assert (
            outcome.unwrap_err().error_code
            is OperationalErrorCode.WORKFLOW_RECOVERY_LIMIT_REACHED
        )

    async def test_case_1_5_serialization_defect(self, harness: Harness) -> None:
        """A serialization defect in a child start is counted and bounded."""
        workflow_id = await _seed_workflow(harness)
        await _seed_node(
            harness, workflow_id, 0, status='READY',
            is_subworkflow=True,
            sub_definition_key=PoisonOptionsChild.definition_key,
        )

        reports = await _run_passes(harness, LIMIT)

        assert [r.metrics['case_1_5'].errors for r in reports] == [1, 1, 1]
        assert await _workflow_status(harness, workflow_id) == 'FAILED'
        assert await _node_status(harness, workflow_id, 0) == 'FAILED'
        error = await _workflow_error(harness, workflow_id)
        assert error is not None
        assert 'SerializationError' in error

    async def test_case_1_6_fails_the_parent_workflow(
        self, harness: Harness,
    ) -> None:
        parent = await _seed_workflow(harness)
        child = await _seed_workflow(
            harness,
            status='COMPLETED',
            result=OK_ENVELOPE,
            parent=parent,
            parent_index=0,
        )
        await _seed_node(harness, parent, 0, status='RUNNING', sub_workflow_id=child)
        await _seed_node(
            harness, parent, 1, status='PENDING',
            dependencies=(0,), options=BAD_GOOD_UNTIL,
        )

        await _run_passes(harness, LIMIT)

        assert await _failed_passes(harness, child) is None
        assert await _failed_passes(harness, parent) == LIMIT
        assert await _workflow_status(harness, parent) == 'FAILED'
        assert await _node_status(harness, parent, 0) == 'FAILED'
        assert await _node_status(harness, parent, 1) == 'SKIPPED'
        assert await _workflow_status(harness, child) == 'COMPLETED'

    async def test_case_2_3_keeps_a_stored_first_failure(
        self, harness: Harness,
    ) -> None:
        workflow_id = await _seed_workflow(
            harness, policy=BAD_POLICY, error=FIRST_FAILURE,
        )
        await _seed_node(harness, workflow_id, 0, status='FAILED')

        reports = await _run_passes(harness, LIMIT)

        assert await _workflow_status(harness, workflow_id) == 'FAILED'
        assert await _workflow_error(harness, workflow_id) == FIRST_FAILURE
        assert reports[-1].metrics['case_2_3'].limit_reached == 1

    async def test_case_2_3_assertion_defect(self, harness: Harness) -> None:
        """The output index has no node row: the final result raises
        AssertionError on each pass, and the limit bounds it."""
        workflow_id = await _seed_workflow(harness, output_index=7)
        await _seed_node(harness, workflow_id, 0, status='COMPLETED')

        reports = await _run_passes(harness, LIMIT)

        assert [r.metrics['case_2_3'].errors for r in reports] == [1, 1, 1]
        assert await _workflow_status(harness, workflow_id) == 'FAILED'
        error = await _workflow_error(harness, workflow_id)
        assert error is not None
        assert 'AssertionError' in error
        assert 'WORKFLOW_RECOVERY_LIMIT_REACHED' in error

    async def test_on_error_pause_still_ends_failed(
        self, harness: Harness,
    ) -> None:
        workflow_id = await _seed_workflow(harness, on_error='pause')
        await _seed_node(harness, workflow_id, 0, status='READY', options=BAD_GOOD_UNTIL)

        await _run_passes(harness, LIMIT)

        assert await _workflow_status(harness, workflow_id) == 'FAILED'


class TestLiveNodes:
    async def test_live_node_finishes_and_ends_the_workflow(
        self, harness: Harness,
    ) -> None:
        live_task = str(uuid.uuid4())
        workflow_id = await _seed_workflow(harness)
        await _seed_node(harness, workflow_id, 0, status='READY', options=BAD_GOOD_UNTIL)
        await _seed_node(harness, workflow_id, 1, status='RUNNING', task_id=live_task)

        reports = await _run_passes(harness, LIMIT)

        assert await _workflow_status(harness, workflow_id) == 'RUNNING'
        assert await _node_status(harness, workflow_id, 0) == 'FAILED'
        assert await _node_status(harness, workflow_id, 1) == 'RUNNING'
        assert reports[-1].metrics['case_1'].limit_reached == 1
        assert reports[-1].over_failure_limit == 1

        async with harness.sessions() as session:
            await on_workflow_task_complete(
                session,
                live_task,
                TaskResult(ok=1),
                harness.broker,
                task_name=TASK_NAME,
            )
            await session.commit()

        assert await _workflow_status(harness, workflow_id) == 'FAILED'
        error = await _workflow_error(harness, workflow_id)
        assert error is not None
        assert 'WORKFLOW_RECOVERY_LIMIT_REACHED' in error
        assert (await _run_pass(harness)).over_failure_limit == 0

    async def test_success_policy_met_by_a_live_node_can_complete(
        self, harness: Harness,
    ) -> None:
        """The nodes that the success case requires completed, so the
        declared policy decides."""
        live_task = str(uuid.uuid4())
        workflow_id = await _seed_workflow(
            harness, policy='{"cases":[{"required_indices":[1]}]}',
        )
        await _seed_node(harness, workflow_id, 0, status='READY', options=BAD_GOOD_UNTIL)
        await _seed_node(harness, workflow_id, 1, status='RUNNING', task_id=live_task)
        await _run_passes(harness, LIMIT)

        async with harness.sessions() as session:
            await on_workflow_task_complete(
                session,
                live_task,
                TaskResult(ok=1),
                harness.broker,
                task_name=TASK_NAME,
            )
            await session.commit()

        assert await _node_status(harness, workflow_id, 0) == 'FAILED'
        assert await _workflow_status(harness, workflow_id) == 'COMPLETED'


class TestParentHears:
    async def test_child_at_the_limit_fails_the_parent_node(
        self, harness: Harness,
    ) -> None:
        parent = await _seed_workflow(harness)
        child = await _seed_workflow(harness, parent=parent, parent_index=0)
        await _seed_node(harness, parent, 0, status='RUNNING', sub_workflow_id=child)
        await _seed_node(harness, child, 0, status='READY', options=BAD_GOOD_UNTIL)

        await _run_passes(harness, LIMIT + 1)

        assert await _workflow_status(harness, child) == 'FAILED'
        assert await _node_status(harness, parent, 0) == 'FAILED'
        assert await _workflow_status(harness, parent) == 'FAILED'
        assert await _failed_passes(harness, parent) is None
        parsed = loads_json(await _node_result(harness, parent, 0))
        assert not is_err(parsed)
        stored = str(parsed.ok_value)
        assert OutcomeCode.SUBWORKFLOW_FAILED.value in stored
        assert 'Workflow recovery raised a persistent error' in stored


class TestGlobalCompletion:
    async def test_a_drain_raise_keeps_the_child_and_counts_on_the_parent(
        self, harness: Harness,
    ) -> None:
        """The child completes in its own candidate, and the parent
        advancement raises in a second one. If that raise reached the child
        completion, the child would return on every pass with count 0."""
        parent = await _seed_workflow(harness)
        child = await _seed_workflow(harness, parent=parent, parent_index=0)
        await _seed_node(harness, child, 0, status='COMPLETED')
        await _seed_node(harness, parent, 0, status='RUNNING', sub_workflow_id=child)
        await _seed_node(
            harness, parent, 1, status='PENDING',
            dependencies=(0,), options=BAD_GOOD_UNTIL,
        )

        first = await _run_pass(harness)

        assert first.recovered == 1
        assert await _workflow_status(harness, child) == 'COMPLETED'
        assert await _failed_passes(harness, child) is None

        await _run_passes(harness, LIMIT)

        assert await _failed_passes(harness, parent) == LIMIT
        assert await _workflow_status(harness, parent) == 'FAILED'

    async def test_a_connection_loss_in_a_completion_is_one_error(
        self, harness: Harness,
    ) -> None:
        """A commit raises on the lost session, so this path rolls back. The
        pass goes on, and the workflow finishes on the next pass."""
        workflow_id = await _seed_workflow(harness)
        await _seed_node(harness, workflow_id, 0, status='COMPLETED')
        await _execute(
            harness,
            f"""
            CREATE FUNCTION limit_test_kill() RETURNS trigger
            LANGUAGE plpgsql AS $body$
            BEGIN
                IF NEW.id = '{workflow_id}'::uuid THEN
                    PERFORM pg_terminate_backend(pg_backend_pid());
                END IF;
                RETURN NEW;
            END
            $body$
            """,
        )
        await _execute(
            harness,
            """
            CREATE TRIGGER limit_test_kill BEFORE UPDATE ON horsies_workflows
            FOR EACH ROW EXECUTE FUNCTION limit_test_kill()
            """,
        )

        report = await _run_pass(harness)

        await _execute(harness, 'DROP TRIGGER limit_test_kill ON horsies_workflows')
        await _execute(harness, 'DROP FUNCTION limit_test_kill()')
        assert report.metrics['case_2_3'].errors == 1
        assert report.health()['errors'] == 1
        assert await _failed_passes(harness, workflow_id) is None

        await _run_pass(harness)
        assert await _workflow_status(harness, workflow_id) == 'COMPLETED'

    async def test_a_raised_completion_commits_its_count(
        self, harness: Harness,
    ) -> None:
        workflow_id = await _seed_workflow(harness, policy=BAD_POLICY)
        await _seed_node(harness, workflow_id, 0, status='COMPLETED')

        await _run_pass(harness)

        assert await _failed_passes(harness, workflow_id) == 1
        assert await _workflow_status(harness, workflow_id) == 'RUNNING'


class TestCountWrite:
    async def test_a_failed_count_write_changes_nothing_and_the_pass_goes_on(
        self, harness: Harness,
    ) -> None:
        """The count write has its own SAVEPOINT, so its failure does not
        abort the transaction of the pass."""
        poison = await _seed_workflow(harness)
        healthy = await _seed_workflow(harness)
        await _seed_node(harness, poison, 0, status='READY', options=BAD_GOOD_UNTIL)
        await _seed_node(harness, healthy, 0, status='READY')
        await _execute(
            harness,
            """
            CREATE FUNCTION limit_test_refuse_count() RETURNS trigger
            LANGUAGE plpgsql AS $body$
            BEGIN
                RAISE EXCEPTION 'refused by the test' USING ERRCODE = 'XX000';
            END
            $body$
            """,
        )
        await _execute(
            harness,
            """
            CREATE TRIGGER limit_test_refuse_count
            BEFORE INSERT ON horsies_workflow_recovery_failures
            FOR EACH ROW EXECUTE FUNCTION limit_test_refuse_count()
            """,
        )

        report = await _run_pass(harness)

        assert report.recovered == 1
        assert report.metrics['case_1'].failures_counted == 0
        assert await _failure_rows(harness) == 0
        assert await _node_status(harness, healthy, 0) == 'ENQUEUED'
        assert await _node_status(harness, poison, 0) == 'READY'
        assert await _workflow_status(harness, poison) == 'RUNNING'


class TestLimitWrite:
    async def test_a_failed_limit_write_keeps_the_count_and_retries(
        self, harness: Harness,
    ) -> None:
        workflow_id = await _seed_workflow(harness)
        await _seed_node(harness, workflow_id, 0, status='READY', options=BAD_GOOD_UNTIL)
        await _raise_on_update(
            harness,
            table='horsies_workflows',
            id_column='id',
            workflow_id=workflow_id,
            sqlstate='XX000',
        )

        reports = await _run_passes(harness, LIMIT)

        assert await _failed_passes(harness, workflow_id) == LIMIT
        assert await _workflow_status(harness, workflow_id) == 'RUNNING'
        assert await _node_status(harness, workflow_id, 0) == 'READY'
        assert reports[-1].metrics['case_1'].limit_reached == 0
        assert reports[-1].over_failure_limit == 1

        await _stop_raising_on_update(harness, table='horsies_workflows')
        await _run_pass(harness)

        assert await _failed_passes(harness, workflow_id) == LIMIT + 1
        assert await _workflow_status(harness, workflow_id) == 'FAILED'

    @pytest.mark.parametrize('status', ['PAUSED', 'CANCELLED'])
    async def test_a_workflow_that_is_not_running_is_refused(
        self, harness: Harness, status: str,
    ) -> None:
        workflow_id = await _seed_workflow(harness, status=status)
        await _seed_node(harness, workflow_id, 0, status='READY')
        failure = RecoveryRaise(
            workflow_id=workflow_id,
            task_index=0,
            case=RecoveryCase.READY_NOT_ENQUEUED,
            exception_type='DataError',
            detail='refused',
        )

        async with harness.sessions() as session:
            result = await apply_recovery_limit(session, failure, LIMIT)
            await session.commit()

        assert result.is_ok()
        assert result.unwrap() is RecoveryLimitOutcome.REFUSED
        assert await _workflow_status(harness, workflow_id) == status
        assert await _node_status(harness, workflow_id, 0) == 'READY'
        assert await _workflow_error(harness, workflow_id) is None


class TestPhase2Catch:
    async def test_a_raised_row_is_counted_and_fails_the_workflow_at_the_bound(
        self, harness: Harness,
    ) -> None:
        workflow_id, task_id = await _start_a_then_b(harness)

        summaries = [await _run_phase2(harness) for _ in range(LIMIT)]

        assert [s.raised_counted for s in summaries] == [1, 1, 1]
        assert [s.quarantined for s in summaries] == [0, 0, 1]
        assert await _workflow_status(harness, workflow_id) == 'FAILED'
        assert await _node_status(harness, workflow_id, 0) == 'FAILED'
        assert await _node_status(harness, workflow_id, 1) == 'SKIPPED'
        async with harness.sessions() as session:
            row = (
                await session.execute(
                    text(
                        'SELECT attempt_count, last_failure_class '
                        'FROM horsies_workflow_phase2_pending '
                        'WHERE task_id = :id',
                    ),
                    {'id': task_id},
                )
            ).one()
        assert row.attempt_count == LIMIT
        assert row.last_failure_class == 'RAISED'
        error = await _workflow_error(harness, workflow_id)
        assert error is not None
        assert 'WORKFLOW_RECOVERY_LIMIT_REACHED' in error

        after = await _run_phase2(harness)
        assert after.considered == 0

    async def test_a_transient_raise_stays_uncounted(
        self, harness: Harness,
    ) -> None:
        workflow_id, task_id = await _start_a_then_b(harness)
        await _raise_on_update(
            harness,
            table='horsies_workflow_tasks',
            id_column='workflow_id',
            workflow_id=workflow_id,
            sqlstate='55P03',
        )

        summaries = [await _run_phase2(harness) for _ in range(LIMIT + 1)]

        assert all(s.failed == 1 and s.raised_counted == 0 for s in summaries)
        async with harness.sessions() as session:
            attempts = (
                await session.execute(
                    text(
                        'SELECT attempt_count FROM horsies_workflow_phase2_pending '
                        'WHERE task_id = :id',
                    ),
                    {'id': task_id},
                )
            ).scalar_one()
        assert attempts == 0
        assert await _workflow_status(harness, workflow_id) == 'RUNNING'

    async def test_a_failed_limit_write_keeps_the_row_under_the_bound(
        self, harness: Harness,
    ) -> None:
        """Discovery stops at the bound, so the row must stay under it, or no
        later pass would try the write again."""
        workflow_id, task_id = await _start_a_then_b(harness)
        await _raise_on_update(
            harness,
            table='horsies_workflows',
            id_column='id',
            workflow_id=workflow_id,
            sqlstate='XX000',
        )

        for _ in range(LIMIT):
            await _run_phase2(harness)

        async with harness.sessions() as session:
            attempts = (
                await session.execute(
                    text(
                        'SELECT attempt_count FROM horsies_workflow_phase2_pending '
                        'WHERE task_id = :id',
                    ),
                    {'id': task_id},
                )
            ).scalar_one()
        assert attempts == LIMIT - 1
        assert await _workflow_status(harness, workflow_id) == 'RUNNING'

        await _stop_raising_on_update(harness, table='horsies_workflows')
        await _run_phase2(harness)

        assert await _workflow_status(harness, workflow_id) == 'FAILED'


class TestPhase2PauseWindow:
    async def test_a_pause_at_the_bound_leaves_the_row_for_a_later_pass(
        self, harness: Harness,
    ) -> None:
        """The workflow pauses after the raise and before the limit write.
        Discovery stops at the bound, so a stored attempt would leave the
        resumed workflow RUNNING with no path to end."""
        workflow_id, task_id = await _start_a_then_b(harness)
        for _ in range(LIMIT - 1):
            await _run_phase2(harness)
        await _execute(
            harness,
            "UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = :id",
            id=workflow_id,
        )

        row = await _record_raised_row(
            harness.sessions,
            task_id=task_id,
            exc=RuntimeError('raised before the pause'),
            quarantine_after_attempts=LIMIT,
        )

        assert isinstance(row, _RaisedRowNotRecorded)
        async with harness.sessions() as session:
            stored = (
                await session.execute(
                    text(
                        'SELECT attempt_count, recovery_source '
                        'FROM horsies_workflow_phase2_pending '
                        'WHERE task_id = :id',
                    ),
                    {'id': task_id},
                )
            ).one()
        assert stored.attempt_count == LIMIT - 1
        assert stored.recovery_source == 'HISTORY'
        assert await _workflow_status(harness, workflow_id) == 'PAUSED'

        await _execute(
            harness,
            "UPDATE horsies_workflows SET status = 'RUNNING' WHERE id = :id",
            id=workflow_id,
        )
        summary = await _run_phase2(harness)

        assert summary.considered == 1
        assert summary.quarantined == 1
        assert await _workflow_status(harness, workflow_id) == 'FAILED'


class TestRetention:
    async def test_rows_go_with_their_workflow_and_with_a_cascaded_child(
        self, harness: Harness,
    ) -> None:
        """A parent delete cascades to its child, so the statement that
        deletes workflows cannot name the child."""
        parent = await _seed_workflow(harness, status='FAILED')
        child = await _seed_workflow(
            harness, status='FAILED', parent=parent, parent_index=0,
        )
        kept = await _seed_workflow(harness)
        await _execute(
            harness,
            """
            UPDATE horsies_workflows
            SET completed_at = NOW() - INTERVAL '10 days'
            WHERE status = 'FAILED'
            """,
        )
        async with harness.sessions() as session:
            for workflow_id in (parent, child, kept):
                await session.execute(
                    text("""
                        INSERT INTO horsies_workflow_recovery_failures (
                            workflow_id, failed_passes, last_pass_id,
                            first_failed_at, last_failed_at, last_case,
                            last_exception_type
                        ) VALUES (
                            :id, 1, gen_random_uuid(), NOW(), NOW(),
                            'ready_not_enqueued', 'DataError'
                        )
                    """),
                    {'id': workflow_id},
                )
            await session.commit()

        async with harness.sessions() as session:
            await session.execute(
                DELETE_EXPIRED_WORKFLOWS_SQL,
                {'retention_hours': 24, 'batch_size': 100},
            )
            await session.execute(
                DELETE_ORPHANED_RECOVERY_FAILURES_SQL,
                {'batch_size': 100},
            )
            await session.commit()

        assert await _failed_passes(harness, parent) is None
        assert await _failed_passes(harness, child) is None
        assert await _failed_passes(harness, kept) == 1
