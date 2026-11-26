"""Integration tests for the monitoring HTTP API.

Runs the real app over httpx's ASGI transport against a real database, so the
wiring between routes, the query package, and the task/workflow primitives is
exercised end to end. The app mounts the core's handler. Coverage:

1. read routes — envelopes, 404 copy, and the vanished-backing-task case
2. worker routes — snapshots, liveness, schedules, unknown-worker history
3. the section 7.4 action mapping table, row by row
4. a resume whose child build raises
5. the SSE event layer — a real NOTIFY and the authorization of the stream
6. schema compatibility (spec 7.5b) — the tool never runs DDL, and refuses to
   write through a schema it does not recognize
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Final, LiteralString

import psycopg
from psycopg import sql
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core import cli
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.schema_mode import SchemaMode
from horsies.core.types.result import Ok, Result, is_err
from horsies.core.types.status import TaskStatus
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import TaskNode as WorkflowTaskNode
from horsies.core.models.workflow import WorkflowDefinition, WorkflowSpec
from horsies.core.workflows.registry import register_workflow_definition
from horsies.web import INTENT_HEADER, INTENT_VALUE, AllowAll, create_monitoring_app
from horsies.core.utils.url import to_psycopg_url
from horsies.web import schema as schema_module
from tests.integration.conftest import DB_URL
from tests.integration.history_seeding import stored_history_status
from tests.integration.monitoring_seeding import (
    NodeRow,
    SeedRow,
    SubworkflowNode,
    TaskNode,
    TaskRow,
    WorkflowRow,
    insert_rows,
    task_row,
)
from tests.session_database import database_name_prefix
from tests.web_event_stream import AsgiEventStream

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

UTC = timezone.utc
ACT = {INTENT_HEADER: INTENT_VALUE}
RUNNING_WORKER_ID: Final = 'worker-api-aaaa'


# --------------------------------------------------------------------------- #
# Fixtures and helpers
# --------------------------------------------------------------------------- #
@pytest_asyncio.fixture
async def clean_tables(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - ensures migrations are applied
) -> AsyncGenerator[None, None]:
    """Empty the tables the API aggregates over."""
    await session.execute(
        text(
            'TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks, '
            'horsies_schedule_state, horsies_task_history CASCADE'
        )
    )
    await session.commit()
    yield


@pytest_asyncio.fixture
async def monitoring(app: Horsies) -> AsyncGenerator[FastAPI, None]:
    """The monitoring app for this horsies app."""
    built = create_monitoring_app(app, auth_policy=AllowAll())
    yield built
    await built.state.events.close()


@pytest_asyncio.fixture
async def api(monitoring: FastAPI) -> AsyncGenerator[AsyncClient, None]:
    """A client speaking to the monitoring app."""
    async with AsyncClient(
        transport=ASGITransport(app=monitoring), base_url='http://test'
    ) as client:
        yield client


def make_task(
    *,
    task_name: str = 'api_task',
    queue_name: str = 'default',
    status: TaskStatus = TaskStatus.PENDING,
    is_workflow_task: bool = False,
    good_until: datetime | None = None,
    error_code: str | None = None,
    worker_id: str | None = None,
) -> TaskRow:
    """Build a task row that no claim holds: PENDING or terminal. A terminal
    row ends now.

    A CLAIMED or RUNNING row holds a claim (``claimed_at``), which this
    builder does not write. A RUNNING row comes from ``make_running_task``.
    """
    match status:
        case TaskStatus.CLAIMED | TaskStatus.RUNNING:
            raise AssertionError(
                f'make_task writes no claim; a {status.value} row holds one '
                '(use make_running_task for RUNNING)',
            )
        case (
            TaskStatus.PENDING
            | TaskStatus.COMPLETED
            | TaskStatus.FAILED
            | TaskStatus.CANCELLED
            | TaskStatus.EXPIRED
        ):
            pass
    return task_row(
        task_id=str(uuid.uuid4()),
        task_name=task_name,
        queue_name=queue_name,
        status=status,
        priority=100,
        retry_count=0,
        max_retries=3,
        is_workflow_task=is_workflow_task,
        enqueued_at=datetime.now(UTC) - timedelta(seconds=60),
        claimed_at=None,
        started_at=None,
        completed_at=None,
        failed_at=None,
        good_until=good_until,
        error_code=error_code,
        failed_reason=None,
        worker_id=worker_id,
        worker_hostname=None,
        worker_pid=None,
        worker_process_name=None,
        claim_expires_at=None,
    )


def make_running_task(*, worker_id: str = RUNNING_WORKER_ID) -> TaskRow:
    """Build a RUNNING plain task as its start leaves it.

    ``horsies_start_attempt`` moves a CLAIMED row of ``worker_id`` to RUNNING
    and keeps its claim generation (``claimed_at``). It writes ``started_at``
    and the process facts. The cancel of a RUNNING task copies these facts
    into the attempt row that it writes.
    """
    now = datetime.now(UTC)
    return task_row(
        task_id=str(uuid.uuid4()),
        task_name='api_task',
        queue_name='default',
        status=TaskStatus.RUNNING,
        priority=100,
        retry_count=0,
        max_retries=3,
        is_workflow_task=False,
        enqueued_at=now - timedelta(seconds=60),
        claimed_at=now - timedelta(seconds=40),
        started_at=now - timedelta(seconds=30),
        completed_at=None,
        failed_at=None,
        good_until=None,
        error_code=None,
        failed_reason=None,
        worker_id=worker_id,
        worker_hostname='api-host',
        worker_pid=4242,
        worker_process_name='api-process',
        claim_expires_at=None,
    )


def make_workflow(*, name: str = 'api_flow', status: str = 'RUNNING') -> WorkflowRow:
    """Build a root workflow run row."""
    created_at = datetime.now(UTC) - timedelta(seconds=120)
    return WorkflowRow(
        id=str(uuid.uuid4()),
        name=name,
        status=status,
        on_error='fail',
        definition_key=None,
        parent=None,
        sent_at=created_at,
        created_at=created_at,
        completed_at=None,
    )


def make_node(*, workflow_id: str, status: str, task_id: str | None) -> NodeRow:
    """Build node 0 of a run, a task node named ``step``."""
    return NodeRow(
        id=str(uuid.uuid4()),
        workflow_id=workflow_id,
        task_index=0,
        node_id=None,
        task_name='step',
        status=status,
        dependencies=(),
        allow_failed_deps=False,
        kind=TaskNode(task_id=task_id),
        result=None,
        created_at=datetime.now(UTC) - timedelta(seconds=120),
        started_at=None,
        completed_at=None,
    )


async def persist(session: AsyncSession, *rows: SeedRow) -> None:
    """Persist rows on their lifecycle side and commit.

    Terminal-status tasks are seeded as history rows — the live table
    holds only live rows (``tests/integration/monitoring_seeding.py``).
    """
    await insert_rows(session, *rows)


async def read_status(session: AsyncSession, workflow_id: str) -> str:
    """Current status of a workflow run."""
    row = (
        await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :id'),
            {'id': workflow_id},
        )
    ).first()
    assert row is not None
    return str(row[0])


# --------------------------------------------------------------------------- #
# Read routes
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_tables')
class TestTaskReads:
    """Task endpoints return the query package's shapes unchanged."""

    async def test_stats_returns_seven_fixed_cards(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        await persist(session, make_task(status=TaskStatus.FAILED))

        response = await api.get('/api/tasks/stats')

        assert response.status_code == 200
        body = response.json()
        assert [row['status'] for row in body] == [
            'PENDING',
            'CLAIMED',
            'RUNNING',
            'COMPLETED',
            'FAILED',
            'CANCELLED',
            'EXPIRED',
        ]
        assert {row['status']: row['count'] for row in body}['FAILED'] == 1

    async def test_stats_answer_from_the_shared_cache_within_its_ttl(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        """A second poll of the same scope inside the TTL is the cached answer."""
        await persist(session, make_task(status=TaskStatus.FAILED))
        first = (await api.get('/api/tasks/stats')).json()

        await persist(session, make_task(status=TaskStatus.FAILED))
        second = (await api.get('/api/tasks/stats')).json()
        other_scope = (await api.get('/api/tasks/stats?retried_only=false&queue=default')).json()

        failed = {row['status']: row['count'] for row in first}['FAILED']
        assert failed == 1
        assert second == first
        assert {row['status']: row['count'] for row in other_scope}['FAILED'] == 2

    async def test_list_returns_the_page_envelope(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        await persist(session, make_task(), make_task())

        body = (await api.get('/api/tasks?limit=1')).json()

        assert body['total'] == 2
        assert len(body['rows']) == 1

    async def test_facets_and_breakdown_answer(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        await persist(session, make_task(task_name='alpha'))

        facets = (await api.get('/api/tasks/facets')).json()
        breakdown = (await api.get('/api/tasks/breakdown?group_by=task_name')).json()

        assert [f['value'] for f in facets['task_names']] == ['alpha']
        assert breakdown['group_by'] == 'task_name'
        assert breakdown['total']['total'] == 1

    async def test_breakdown_groups_carry_their_kind(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        await persist(
            session,
            make_running_task(worker_id='unknown'),
            make_task(worker_id=None),
        )

        breakdown = (await api.get('/api/tasks/breakdown?group_by=worker')).json()

        assert [row['group'] for row in breakdown['groups']] == [
            {'kind': 'value', 'value': 'unknown'},
            {'kind': 'unclaimed'},
        ]
        assert 'group' not in breakdown['total']

    async def test_detail_carries_the_leaf_and_attempts(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.FAILED, error_code='TASK_EXCEPTION')
        await persist(session, task)

        body = (await api.get(f'/api/tasks/{task.id}')).json()

        assert body['leaf']['task_id'] == task.id
        assert body['leaf']['good_until'] is None
        assert body['error_category'] == 'OPERATIONAL'
        assert body['attempts'] == []

    async def test_unknown_task_is_not_found(self, api: AsyncClient) -> None:
        response = await api.get(f'/api/tasks/{uuid.uuid4()}')

        assert response.status_code == 404
        assert response.json()['detail'] == 'Task not found.'

    async def test_an_id_that_is_not_a_uuid_is_invalid_input(
        self, api: AsyncClient,
    ) -> None:
        response = await api.get('/api/tasks/not-a-uuid')

        assert response.status_code == 400
        body = response.json()
        assert body['code'] == 'INVALID_INPUT'
        assert body['detail'].startswith(
            'Path parameter task_id: Input should be a valid UUID, ',
        )

    async def test_error_category_filters_every_task_surface(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        """One repeatable param, expanded to codes by the query layer."""
        await persist(
            session,
            make_task(status=TaskStatus.FAILED, error_code='TASK_EXCEPTION'),
            make_task(status=TaskStatus.FAILED, error_code='PAYMENT_DECLINED'),
        )
        scope = 'error_category=OPERATIONAL'

        listing = (await api.get(f'/api/tasks?{scope}')).json()
        stats = (await api.get(f'/api/tasks/stats?{scope}')).json()
        breakdown = (
            await api.get(f'/api/tasks/breakdown?group_by=task_name&{scope}')
        ).json()
        facets = (await api.get(f'/api/tasks/facets?{scope}')).json()

        assert [row['error_code'] for row in listing['rows']] == ['TASK_EXCEPTION']
        assert sum(row['count'] for row in stats) == 1
        assert breakdown['total']['total'] == 1
        # The code list follows the selection; the strip's own totals do not,
        # so the categories it does not have selected stay offerable.
        assert [f['value'] for f in facets['error_codes']] == ['TASK_EXCEPTION']
        assert facets['error_category_totals'] == {'OPERATIONAL': 1, 'DOMAIN': 1}

    async def test_repeated_error_category_params_are_or_combined(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        await persist(
            session,
            make_task(status=TaskStatus.FAILED, error_code='TASK_EXCEPTION'),
            make_task(status=TaskStatus.CANCELLED, error_code='TASK_CANCELLED'),
            make_task(status=TaskStatus.FAILED, error_code='PAYMENT_DECLINED'),
        )

        body = (
            await api.get('/api/tasks?error_category=OUTCOME&error_category=DOMAIN')
        ).json()

        assert {row['error_code'] for row in body['rows']} == {
            'TASK_CANCELLED',
            'PAYMENT_DECLINED',
        }

    @pytest.mark.parametrize(
        'path',
        ['/api/tasks', '/api/tasks/stats', '/api/tasks/breakdown', '/api/tasks/facets'],
    )
    async def test_unknown_error_category_is_a_bad_request(
        self, api: AsyncClient, path: str
    ) -> None:
        """An unknown family cannot be expanded, so it is rejected, not ignored."""
        response = await api.get(f'{path}?error_category=NOT_A_FAMILY')

        assert response.status_code == 400
        assert response.json()['detail'] == "Unknown error category 'NOT_A_FAMILY'."


@pytest.mark.usefixtures('clean_tables')
class TestWorkflowReads:
    """Workflow endpoints, including the drilled-into node case."""

    async def test_names_and_runs(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        await persist(session, make_workflow(name='alpha_flow'))

        names = (await api.get('/api/workflows/names')).json()
        runs = (await api.get('/api/workflows')).json()

        assert names == ['alpha_flow']
        assert len(runs) == 1
        assert runs[0]['name'] == 'alpha_flow'

    async def test_run_detail_carries_nodes_and_edges(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        run = make_workflow()
        await persist(session, run)
        await persist(
            session,
            make_node(workflow_id=run.id, status='FAILED', task_id=None),
        )

        body = (await api.get(f'/api/workflows/{run.id}')).json()

        assert body['run']['id'] == run.id
        assert body['failed_indices'] == [0]
        assert body['edges'] == []

    async def test_unknown_run_is_not_found(self, api: AsyncClient) -> None:
        response = await api.get(f'/api/workflows/{uuid.uuid4()}')

        assert response.status_code == 404
        assert response.json()['detail'] == 'Workflow run not found.'

    async def test_unknown_node_is_not_found(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        run = make_workflow()
        await persist(session, run)

        response = await api.get(f'/api/workflows/{run.id}/tasks/0')

        assert response.status_code == 404
        assert response.json()['detail'] == 'Workflow task not found.'

    async def test_node_with_a_vanished_task_row_still_resolves(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        """Retention can remove the task while the node row survives."""
        run = make_workflow()
        await persist(session, run)
        await persist(
            session,
            make_node(
                workflow_id=run.id, status='COMPLETED', task_id=str(uuid.uuid4()),
            ),
        )

        response = await api.get(f'/api/workflows/{run.id}/tasks/0')

        assert response.status_code == 200
        assert response.json()['leaf'] is None


@pytest.mark.usefixtures('clean_tables')
class TestWorkerReads:
    """Worker snapshots, liveness, schedules, history."""

    async def test_worker_list_answers(self, api: AsyncClient) -> None:
        response = await api.get('/api/workers')

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    async def test_worker_list_carries_the_slot_health(
        self,
        api: AsyncClient,
        session: AsyncSession,
    ) -> None:
        worker_id = f'itest-{uuid.uuid4().hex}'
        await session.execute(
            text("""
                INSERT INTO horsies_worker_states (
                    worker_id, snapshot_at, hostname, pid, processes,
                    max_claim_batch, max_claim_per_worker, queues,
                    queue_priorities, queue_max_concurrency, recovery_config,
                    tasks_running, tasks_claimed, worker_started_at,
                    slots_down, class_capacity, telemetry
                ) VALUES (
                    :wid, now(), 'itest-host', 4242, 2, 0, 2,
                    ARRAY['default'], '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
                    0, 0, now(),
                    CAST(:slots_down AS JSONB), '{}'::jsonb, jsonb_build_object('endpoint', jsonb_build_object('state', 'off'))
                )
            """),
            {
                'wid': worker_id,
                'slots_down': json.dumps(
                    [
                        {
                            'slot': 1,
                            'cause': {'kind': 'resource_shortage', 'errno': 'EMFILE'},
                            'detail': 'OSError: [Errno 24] Too many open files',
                            'down_since': '2026-11-10T12:00:00+00:00',
                            'failed_boots': 2,
                        },
                    ],
                ),
            },
        )
        await session.commit()
        try:
            body = (await api.get('/api/workers')).json()
        finally:
            await session.execute(
                text('DELETE FROM horsies_worker_states WHERE worker_id = :wid'),
                {'wid': worker_id},
            )
            await session.commit()

        (worker,) = [entry for entry in body if entry['worker_id'] == worker_id]
        assert worker['slot_health'] == {
            'kind': 'slots_down',
            'slots': [
                {
                    'slot': 1,
                    'cause': 'the OS ran short of a resource (EMFILE)',
                    'detail': 'OSError: [Errno 24] Too many open files',
                    'down_since': '2026-11-10T12:00:00Z',
                    'failed_boots': 2,
                },
            ],
        }
        assert 'down_slots' not in worker

    async def test_class_report_answers_with_streams_and_budgets(
        self,
        api: AsyncClient,
        session: AsyncSession,
    ) -> None:
        await session.execute(text('DELETE FROM horsies_worker_states'))
        await session.execute(
            text("""
                INSERT INTO horsies_worker_states (
                    worker_id, snapshot_at, hostname, pid, processes,
                    max_claim_batch, max_claim_per_worker, queues,
                    queue_priorities, queue_max_concurrency, recovery_config,
                    tasks_running, tasks_claimed, worker_started_at,
                    slots_down, class_capacity, telemetry
                ) VALUES (
                    'w-async', now(), 'itest-host', 4242, 2, 0, 2,
                    ARRAY['default'], '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
                    0, 0, now(), '[]'::jsonb,
                    '{"SHARED_ASYNC": {"capacity": 8, "serves": ["SHARED_ASYNC"]}}', jsonb_build_object('endpoint', jsonb_build_object('state', 'off'))
                )
            """),
        )
        await session.commit()
        await persist(session, make_task())
        try:
            response = await api.get('/api/workers/classes')
        finally:
            await session.execute(text('DELETE FROM horsies_worker_states'))
            await session.commit()

        assert response.status_code == 200
        body = response.json()
        (stream,) = body['streams']
        assert stream['queue'] == 'default'
        assert stream['execution'] == 'ISOLATED'
        assert stream['coverage'] == {'kind': 'unserved'}
        assert stream['wait']['kind'] == 'due'
        assert body['live_window_seconds'] == 120.0
        assert 'not_recorded' not in body
        assert body['workers'] == [
            {
                'worker_id': 'w-async',
                'snapshot_at': body['workers'][0]['snapshot_at'],
                'budgets': [
                    {
                        'budget': 'SHARED_ASYNC',
                        'capacity': 8,
                        'serves': ['SHARED_ASYNC'],
                        'in_flight': 0,
                    },
                ],
            },
        ]

    async def test_schedules_answer(self, api: AsyncClient) -> None:
        response = await api.get('/api/workers/schedules')

        assert response.status_code == 200
        assert response.json() == []

    async def test_liveness_reports_a_reachable_database(
        self, api: AsyncClient
    ) -> None:
        """An unreachable database would be data here, not an error."""
        body = (await api.get('/api/workers/ping?timeout_seconds=0.1')).json()

        assert body['database']['kind'] == 'ping'
        assert body['database']['latency_ms'] >= 0.0
        assert body['workers'] == []

    async def test_unknown_worker_history_is_empty_not_missing(
        self, api: AsyncClient
    ) -> None:
        response = await api.get('/api/workers/never-reported/history')

        assert response.status_code == 200
        assert response.json() == []


# --------------------------------------------------------------------------- #
# Action mapping (spec 7.4)
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_tables')
class TestTaskActionMapping:
    """Every row of the task half of the mapping table."""

    async def test_cancel_ok_is_two_hundred_with_the_previous_status(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)

        response = await api.post(f'/api/tasks/{task.id}/cancel', headers=ACT)

        assert response.status_code == 200
        body = response.json()
        assert body['outcome'] == 'cancelled'
        assert body['was_status'] == 'PENDING'
        assert body['warning'] is None

    async def test_missing_task_is_not_found(self, api: AsyncClient) -> None:
        response = await api.post(f'/api/tasks/{uuid.uuid4()}/cancel', headers=ACT)

        assert response.status_code == 404
        assert 'detail' in response.json()

    async def test_an_id_that_is_not_a_uuid_is_invalid_input(
        self, api: AsyncClient,
    ) -> None:
        response = await api.post('/api/tasks/not-a-uuid/cancel', headers=ACT)

        assert response.status_code == 400
        body = response.json()
        assert body['code'] == 'INVALID_INPUT'
        assert body['detail'].startswith(
            'Path parameter task_id: Input should be a valid UUID, ',
        )

    async def test_uncancellable_task_is_a_conflict(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.COMPLETED)
        await persist(session, task)

        response = await api.post(f'/api/tasks/{task.id}/cancel', headers=ACT)

        assert response.status_code == 409
        assert response.json() == {
            'code': 'TASK_NOT_CANCELLABLE',
            'current_status': 'COMPLETED',
        }

    async def test_workflow_bound_task_is_a_bad_request(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        """The UI hides these buttons; reaching here means a stale client."""
        task = make_task(status=TaskStatus.FAILED, is_workflow_task=True)
        await persist(session, task)

        response = await api.post(f'/api/tasks/{task.id}/cancel', headers=ACT)

        assert response.status_code == 400
        assert response.json() == {'code': 'TASK_IS_WORKFLOW_TASK'}

    async def test_running_task_needs_the_explicit_body_flag(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        task = make_running_task()
        await persist(session, task)

        refused = await api.post(f'/api/tasks/{task.id}/cancel', headers=ACT)
        allowed = await api.post(
            f'/api/tasks/{task.id}/cancel',
            json={'include_running': True},
            headers=ACT,
        )

        assert refused.status_code == 409
        assert refused.json()['current_status'] == 'RUNNING'
        assert allowed.status_code == 200
        assert allowed.json()['was_status'] == 'RUNNING'

    async def test_running_cancel_records_the_attempt_as_cancelled(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        """The cancel statement writes the attempt of the run before the move;
        the task detail reads it from the attempt archive."""
        task = make_running_task()
        await persist(session, task)

        cancel = await api.post(
            f'/api/tasks/{task.id}/cancel',
            json={'include_running': True},
            headers=ACT,
        )
        detail = await api.get(f'/api/tasks/{task.id}')

        assert cancel.status_code == 200
        assert detail.status_code == 200
        body = detail.json()
        assert body['leaf']['status'] == 'CANCELLED'
        [attempt] = body['attempts']
        assert attempt['attempt'] == 1
        assert attempt['outcome'] == 'CANCELLED'
        assert attempt['will_retry'] is False
        assert attempt['error_code'] == 'TASK_CANCELLED'
        assert attempt['worker_hostname'] == 'api-host'
        assert attempt['phases']['kind'] == 'captured'
        assert attempt['phases']['recorded_by'] == {'kind': 'cancel'}

    async def test_stored_data_that_does_not_parse_is_a_server_error(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        """The request was valid, and a repeat gives the same answer: not 503."""
        task = make_task(status=TaskStatus.FAILED)
        await persist(session, task)

        async with stored_history_status(session, task.id, 'NOT_KNOWN'):
            cancel = await api.post(f'/api/tasks/{task.id}/cancel', headers=ACT)
            detail = await api.get(f'/api/tasks/{task.id}')

        assert cancel.status_code == 500
        assert "'NOT_KNOWN'" in cancel.json()['detail']
        assert detail.status_code == 500
        assert detail.json()['detail'].startswith('Task detail query failed:')
        assert "'NOT_KNOWN'" in detail.json()['detail']


@pytest.mark.usefixtures('clean_tables')
class TestWorkflowActionMapping:
    """Every row of the workflow half of the mapping table."""

    async def test_pause_of_a_running_run_succeeds(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        run = make_workflow(status='RUNNING')
        await persist(session, run)

        response = await api.post(f'/api/workflows/{run.id}/pause', headers=ACT)

        assert response.status_code == 200
        assert response.json()['outcome'] == 'paused'
        assert await read_status(session, run.id) == 'PAUSED'

    async def test_pause_of_a_non_running_run_is_a_conflict(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        """Ok(False) is not an error, but it is not the requested effect."""
        run = make_workflow(status='COMPLETED')
        await persist(session, run)

        response = await api.post(f'/api/workflows/{run.id}/pause', headers=ACT)

        assert response.status_code == 409
        assert response.json() == {
            'code': 'STATE_CONFLICT',
            'current_status': 'COMPLETED',
        }

    async def test_resume_of_a_paused_run_succeeds(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        run = make_workflow(status='PAUSED')
        await persist(session, run)

        response = await api.post(f'/api/workflows/{run.id}/resume', headers=ACT)

        assert response.status_code == 200
        assert response.json()['outcome'] == 'resumed'
        assert response.json()['warning'] is None

    async def test_resume_of_a_non_paused_run_is_a_conflict(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        run = make_workflow(status='RUNNING')
        await persist(session, run)

        response = await api.post(f'/api/workflows/{run.id}/resume', headers=ACT)

        assert response.status_code == 409
        assert response.json()['code'] == 'STATE_CONFLICT'

    async def test_cancel_of_a_live_run_succeeds(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        run = make_workflow(status='RUNNING')
        await persist(session, run)

        response = await api.post(f'/api/workflows/{run.id}/cancel', headers=ACT)

        assert response.status_code == 200
        assert response.json()['outcome'] == 'cancelled'
        assert await read_status(session, run.id) == 'CANCELLED'

    async def test_cancel_of_a_finished_run_is_a_conflict(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        """cancel_async reports success on a no-op, so status decides."""
        run = make_workflow(status='COMPLETED')
        await persist(session, run)

        response = await api.post(f'/api/workflows/{run.id}/cancel', headers=ACT)

        assert response.status_code == 409
        assert response.json() == {
            'code': 'STATE_CONFLICT',
            'current_status': 'COMPLETED',
        }

    @pytest.mark.parametrize('action', ['pause', 'resume', 'cancel'])
    async def test_missing_run_is_not_found(
        self, api: AsyncClient, action: str
    ) -> None:
        response = await api.post(
            f'/api/workflows/{uuid.uuid4()}/{action}', headers=ACT
        )

        assert response.status_code == 404


_CORE_LOG_WAIT_POLLS: Final = 100
"""Reads of ``caplog`` while a test waits for a record of the core."""

_CORE_LOG_WAIT_POLL_S: Final = 0.05
"""The pause between two of those reads: the wait ends within 5 s."""


async def _core_messages_holding(
    caplog: pytest.LogCaptureFixture, logger_name: str, text_part: str,
) -> list[str]:
    """The messages of the records of ``logger_name`` that hold
    ``text_part``, once one reached ``caplog``.

    The core hands its records to Python ``logging`` on its forwarder
    thread, so a record can arrive after the call that logged it returned.

    Raises:
        AssertionError: no such record within the wait.
    """
    for _ in range(_CORE_LOG_WAIT_POLLS):
        messages = [
            record.getMessage()
            for record in caplog.records
            if record.name == logger_name and text_part in record.getMessage()
        ]
        if messages:
            return messages
        await asyncio.sleep(_CORE_LOG_WAIT_POLL_S)
    raise AssertionError(f'no record of {logger_name} holds {text_part!r}')


def _register_child_with_a_corrupt_root(app: Horsies) -> str:
    """Register a child definition whose root task has task options that do
    not parse and a ``good_until``, so its build raises ``SerializationError``
    when it merges the deadline into the options. Returns its definition
    key."""
    suffix = uuid.uuid4().hex[:8]

    @app.task(task_name=f'web_corrupt_root_{suffix}')
    def corrupt_root(*, value: int) -> Result[int, TaskError]:
        return Ok(value)

    corrupt_root.task_options_json = '{not-json'  # type: ignore[attr-defined]

    class CorruptRootChild(WorkflowDefinition[int]):
        name = f'web_corrupt_child_{suffix}'
        definition_key = f'web.corrupt_child.{suffix}.v1'

        @classmethod
        def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
            _ = (args, params)
            node = WorkflowTaskNode(
                fn=corrupt_root,
                kwargs={'value': 1},
                good_until=datetime.now(timezone.utc) + timedelta(hours=1),
            )
            return app.workflow(cls.name, [node], output=node, definition_key=cls.definition_key)

    register_workflow_definition(CorruptRootChild)
    return CorruptRootChild.definition_key


async def _seed_paused_parent_of_a_ready_child(
    session: AsyncSession, definition_key: str,
) -> WorkflowRow:
    """A PAUSED run whose node 0 is a READY sub-workflow node of
    ``definition_key`` with no child: a resume owes its child start."""
    run = make_workflow(status='PAUSED')
    node = NodeRow(
        id=str(uuid.uuid4()),
        workflow_id=run.id,
        task_index=0,
        node_id=None,
        task_name='corrupt_child',
        status='READY',
        dependencies=(),
        allow_failed_deps=False,
        kind=SubworkflowNode(sub_workflow_id=None),
        result=None,
        created_at=datetime.now(UTC) - timedelta(seconds=120),
        started_at=None,
        completed_at=None,
    )
    await persist(session, run, node)
    await session.execute(
        text(
            'UPDATE horsies_workflow_tasks '
            'SET sub_definition_key = :key, sub_workflow_name = :name '
            'WHERE workflow_id = CAST(:workflow_id AS uuid) AND task_index = 0'
        ),
        {'key': definition_key, 'name': 'corrupt_child', 'workflow_id': run.id},
    )
    await session.commit()
    return run


@pytest.mark.usefixtures('clean_tables')
class TestResumeOfAChildBuildThatRaises:
    """A resume whose owed child start builds a child that raises: the task
    options of the child's root do not parse, and the build merges the
    root's ``good_until`` into them (``SerializationError``).

    Python's resume route let the exception propagate, and Starlette answered
    500 with the text ``Internal Server Error`` and nothing of the exception,
    whose text can hold user data. The core answers the same, commits
    nothing, and logs the exception's type and text with the resume and the
    build on ``horsies.monitoring``. The core's mapping is also tested in
    ``core/src/web/tests.rs``
    (``a_resume_whose_child_build_raised_is_python_s_internal_server_error``).
    """

    async def test_the_answer_is_python_s_internal_server_error(
        self,
        app: Horsies,
        monitoring: FastAPI,
        session: AsyncSession,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        definition_key = _register_child_with_a_corrupt_root(app)
        run = await _seed_paused_parent_of_a_ready_child(session, definition_key)

        # Python's route raised: Starlette sends its 500, then raises the
        # exception again to the server, which the transport keeps.
        async with AsyncClient(
            transport=ASGITransport(app=monitoring, raise_app_exceptions=False),
            base_url='http://test',
        ) as client:
            with caplog.at_level(logging.ERROR, logger='horsies.monitoring'):
                response = await client.post(f'/api/workflows/{run.id}/resume', headers=ACT)
                messages = await _core_messages_holding(
                    caplog, 'horsies.monitoring', run.id,
                )

        assert response.status_code == 500
        assert response.headers['content-type'] == 'text/plain; charset=utf-8'
        assert response.text == 'Internal Server Error'
        assert await read_status(session, run.id) == 'PAUSED'
        match messages:
            case [message]:
                assert message.startswith(
                    f'monitoring resume of workflow {run.id} failed and nothing committed: '
                    f'build of the child of {run.name}:0 (definition {definition_key}) '
                    'raised SerializationError: '
                ), message
            case _:
                raise AssertionError(f'one ERROR record of the resume, got {messages}')


# --------------------------------------------------------------------------- #
# Event stream (spec 7.5)
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_tables')
class TestEventStream:
    """Invalidation events from the installed trigger."""

    async def test_a_real_task_change_produces_a_tasks_event(
        self, monitoring: FastAPI, session: AsyncSession
    ) -> None:
        """Driven through the installed trigger, not a synthesized notify."""

        async def keep_changing() -> None:
            for _ in range(60):
                await persist(session, make_task())
                await asyncio.sleep(0.25)

        async with AsgiEventStream(monitoring, '/api/events') as stream:
            assert stream.status_code == 200
            assert 'text/event-stream' in stream.headers['content-type']
            writer = asyncio.create_task(keep_changing())
            try:
                frame = await stream.next_data(timeout_s=25)
            finally:
                writer.cancel()
                try:
                    await writer
                except asyncio.CancelledError:
                    pass

        assert frame['topic'] == 'tasks'
        assert isinstance(frame['ids'], list)


class TestEventStreamAuthorization:
    """The stream is an API route and is gated like every other one."""

    async def test_unauthorized_stream_is_refused(self, app: Horsies) -> None:
        from horsies.web import TrustedHeader

        monitoring_app = create_monitoring_app(
            app, auth_policy=TrustedHeader('X-Forwarded-User', allow_actions=False)
        )
        async with AsyncClient(
            transport=ASGITransport(app=monitoring_app), base_url='http://test'
        ) as client:
            response = await client.get('/api/events')

        assert response.status_code == 403
        await monitoring_app.state.events.close()


class TestMountedUnderAHost:
    """The API answers under a mount path, which is how adopters deploy it."""

    async def test_routes_answer_under_the_mount(self, app: Horsies) -> None:
        monitoring_app = create_monitoring_app(app, auth_policy=AllowAll())
        host = FastAPI()
        host.mount('/monitoring', monitoring_app)

        async with AsyncClient(
            transport=ASGITransport(app=host), base_url='http://test'
        ) as client:
            stats = await client.get('/monitoring/api/tasks/stats')
            meta = await client.get('/monitoring/api/meta')

        assert stats.status_code == 200
        assert meta.json()['base_path'] == '/monitoring'
        await monitoring_app.state.events.close()


# --------------------------------------------------------------------------- #
# Schema compatibility (spec 7.5b)
# --------------------------------------------------------------------------- #
def web_args(**overrides: Any) -> Any:
    """A namespace shaped like the web subparser's output."""
    import argparse

    values: dict[str, Any] = {
        'module': None,
        'database_url': None,
        'session_database_url': None,
        'pgbouncer_transaction_mode': False,
        'host': '127.0.0.1',
        'port': 8600,
        'auth': 'none',
        'trusted_header': 'X-Forwarded-User',
        'enable_actions': False,
        'log_level': 'INFO',
    }
    values.update(overrides)
    return argparse.Namespace(**values)


@pytest_asyncio.fixture
async def fresh_database() -> AsyncGenerator[str, None]:
    """A brand-new database with no horsies schema in it."""
    name = f'{database_name_prefix()}noddl_{uuid.uuid4().hex[:12]}'
    admin = to_psycopg_url(DB_URL)

    connection = await psycopg.AsyncConnection.connect(admin, autocommit=True)
    try:
        await connection.execute(
            sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name))
        )
    finally:
        await connection.close()

    base, _, _old = DB_URL.rpartition('/')
    yield f'{base}/{name}'

    connection = await psycopg.AsyncConnection.connect(admin, autocommit=True)
    try:
        await connection.execute(
            sql.SQL('DROP DATABASE IF EXISTS {} WITH (FORCE)').format(
                sql.Identifier(name)
            )
        )
    finally:
        await connection.close()


async def relation_names(database_url: str) -> tuple[Any, Any]:
    """Whether the core horsies relations exist in a database."""
    connection = await psycopg.AsyncConnection.connect(
        to_psycopg_url(database_url), autocommit=True
    )
    try:
        cursor = await connection.execute(
            "SELECT to_regclass('horsies_tasks'), to_regclass('horsies_schema_version')"
        )
        row = await cursor.fetchone()
        assert row is not None
        return (row[0], row[1])
    finally:
        await connection.close()


class TestTheToolNeverRunsDdl:
    """Pointing the command at a database must never migrate it."""

    async def test_an_enabled_broker_would_have_created_the_schema(
        self, fresh_database: str
    ) -> None:
        """Control: the same call on a normal broker does migrate.

        Without this, the zero-DDL assertion above would also hold if the
        call simply never reached the migration seam.
        """
        enabled = cli.resolve_web_app(web_args(database_url=fresh_database))
        enabled.schema_mode = SchemaMode.MIGRATE_AND_VALIDATE

        initialized = await enabled.get_broker().ensure_schema_initialized()

        assert not is_err(initialized)
        tasks_relation, version_relation = await relation_names(fresh_database)
        assert tasks_relation is not None
        assert version_relation is not None
        await enabled.get_broker().close_async()

    async def test_meta_reports_the_absent_schema(self, fresh_database: str) -> None:
        cli_app = cli.resolve_web_app(web_args(database_url=fresh_database))
        built = create_monitoring_app(cli_app, auth_policy=AllowAll())

        async with AsyncClient(
            transport=ASGITransport(app=built), base_url='http://test'
        ) as client:
            body = (await client.get('/api/meta')).json()

        assert body['schema_version'] is None
        assert body['schema_compatible'] is False
        assert body['actions_enabled'] is False
        # Reachable but empty is ABSENT, not UNKNOWN: the operator is told to
        # start a worker, which is the correct instruction here.
        assert body['actions_disabled_reason'] == 'SCHEMA_INCOMPATIBLE'
        await built.state.events.close()
        await cli_app.get_broker().close_async()

    async def test_actions_are_refused_and_still_nothing_is_created(
        self, fresh_database: str
    ) -> None:
        cli_app = cli.resolve_web_app(web_args(database_url=fresh_database))
        built = create_monitoring_app(cli_app, auth_policy=AllowAll())

        async with AsyncClient(
            transport=ASGITransport(app=built), base_url='http://test'
        ) as client:
            response = await client.post(
                f'/api/tasks/{uuid.uuid4()}/cancel', headers=ACT
            )

        assert response.status_code == 409
        body = response.json()
        assert body['code'] == 'SCHEMA_INCOMPATIBLE'
        assert 'no horsies schema' in body['detail']
        assert await relation_names(fresh_database) == (None, None)
        await built.state.events.close()
        await cli_app.get_broker().close_async()


async def run_statements(database_url: str, *statements: LiteralString) -> None:
    """Run fixed statements in one autocommit connection."""
    connection = await psycopg.AsyncConnection.connect(
        to_psycopg_url(database_url), autocommit=True
    )
    try:
        for statement in statements:
            await connection.execute(statement)
    finally:
        await connection.close()


# The bookkeeping and one table of a horsies 0.x Python database (0.7.0, v61).
_ZERO_X_PYTHON_SCHEMA: tuple[LiteralString, ...] = (
    'CREATE TABLE horsies_schema_version '
    '(version integer PRIMARY KEY, applied_at timestamptz NOT NULL)',
    'INSERT INTO horsies_schema_version VALUES (61, now())',
    'CREATE TABLE horsies_tasks (id uuid PRIMARY KEY)',
)

# The bookkeeping of a horsies-rust 0.x database: sqlx rows up to version 65.
_ZERO_X_RUST_SCHEMA: tuple[LiteralString, ...] = (
    'CREATE TABLE horsies_migrations (version bigint PRIMARY KEY, '
    'description text NOT NULL, installed_on timestamptz NOT NULL, '
    'success boolean NOT NULL, checksum bytea NOT NULL, '
    'execution_time bigint NOT NULL)',
    "INSERT INTO horsies_migrations VALUES (65, 'rust 0.x', now(), TRUE, "
    "'\\x00'::bytea, 1)",
)


class TestZeroXSchema:
    """A horsies 0.x database has its own state: the remedy is a new database."""

    async def _meta(self, database_url: str) -> dict[str, Any]:
        cli_app = cli.resolve_web_app(web_args(database_url=database_url))
        built = create_monitoring_app(cli_app, auth_policy=AllowAll())
        try:
            async with AsyncClient(
                transport=ASGITransport(app=built), base_url='http://test'
            ) as client:
                body: dict[str, Any] = (await client.get('/api/meta')).json()
        finally:
            await built.state.events.close()
            await cli_app.get_broker().close_async()
        return body

    async def test_meta_reports_a_python_0x_schema(self, fresh_database: str) -> None:
        await run_statements(fresh_database, *_ZERO_X_PYTHON_SCHEMA)

        body = await self._meta(fresh_database)

        assert body['schema_state'] == 'ZERO_X'
        assert body['schema_version'] is None
        assert body['schema_compatible'] is False
        assert body['actions_enabled'] is False
        assert body['actions_disabled_reason'] == 'SCHEMA_INCOMPATIBLE'

    async def test_meta_reports_a_rust_0x_schema(self, fresh_database: str) -> None:
        await run_statements(fresh_database, *_ZERO_X_RUST_SCHEMA)

        body = await self._meta(fresh_database)

        assert body['schema_state'] == 'ZERO_X'
        assert body['schema_version'] == 65
        assert body['actions_disabled_reason'] == 'SCHEMA_INCOMPATIBLE'

    async def test_actions_are_refused_and_nothing_is_changed(
        self, fresh_database: str
    ) -> None:
        await run_statements(fresh_database, *_ZERO_X_PYTHON_SCHEMA)
        cli_app = cli.resolve_web_app(web_args(database_url=fresh_database))
        built = create_monitoring_app(cli_app, auth_policy=AllowAll())

        async with AsyncClient(
            transport=ASGITransport(app=built), base_url='http://test'
        ) as client:
            response = await client.post(
                f'/api/tasks/{uuid.uuid4()}/cancel', headers=ACT
            )

        assert response.status_code == 409
        body = response.json()
        assert body['code'] == 'SCHEMA_INCOMPATIBLE'
        assert 'horsies 0.x schema' in body['detail']
        assert 'Start a horsies app or worker' not in body['detail']
        tasks_relation, version_relation = await relation_names(fresh_database)
        assert tasks_relation is not None
        assert version_relation is not None
        await built.state.events.close()
        await cli_app.get_broker().close_async()


@pytest.mark.usefixtures('clean_tables')
class TestSchemaMatch:
    """A database at the expected version is fully usable."""

    async def test_meta_round_trips_the_stored_version(self, api: AsyncClient) -> None:
        body = (await api.get('/api/meta')).json()

        assert body['schema_version'] == schema_module.EXPECTED_SCHEMA_VERSION
        assert body['expected_schema_version'] == schema_module.EXPECTED_SCHEMA_VERSION
        assert body['schema_compatible'] is True
        assert body['actions_enabled'] is True
        assert body['actions_disabled_reason'] is None

    async def test_actions_are_permitted(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)

        response = await api.post(f'/api/tasks/{task.id}/cancel', headers=ACT)

        assert response.status_code == 200


LATER_SCHEMA_VERSION = schema_module.EXPECTED_SCHEMA_VERSION + 1
"""A schema version that a later build records."""


@pytest.mark.usefixtures('clean_tables')
class TestSchemaMismatch:
    """A version step disables writes without disabling the dashboard."""

    @pytest_asyncio.fixture(autouse=True)
    async def a_later_version_is_recorded(
        self,
        session: AsyncSession,
        broker: PostgresBroker,  # noqa: ARG002 - installs the schema first
    ) -> AsyncGenerator[None, None]:
        """Record a successful migration past the version this build expects,
        as a newer build would; removed after the test.

        The install runs before the record (``broker``): the install refuses
        a database that records a version later than its build.
        """
        await session.execute(
            text(
                'INSERT INTO horsies_migrations (version, description, installed_on, '
                'success, checksum, execution_time) VALUES '
                "(:version, 'a later build', now(), TRUE, '\\x00'::bytea, 1)"
            ),
            {'version': LATER_SCHEMA_VERSION},
        )
        await session.commit()
        yield
        await session.execute(
            text('DELETE FROM horsies_migrations WHERE version = :version'),
            {'version': LATER_SCHEMA_VERSION},
        )
        await session.commit()

    async def test_meta_reports_the_mismatch(self, api: AsyncClient) -> None:
        body = (await api.get('/api/meta')).json()

        assert body['schema_version'] == LATER_SCHEMA_VERSION
        assert body['expected_schema_version'] == schema_module.EXPECTED_SCHEMA_VERSION
        assert body['schema_state'] == 'MISMATCH'
        assert body['schema_compatible'] is False
        assert body['actions_enabled'] is False
        assert body['actions_disabled_reason'] == 'SCHEMA_INCOMPATIBLE'

    @pytest.mark.parametrize(
        'path',
        [
            '/api/tasks/{id}/cancel',
            '/api/workflows/{id}/cancel',
            '/api/workflows/{id}/pause',
            '/api/workflows/{id}/resume',
        ],
    )
    async def test_every_action_endpoint_is_refused(
        self, api: AsyncClient, path: str
    ) -> None:
        response = await api.post(path.format(id=uuid.uuid4()), headers=ACT)

        assert response.status_code == 409
        body = response.json()
        assert body['code'] == 'SCHEMA_INCOMPATIBLE'
        assert 'detail' in body

    async def test_the_refusal_beats_a_permissive_policy(
        self, app: Horsies, session: AsyncSession
    ) -> None:
        """Force-disabled server-side means regardless of authorization."""
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)
        built = create_monitoring_app(app, auth_policy=AllowAll(), actions_enabled=True)

        async with AsyncClient(
            transport=ASGITransport(app=built), base_url='http://test'
        ) as client:
            response = await client.post(f'/api/tasks/{task.id}/cancel', headers=ACT)

        assert response.status_code == 409
        row = (
            await session.execute(
                text('SELECT status FROM horsies_tasks WHERE id = :id'),
                {'id': task.id},
            )
        ).first()
        assert row is not None
        assert row[0] == 'PENDING'
        await built.state.events.close()

    async def test_reads_are_still_served(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        """A mismatch is read-only mode, not an outage."""
        await persist(session, make_task())

        response = await api.get('/api/tasks')

        assert response.status_code == 200
        assert response.json()['total'] == 1
