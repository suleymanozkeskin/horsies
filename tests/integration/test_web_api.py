"""Integration tests for the monitoring HTTP API.

Runs the real app over httpx's ASGI transport against a real database, so the
wiring between routes, the query package, and the task/workflow primitives is
exercised end to end. Coverage:

1. read routes — envelopes, 404 copy, and the vanished-backing-task case
2. worker routes — snapshots, liveness, schedules, unknown-worker history
3. the section 7.4 action mapping table, row by row
4. the resume committed-then-failed ambiguity, resolved server-side
5. the SSE event layer — a real NOTIFY, heartbeats, and the degraded path
6. schema compatibility (spec 7.5b) — the tool never runs DDL, and refuses to
   write through a schema it does not recognize
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator

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
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationError
from horsies.core.models.task_pg import TaskModel
from horsies.core.models.workflow_pg import WorkflowModel, WorkflowTaskModel
from horsies.core.types.result import Err, Ok, is_err
from horsies.core.types.status import TaskStatus
from horsies.web import INTENT_HEADER, INTENT_VALUE, AllowAll, create_monitoring_app
from horsies.web import events as events_module
from horsies.web.routes import actions as actions_module
from horsies.web.routes import events as events_route_module
from horsies.core.utils.url import to_psycopg_url
from horsies.web import schema as schema_module
from tests.integration.conftest import DB_URL, compute_test_enqueue_sha

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

UTC = timezone.utc
ACT = {INTENT_HEADER: INTENT_VALUE}


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
            'horsies_schedule_state CASCADE'
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


class AsgiEventStream:
    """Reads a streaming ASGI response while it is still open.

    httpx's ASGI transport accumulates the whole body before returning a
    response, so an endless event stream deadlocks when read through it.
    Speaking ASGI directly is what makes the stream observable — and it still
    exercises the real app: routing, dependencies, and SSE framing.
    """

    def __init__(
        self, app: Any, path: str, *, headers: dict[str, str] | None = None
    ) -> None:
        self._app = app
        self._path = path
        self._headers = headers or {}
        self._chunks: asyncio.Queue[bytes] = asyncio.Queue()
        self._disconnect = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._start: asyncio.Future[dict[str, Any]] | None = None
        self._buffer = ''

    async def __aenter__(self) -> AsgiEventStream:
        self._start = asyncio.get_running_loop().create_future()
        self._task = asyncio.create_task(self._run())
        await asyncio.wait_for(asyncio.shield(self._start), timeout=20)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        self._disconnect.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    @property
    def status_code(self) -> int:
        assert self._start is not None
        return int(self._start.result()['status'])

    @property
    def headers(self) -> dict[str, str]:
        assert self._start is not None
        return {
            key.decode().lower(): value.decode()
            for key, value in self._start.result()['headers']
        }

    async def _run(self) -> None:
        scope: dict[str, Any] = {
            'type': 'http',
            'asgi': {'version': '3.0', 'spec_version': '2.3'},
            'http_version': '1.1',
            'method': 'GET',
            'scheme': 'http',
            'path': self._path,
            'raw_path': self._path.encode(),
            'query_string': b'',
            'root_path': '',
            'headers': [
                (key.lower().encode(), value.encode())
                for key, value in self._headers.items()
            ],
            'client': ('127.0.0.1', 12345),
            'server': ('testserver', 80),
        }

        async def receive() -> dict[str, Any]:
            await self._disconnect.wait()
            return {'type': 'http.disconnect'}

        async def send(message: dict[str, Any]) -> None:
            if message['type'] == 'http.response.start':
                assert self._start is not None
                if not self._start.done():
                    self._start.set_result(message)
            elif message['type'] == 'http.response.body':
                await self._chunks.put(bytes(message.get('body', b'')))

        await self._app(scope, receive, send)

    async def next_line(self, *, timeout_s: float) -> str:
        while '\n' not in self._buffer:
            chunk = await asyncio.wait_for(self._chunks.get(), timeout=timeout_s)
            self._buffer += chunk.decode()
        line, _, rest = self._buffer.partition('\n')
        self._buffer = rest
        return line

    async def next_data(self, *, timeout_s: float) -> dict[str, Any]:
        while True:
            line = await self.next_line(timeout_s=timeout_s)
            if line.startswith('data: '):
                parsed: dict[str, Any] = json.loads(line.removeprefix('data: '))
                return parsed

    async def next_comment(self, *, timeout_s: float) -> str:
        while True:
            line = await self.next_line(timeout_s=timeout_s)
            if line.startswith(':'):
                return line


def _terminal_instant(
    status: TaskStatus,
    completed_at: datetime | None,
    failed_at: datetime | None,
) -> datetime | None:
    """The instant a terminal row is dated by, mirroring what production writes.

    Terminal exactly when dated is a database constraint, so a fixture that
    sets a terminal status without one is not a lighter fixture — it is a row
    that cannot exist. The instant is taken from whichever end timestamp the
    row carries, which keeps aged fixtures aged: a row completed sixty days ago
    is terminal sixty days ago, not now.
    """
    if not status.is_terminal:
        return None
    return completed_at or failed_at or datetime.now(UTC)


def make_task(
    *,
    task_name: str = 'api_task',
    queue_name: str = 'default',
    status: TaskStatus = TaskStatus.PENDING,
    is_workflow_task: bool = False,
    good_until: datetime | None = None,
    error_code: str | None = None,
) -> TaskModel:
    """Build a task row."""
    sent_at, enqueue_sha = compute_test_enqueue_sha(
        task_name=task_name, queue_name=queue_name
    )
    return TaskModel(
        id=str(uuid.uuid4()),
        task_name=task_name,
        queue_name=queue_name,
        priority=100,
        args='[]',
        kwargs='{}',
        status=status,
        sent_at=sent_at,
        terminal_at=_terminal_instant(status, None, None),
        enqueued_at=datetime.now(UTC) - timedelta(seconds=60),
        good_until=good_until,
        error_code=error_code,
        claimed=False,
        retry_count=0,
        max_retries=3,
        is_workflow_task=is_workflow_task,
        enqueue_sha=enqueue_sha,
    )


def make_workflow(*, name: str = 'api_flow', status: str = 'RUNNING') -> WorkflowModel:
    """Build a workflow run row."""
    return WorkflowModel(
        id=str(uuid.uuid4()),
        name=name,
        status=status,
        on_error='fail',
        depth=0,
        created_at=datetime.now(UTC) - timedelta(seconds=120),
    )


async def persist(session: AsyncSession, *rows: Any) -> None:
    """Persist rows and commit."""
    session.add_all(list(rows))
    await session.commit()


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


class FailingListener:
    """A listener that cannot connect, for the degraded-stream path."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

    async def start(self) -> Err[BrokerOperationError]:
        return Err(
            BrokerOperationError(
                code=BrokerErrorCode.LISTENER_START_FAILED,
                message='stubbed listener failure',
                retryable=True,
            )
        )

    async def close(self) -> None:
        return None


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
            WorkflowTaskModel(
                id=str(uuid.uuid4()),
                workflow_id=run.id,
                task_index=0,
                task_name='step',
                queue_name='default',
                priority=100,
                dependencies=[],
                allow_failed_deps=False,
                join_type='all',
                status='FAILED',
                is_subworkflow=False,
            ),
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
            WorkflowTaskModel(
                id=str(uuid.uuid4()),
                workflow_id=run.id,
                task_index=0,
                task_name='step',
                queue_name='default',
                priority=100,
                dependencies=[],
                allow_failed_deps=False,
                join_type='all',
                status='COMPLETED',
                task_id=str(uuid.uuid4()),
                is_subworkflow=False,
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

    async def test_schedules_answer(self, api: AsyncClient) -> None:
        response = await api.get('/api/workers/schedules')

        assert response.status_code == 200
        assert response.json() == []

    async def test_liveness_reports_a_reachable_database(
        self, api: AsyncClient
    ) -> None:
        """An unreachable database would be data here, not an error."""
        body = (await api.get('/api/workers/ping?timeout_seconds=0.1')).json()

        assert body['db_reachable'] is True
        assert body['db_latency_ms'] is not None
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

    async def test_retry_ok_reports_the_next_attempt_number(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.FAILED)
        await persist(session, task)

        body = (await api.post(f'/api/tasks/{task.id}/retry', headers=ACT)).json()

        assert body['outcome'] == 'retried'
        assert body['was_status'] == 'FAILED'
        assert body['next_attempt_number'] == 1

    async def test_missing_task_is_not_found(self, api: AsyncClient) -> None:
        response = await api.post(f'/api/tasks/{uuid.uuid4()}/cancel', headers=ACT)

        assert response.status_code == 404
        assert 'detail' in response.json()

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

    async def test_unretryable_task_is_a_conflict(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)

        response = await api.post(f'/api/tasks/{task.id}/retry', headers=ACT)

        assert response.status_code == 409
        assert response.json() == {
            'code': 'TASK_NOT_RETRYABLE',
            'current_status': 'PENDING',
        }

    async def test_expired_task_is_a_conflict(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        task = make_task(
            status=TaskStatus.FAILED,
            good_until=datetime.now(UTC) - timedelta(minutes=5),
        )
        await persist(session, task)

        response = await api.post(f'/api/tasks/{task.id}/retry', headers=ACT)

        assert response.status_code == 409
        assert response.json()['code'] == 'TASK_EXPIRY_PASSED'

    async def test_workflow_bound_task_is_a_bad_request(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        """The UI hides these buttons; reaching here means a stale client."""
        task = make_task(status=TaskStatus.FAILED, is_workflow_task=True)
        await persist(session, task)

        response = await api.post(f'/api/tasks/{task.id}/retry', headers=ACT)

        assert response.status_code == 400
        assert response.json() == {'code': 'TASK_IS_WORKFLOW_TASK'}

    async def test_running_task_needs_the_explicit_body_flag(
        self, api: AsyncClient, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.RUNNING)
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


@pytest.mark.usefixtures('clean_tables')
class TestResumeCommittedThenFailed:
    """Resume's recovery pass runs after the state change commits.

    A failure returned by resume therefore does not mean the resume did not
    happen. The server re-reads the run rather than handing the ambiguity to
    the browser.
    """

    async def test_failure_with_a_running_run_is_success_plus_a_warning(
        self,
        api: AsyncClient,
        session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        run = make_workflow(status='RUNNING')
        await persist(session, run)

        async def failing_resume(*_args: Any, **_kwargs: Any) -> Any:
            from horsies.core.models.workflow.handle_types import (
                HandleErrorCode,
                HandleOperationError,
            )

            return Err(
                HandleOperationError(
                    code=HandleErrorCode.DB_OPERATION_FAILED,
                    message='recovery pass failed',
                    retryable=True,
                    workflow_id=run.id,
                )
            )

        monkeypatch.setattr(actions_module, 'resume_workflow', failing_resume)

        response = await api.post(f'/api/workflows/{run.id}/resume', headers=ACT)

        assert response.status_code == 200
        assert response.json()['outcome'] == 'resumed'
        assert response.json()['warning'] == 'post_resume_recovery_failed'

    async def test_failure_with_a_still_paused_run_is_service_unavailable(
        self,
        api: AsyncClient,
        session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        run = make_workflow(status='PAUSED')
        await persist(session, run)

        async def failing_resume(*_args: Any, **_kwargs: Any) -> Any:
            from horsies.core.models.workflow.handle_types import (
                HandleErrorCode,
                HandleOperationError,
            )

            return Err(
                HandleOperationError(
                    code=HandleErrorCode.DB_OPERATION_FAILED,
                    message='resume failed outright',
                    retryable=True,
                    workflow_id=run.id,
                )
            )

        monkeypatch.setattr(actions_module, 'resume_workflow', failing_resume)

        response = await api.post(f'/api/workflows/{run.id}/resume', headers=ACT)

        assert response.status_code == 503

    async def test_pause_infrastructure_failure_is_service_unavailable(
        self,
        api: AsyncClient,
        session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        run = make_workflow(status='RUNNING')
        await persist(session, run)

        async def failing_pause(*_args: Any, **_kwargs: Any) -> Any:
            from horsies.core.models.workflow.handle_types import (
                HandleErrorCode,
                HandleOperationError,
            )

            return Err(
                HandleOperationError(
                    code=HandleErrorCode.DB_OPERATION_FAILED,
                    message='pause failed',
                    retryable=True,
                    workflow_id=run.id,
                )
            )

        monkeypatch.setattr(actions_module, 'pause_workflow', failing_pause)

        response = await api.post(f'/api/workflows/{run.id}/pause', headers=ACT)

        assert response.status_code == 503

    async def test_pause_state_conflict_reports_the_observed_status(
        self,
        api: AsyncClient,
        session: AsyncSession,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ok(False) with a live run: the CAS lost, the client refetches."""
        run = make_workflow(status='RUNNING')
        await persist(session, run)

        async def noop_pause(*_args: Any, **_kwargs: Any) -> Any:
            return Ok(False)

        monkeypatch.setattr(actions_module, 'pause_workflow', noop_pause)

        response = await api.post(f'/api/workflows/{run.id}/pause', headers=ACT)

        assert response.status_code == 409
        assert response.json()['current_status'] == 'RUNNING'


# --------------------------------------------------------------------------- #
# Event stream (spec 7.5)
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_tables')
class TestEventStream:
    """Invalidation events, heartbeats, and the honest degraded path."""

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

    async def test_an_idle_stream_sends_heartbeats(
        self, monitoring: FastAPI, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Comment frames keep a proxy from reaping an idle connection."""
        monkeypatch.setattr(events_route_module, 'HEARTBEAT_SECONDS', 0.2)

        async with AsgiEventStream(monitoring, '/api/events') as stream:
            comment = await stream.next_comment(timeout_s=15)

        assert comment.startswith(': heartbeat')

    async def test_a_failed_listener_degrades_instead_of_pretending(
        self, app: Horsies, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The server never fabricates events; the client falls back to polling."""
        monkeypatch.setattr(events_module, 'PostgresListener', FailingListener)
        built = create_monitoring_app(app, auth_policy=AllowAll())

        async with AsgiEventStream(built, '/api/events') as stream:
            frame = await stream.next_data(timeout_s=15)

        assert frame == {'topic': 'degraded'}
        await built.state.events.close()


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
        'app_path': None,
        'database_url': None,
        'session_database_url': None,
        'pgbouncer_transaction_mode': False,
        'host': '127.0.0.1',
        'port': 8600,
        'auth': 'none',
        'trusted_header': 'X-Forwarded-User',
        'enable_actions': False,
        'loglevel': 'INFO',
    }
    values.update(overrides)
    return argparse.Namespace(**values)


@pytest_asyncio.fixture
async def fresh_database() -> AsyncGenerator[str, None]:
    """A brand-new database with no horsies schema in it."""
    name = f'horsies_noddl_{uuid.uuid4().hex[:12]}'
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

    async def test_cli_app_creates_nothing_and_reports_absent(
        self, fresh_database: str
    ) -> None:
        cli_app = cli.resolve_web_app(web_args(database_url=fresh_database))
        assert cli_app.get_broker().run_schema_migrations is False

        initialized = await cli_app.get_broker().ensure_schema_initialized()

        assert not is_err(initialized)
        assert await relation_names(fresh_database) == (None, None)
        await cli_app.get_broker().close_async()

    async def test_an_enabled_broker_would_have_created_the_schema(
        self, fresh_database: str
    ) -> None:
        """Control: the same call on a normal broker does migrate.

        Without this, the zero-DDL assertion above would also hold if the
        call simply never reached the migration seam.
        """
        enabled = cli.resolve_web_app(web_args(database_url=fresh_database))
        enabled.run_schema_migrations = True

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


@pytest.mark.usefixtures('clean_tables')
class TestSchemaMatch:
    """A database at the expected version is fully usable."""

    async def test_meta_round_trips_the_stored_version(self, api: AsyncClient) -> None:
        body = (await api.get('/api/meta')).json()

        assert body['schema_version'] == schema_module.SCHEMA_VERSION
        assert body['expected_schema_version'] == schema_module.SCHEMA_VERSION
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


@pytest.mark.usefixtures('clean_tables')
class TestSchemaMismatch:
    """A version step disables writes without disabling the dashboard."""

    @pytest.fixture(autouse=True)
    def expect_a_different_version(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Make this build expect a version the database does not carry."""
        monkeypatch.setattr(schema_module, 'SCHEMA_VERSION', 9999)

    async def test_meta_reports_the_mismatch(self, api: AsyncClient) -> None:
        body = (await api.get('/api/meta')).json()

        assert body['schema_version'] is not None
        assert body['expected_schema_version'] == 9999
        assert body['schema_compatible'] is False
        assert body['actions_enabled'] is False
        assert body['actions_disabled_reason'] == 'SCHEMA_INCOMPATIBLE'

    @pytest.mark.parametrize(
        'path',
        [
            '/api/tasks/{id}/cancel',
            '/api/tasks/{id}/retry',
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
