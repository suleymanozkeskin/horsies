"""Integration test fixtures for workflow tests."""

from __future__ import annotations

import os
from typing import Any, AsyncGenerator, Generator
import pytest
import pytest_asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.schedule import ScheduleConfig  # noqa: F401 - needed for AppConfig.model_rebuild()

# Rebuild AppConfig to resolve forward references
AppConfig.model_rebuild()
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import (
    TaskNode,
    WorkflowSpec,
    WorkflowContext,
    OnError,
    SuccessPolicy,
    SuccessCase,
)
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.workflows.engine import start_workflow_async
from horsies.core.types.result import is_err


# Database URL
DB_URL = f'postgresql+psycopg://postgres:{os.environ["DB_PASSWORD"]}@localhost:5432/horsies'


@pytest.fixture(scope='session')
def db_url() -> str:
    """Database connection URL."""
    return DB_URL


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def engine(db_url: str) -> AsyncGenerator[AsyncEngine, None]:
    """SQLAlchemy async engine (session-scoped)."""
    eng = create_async_engine(db_url, echo=False)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session(engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """Database session for direct queries."""
    async with AsyncSession(engine, expire_on_commit=False) as sess:
        yield sess


@pytest_asyncio.fixture
async def broker() -> AsyncGenerator[PostgresBroker, None]:
    """PostgresBroker instance with schema initialized."""
    config = PostgresConfig(database_url=DB_URL)
    brk = PostgresBroker(config)
    await brk.ensure_schema_initialized()
    yield brk
    await brk.close_async()


@pytest.fixture
def app_config() -> AppConfig:
    """App configuration for tests."""
    return AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(database_url=DB_URL),
    )


@pytest.fixture
def app(app_config: AppConfig, broker: PostgresBroker) -> Horsies:
    """Horsies app configured with test broker."""
    horsies_app = Horsies(app_config)
    horsies_app._broker = broker
    broker.app = horsies_app
    return horsies_app


@pytest_asyncio.fixture
async def clean_workflow_tables(session: AsyncSession) -> AsyncGenerator[None, None]:
    """Truncate workflow tables before each test."""
    await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
    await session.commit()
    yield


# =============================================================================
# Sample Task Functions (registered per-test as needed)
# =============================================================================


def make_simple_task(app: Horsies, name: str = 'simple_task') -> Any:
    """Create a simple task that doubles its input."""

    @app.task(task_name=name)
    def simple_task(value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value * 2)

    return simple_task


def make_simple_ctx_task(app: Horsies, name: str = 'simple_ctx_task') -> Any:
    """Create a simple task that accepts workflow_ctx and doubles its input."""

    @app.task(task_name=name)
    def simple_ctx_task(
        value: int,
        workflow_ctx: WorkflowContext | None = None,
    ) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value * 2)

    return simple_ctx_task


def make_failing_task(app: Horsies, name: str = 'failing_task') -> Any:
    """Create a task that always fails."""

    @app.task(task_name=name)
    def failing_task() -> TaskResult[Any, TaskError]:
        return TaskResult(
            err=TaskError(error_code='DELIBERATE_FAIL', message='Test failure')
        )

    return failing_task


def make_identity_task(app: Horsies, name: str = 'identity_task') -> Any:
    """Create a task that returns its input unchanged."""

    @app.task(task_name=name)
    def identity_task(value: Any = None) -> TaskResult[Any, TaskError]:
        return TaskResult(ok=value)

    return identity_task


def make_args_receiver_task(app: Horsies, name: str = 'args_receiver') -> Any:
    """Create a task that receives args_from injection."""

    @app.task(task_name=name)
    def args_receiver(
        input_result: TaskResult[int, TaskError],
    ) -> TaskResult[int, TaskError]:
        if input_result.is_err():
            return TaskResult(
                err=TaskError(
                    error_code='UPSTREAM_FAILED',
                    message='Upstream task failed',
                    data={'upstream_error': input_result.err},
                )
            )
        return TaskResult(ok=input_result.unwrap() + 10)

    return args_receiver


def make_recovery_task(app: Horsies, name: str = 'recovery_task') -> Any:
    """Create a task that can recover from upstream failures."""

    @app.task(task_name=name)
    def recovery_task(
        input_result: TaskResult[int, TaskError],
    ) -> TaskResult[int, TaskError]:
        if input_result.is_err():
            # Recover by returning a default value
            return TaskResult(ok=999)
        return TaskResult(ok=input_result.unwrap())

    return recovery_task


def make_ctx_receiver_task(app: Horsies, name: str = 'ctx_receiver') -> Any:
    """Create a task that receives workflow_ctx."""

    @app.task(task_name=name)
    def ctx_receiver(
        workflow_ctx: WorkflowContext | None = None,
    ) -> TaskResult[dict[str, Any], TaskError]:
        if workflow_ctx is None:
            return TaskResult(ok={'ctx': None})
        return TaskResult(
            ok={
                'workflow_id': workflow_ctx.workflow_id,
                'task_index': workflow_ctx.task_index,
                'task_name': workflow_ctx.task_name,
                'result_keys': workflow_ctx._results_by_id.keys(),
            }
        )

    return ctx_receiver


def make_no_ctx_task(app: Horsies, name: str = 'no_ctx_task') -> Any:
    """Create a task that does NOT declare workflow_ctx param."""

    @app.task(task_name=name)
    def no_ctx_task(value: int = 0) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value + 1)

    return no_ctx_task


def make_multi_args_receiver_task(
    app: Horsies, name: str = 'multi_args_receiver'
) -> Any:
    """Create a task that receives multiple args_from injections."""

    @app.task(task_name=name)
    def multi_args_receiver(
        first: TaskResult[int, TaskError],
        second: TaskResult[int, TaskError],
    ) -> TaskResult[int, TaskError]:
        if first.is_err() or second.is_err():
            return TaskResult(
                err=TaskError(
                    error_code='UPSTREAM_FAILED', message='One or more upstreams failed'
                )
            )
        return TaskResult(ok=first.unwrap() + second.unwrap())

    return multi_args_receiver


def make_retryable_task(
    app: Horsies,
    name: str = 'retryable_task',
    max_retries: int = 3,
) -> Any:
    """Create a task with a retry policy configured."""
    from horsies.core.models.tasks import RetryPolicy

    # Create fixed intervals list matching max_retries count
    intervals = [60] * max_retries

    @app.task(
        task_name=name,
        retry_policy=RetryPolicy.fixed(intervals, auto_retry_for=['TASK_EXCEPTION']),
    )
    def retryable_task(value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value * 2)

    return retryable_task


# =============================================================================
# Workflow Spec Helper
# =============================================================================


async def start_ok(
    spec: WorkflowSpec[Any],
    broker: PostgresBroker,
    workflow_id: str | None = None,
) -> 'WorkflowHandle[Any]':
    """Unwrap start result or fail test with error details."""
    from horsies.core.models.workflow import WorkflowHandle

    r = await start_workflow_async(spec, broker, workflow_id)
    if is_err(r):
        pytest.fail(f'start_workflow_async failed: {r.err_value}')
    return r.ok_value


async def complete_task(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
    result: TaskResult[Any, TaskError],
) -> None:
    """Simulate task completion by looking up task_id and calling on_workflow_task_complete."""
    from horsies.core.workflows.engine import on_workflow_task_complete

    res = await session.execute(
        text("""
            SELECT task_id FROM horsies_workflow_tasks
            WHERE workflow_id = :wf_id AND task_index = :idx
        """),
        {'wf_id': workflow_id, 'idx': task_index},
    )
    row = res.fetchone()
    if row and row[0]:
        await on_workflow_task_complete(session, row[0], result)
        await session.commit()


async def get_task_status(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
) -> str:
    """Get workflow task status by index."""
    result = await session.execute(
        text("""
            SELECT status FROM horsies_workflow_tasks
            WHERE workflow_id = :wf_id AND task_index = :idx
        """),
        {'wf_id': workflow_id, 'idx': task_index},
    )
    row = result.fetchone()
    return row[0] if row else 'NOT_FOUND'


async def get_workflow_status(
    session: AsyncSession,
    workflow_id: str,
) -> str:
    """Get workflow status by id."""
    result = await session.execute(
        text("""SELECT status FROM horsies_workflows WHERE id = :wf_id"""),
        {'wf_id': workflow_id},
    )
    row = result.fetchone()
    return row[0] if row else 'NOT_FOUND'


def make_workflow_spec(
    name: str,
    tasks: list[TaskNode[Any]],
    broker: PostgresBroker,
    on_error: OnError = OnError.FAIL,
    output: TaskNode[Any] | None = None,
    success_policy: SuccessPolicy | None = None,
) -> WorkflowSpec:
    """
    Create a WorkflowSpec with resolved queue/priority for testing.

    This is a test helper that mimics what app.workflow() does, but without
    needing a full Horsies app for every test.
    """
    # Resolve queue and priority for each task (mimics app.workflow() behavior)
    for task in tasks:
        if task.queue is None:
            task.queue = getattr(task.fn, 'task_queue_name', None) or 'default'
        if task.priority is None:
            task.priority = 100  # DEFAULT mode priority

    spec = WorkflowSpec(
        name=name,
        tasks=tasks,
        on_error=on_error,
        output=output,
        success_policy=success_policy,
        broker=broker,
    )
    return spec
