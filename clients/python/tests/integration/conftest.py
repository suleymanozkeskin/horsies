"""Integration test fixtures for workflow tests."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, AsyncGenerator, Generator, Literal
import pytest
import pytest_asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.schedule import ScheduleConfig  # noqa: F401 - needed for AppConfig.model_rebuild()
from tests.broker_resources_support import (
    current_resources,
    read_uuid_columns_as_text,
)
from tests.session_database import suite_database_url

# Rebuild AppConfig to resolve forward references
AppConfig.model_rebuild()
from horsies.core.codec.json_value import JsonValue
from horsies.core.models.outcome import Failed, Succeeded, TaskOutcome
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import (
    TaskNode,
    WorkflowHandle,
    WorkflowSpec,
    WorkflowContext,
    OnError,
    SuccessPolicy,
    SuccessCase,
)
import horsies._core as core
from horsies.core.brokers.postgres import PostgresBroker
from tests.fixtures.child_core import (
    drop_child_core_handle,
    install_child_core_handle,
)
from horsies.core.workflows.engine import start_workflow_async
from horsies.core.types.result import Err, Ok, Result, is_err
from tests.sqlalchemy_url import sqlalchemy_url


# A fresh database for this session (tests/session_database.py): the install
# refuses a database that holds another schema.
DB_URL = suite_database_url()


@pytest.fixture(scope='session')
def db_url() -> str:
    """Database connection URL."""
    return DB_URL


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def engine(db_url: str) -> AsyncGenerator[AsyncEngine, None]:
    """SQLAlchemy async engine (session-scoped)."""
    eng = create_async_engine(sqlalchemy_url(db_url), echo=False)
    read_uuid_columns_as_text(eng)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session(engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """Database session for direct queries."""
    async with AsyncSession(engine, expire_on_commit=False) as sess:
        yield sess


@pytest_asyncio.fixture
async def broker() -> AsyncGenerator[PostgresBroker, None]:
    """PostgresBroker instance with schema initialized.

    Also runs the idempotent coverage/publication sequence (default
    retention class, history leaves, heartbeat leaves, staged readers)
    — the test-side stand-in for the production maintenance owner, so
    workers and terminalizations have partitions to land in.
    """
    config = PostgresConfig(database_url=DB_URL)
    brk = PostgresBroker(config)
    initialized = await brk.ensure_schema_initialized()
    assert initialized.is_ok(), initialized
    from tests.integration.history_seeding import ensure_history_seedable

    async with current_resources(brk).engine.begin() as connection:
        await ensure_history_seedable(connection)
    yield brk
    await brk.close_async()


@pytest.fixture
def app_config() -> AppConfig:
    """App configuration for tests."""
    return AppConfig(
        queue_mode=QueueMode.DEFAULT,
        database=PostgresConfig(database_url=DB_URL),
    )


@pytest.fixture
def app(app_config: AppConfig, broker: PostgresBroker) -> Horsies:
    """Horsies app configured with test broker."""
    horsies_app = Horsies(app_config)
    horsies_app._broker = broker
    broker.app = horsies_app
    return horsies_app


@pytest.fixture(scope='session', autouse=True)
def child_core_handle() -> Generator[None, None, None]:
    """The core handle of a worker's child for the suite database, for the
    tests that run the writes of a child in this process
    (``install_child_core_handle``). A test that needs another database puts
    its own in place for a block (``child_core_handle_for``)."""
    install_child_core_handle(DB_URL)
    yield
    drop_child_core_handle()


@pytest_asyncio.fixture
async def clean_workflow_tables(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - ensures schema migrations are applied
) -> AsyncGenerator[None, None]:
    """Truncate workflow tables before each test."""
    await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
    await session.commit()
    yield


# =============================================================================
# Sample Task Functions (registered per-test as needed)
# =============================================================================


def make_simple_task(app: Horsies, name: str = 'simple_task') -> Any:
    """Create a simple task that doubles its input."""

    @app.task(name=name)
    def simple_task(*, value: int) -> Result[int, TaskError]:
        return Ok(value * 2)

    return simple_task


def make_simple_ctx_task(app: Horsies, name: str = 'simple_ctx_task') -> Any:
    """Create a simple task that accepts workflow_context and doubles its input."""

    @app.task(name=name)
    def simple_ctx_task(
        *, value: int,
        workflow_context: WorkflowContext | None = None,
    ) -> Result[int, TaskError]:
        return Ok(value * 2)

    return simple_ctx_task


def make_failing_task(app: Horsies, name: str = 'failing_task') -> Any:
    """Create a task that always fails."""

    @app.task(name=name)
    def failing_task() -> Result[int, TaskError]:
        return Err(TaskError(code='DELIBERATE_FAIL', message='Test failure'))

    return failing_task


def make_identity_task(app: Horsies, name: str = 'identity_task') -> Any:
    """Create a task that returns its input unchanged."""

    @app.task(name=name)
    def identity_task(*, value: JsonValue = None) -> Result[JsonValue, TaskError]:
        return Ok(value)

    return identity_task


def make_args_receiver_task(app: Horsies, name: str = 'args_receiver') -> Any:
    """Create a task that receives args_from injection."""

    @app.task(name=name)
    def args_receiver(
        *, input_result: TaskOutcome[int, TaskError],
    ) -> Result[int, TaskError]:
        match input_result:
            case Succeeded(value=value):
                return Ok(value + 10)
            case Failed(error=error):
                return Err(
                    TaskError(
                        code='UPSTREAM_FAILED',
                        message='Upstream task failed',
                        data={'upstream_error': error},
                    )
                )

    return args_receiver


def make_recovery_task(app: Horsies, name: str = 'recovery_task') -> Any:
    """Create a task that can recover from upstream failures."""

    @app.task(name=name)
    def recovery_task(
        *, input_result: TaskOutcome[int, TaskError],
    ) -> Result[int, TaskError]:
        match input_result:
            case Succeeded(value=value):
                return Ok(value)
            case Failed():
                # Recover by returning a default value
                return Ok(999)

    return recovery_task


def make_ctx_receiver_task(app: Horsies, name: str = 'ctx_receiver') -> Any:
    """Create a task that receives workflow_context."""

    @app.task(name=name)
    def ctx_receiver(
        *, workflow_context: WorkflowContext | None = None,
    ) -> Result[dict[str, JsonValue], TaskError]:
        if workflow_context is None:
            return Ok({'ctx': None})
        return Ok(
            {
                'workflow_id': workflow_context.workflow_id,
                'task_index': workflow_context.node_index,
                'task_name': workflow_context.task_name,
                'result_keys': list(workflow_context._results_by_id.keys()),
            }
        )

    return ctx_receiver


def make_no_ctx_task(app: Horsies, name: str = 'no_ctx_task') -> Any:
    """Create a task that does NOT declare workflow_context param."""

    @app.task(name=name)
    def no_ctx_task(*, value: int = 0) -> Result[int, TaskError]:
        return Ok(value + 1)

    return no_ctx_task


def make_multi_args_receiver_task(
    app: Horsies, name: str = 'multi_args_receiver'
) -> Any:
    """Create a task that receives multiple args_from injections."""

    @app.task(name=name)
    def multi_args_receiver(
        *, first: TaskOutcome[int, TaskError],
        second: TaskOutcome[int, TaskError],
    ) -> Result[int, TaskError]:
        match (first, second):
            case (Succeeded(value=first_value), Succeeded(value=second_value)):
                return Ok(first_value + second_value)
            case _:
                return Err(
                    TaskError(
                        code='UPSTREAM_FAILED', message='One or more upstreams failed'
                    )
                )

    return multi_args_receiver


def make_retryable_task(
    app: Horsies,
    name: str = 'retryable_task',
    max_retries: int = 3,
) -> Any:
    """Create a task with a retry policy configured."""
    from horsies.core.models.tasks import OperationalCode, RetryPolicy

    # Create fixed intervals list matching max_retries count
    intervals = [60] * max_retries

    @app.task(
        name=name,
        retry_policy=RetryPolicy.fixed(intervals, retry_for=[OperationalCode.TASK_EXCEPTION]),
    )
    def retryable_task(*, value: int) -> Result[int, TaskError]:
        return Ok(value * 2)

    return retryable_task


# =============================================================================
# Workflow Spec Helper
# =============================================================================


async def start_ok(
    spec: WorkflowSpec[Any],
    broker: PostgresBroker,
    workflow_id: str | None = None,
) -> WorkflowHandle[Any]:
    """Unwrap start result or fail test with error details."""
    r = await start_workflow_async(spec, broker, workflow_id)
    if is_err(r):
        pytest.fail(f'start_workflow_async failed: {r.err_value}')
    return r.ok_value


FINISH_WORKER_ID = 'worker-integration-finish'
"""The worker that ``finish_task`` claims a node's task for."""

_RUN_NODE_TASK_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'RUNNING', claimed_at = now(), claimed_by_worker_id = :worker,
        claim_expires_at = NULL, started_at = now(), worker_pid = 4242,
        worker_hostname = 'integration-host', worker_process_name = 'integration-worker',
        updated_at = now(), attempt_claims = 1, first_claimed_at = now()
    WHERE id = CAST(:task AS uuid) AND status IN ('PENDING', 'CLAIMED')
    RETURNING claimed_at
""")


def _error_code_text(error: TaskError) -> str:
    code = error.code
    match code:
        case Enum():
            return str(code.value)
        case str():
            return code


NodeFacts = Literal[
    'NO_DEPENDENTS', 'DEPENDENTS_WITHOUT_SUBWORKFLOW', 'SUBWORKFLOW_REACHABLE', 'UNKNOWN',
]
"""The node facts that phase 2 takes (``core.NodeFinishRequest.facts``): the
step that phase 1 read, or ``UNKNOWN`` for a task whose phase 1 committed
earlier."""


async def _phase_one(
    broker: PostgresBroker,
    task_id: str,
    task_name: str,
    claimed_at: datetime,
    result: Result[Any, TaskError],
) -> NodeFacts:
    """Phase 1 of the RUNNING task through the core handle of ``broker``: the
    success (``finish_workflow_success_async``), or the failure with no retry
    (``finish_failed_attempt_async``). Returns the node facts it read."""
    from horsies.core.codec.json_io import dumps_json
    from horsies.core.codec.typed import encode_task_result
    from horsies.core.models.workflow.typing_utils import resolve_source_ok_type

    handle = broker._core_handle  # pyright: ignore[reportPrivateUsage]
    ok_type = resolve_source_ok_type(broker.app, task_name)
    encoded = dumps_json(encode_task_result(result, Any if ok_type is None else ok_type))
    if is_err(encoded):
        pytest.fail(f'the result of task {task_id} does not encode: {encoded.err_value}')
    match result:
        case Ok():
            succeeded = await handle.finish_workflow_success_async(
                uuid.UUID(task_id),
                worker_id=FINISH_WORKER_ID,
                claimed_at=claimed_at,
                result_json=encoded.ok_value,
            )
            match succeeded:
                case core.WorkflowSuccessApplied(phase_two=owed):
                    return owed.step
                case _:
                    pytest.fail(f'phase 1 of the success of task {task_id}: {succeeded!r}')
        case Err(err_value=error):
            failed = await handle.finish_failed_attempt_async(
                core.FinishFailedAttemptRequest(
                    task_id=uuid.UUID(task_id),
                    worker_id=FINISH_WORKER_ID,
                    claimed_at=claimed_at,
                    retry=core.NoRetry(),
                    result_json=encoded.ok_value,
                    error_code=_error_code_text(error),
                    failed_reason=None,
                    attempt_outcome='FAILED',
                    attempt_error_message=error.message,
                    attempt_failed_reason=None,
                ),
            )
            match failed:
                case core.AttemptFailureApplied(after=core.PhaseTwoOwedRecord() as owed):
                    return owed.step
                case _:
                    pytest.fail(f'phase 1 of the failure of task {task_id}: {failed!r}')


@dataclass(frozen=True, slots=True)
class PhaseOneDone:
    """Phase 1 of a node's task committed (or had committed earlier): the
    task and the node facts that phase 2 takes."""

    task_id: str
    facts: NodeFacts


async def finish_phase_one(
    session: AsyncSession,
    broker: PostgresBroker,
    workflow_id: str,
    task_index: int,
    result: Result[Any, TaskError],
) -> PhaseOneDone:
    """Phase 1 of the task of node ``task_index``, as a worker writes it.

    Commits ``session`` first. A PENDING or CLAIMED task is claimed and run
    by ``FINISH_WORKER_ID``; then phase 1 writes the success, or the failure
    of an error result with no retry (``horsies_finish_workflow_success``,
    ``horsies_finish_failed_attempt``), and returns the node facts it read.
    A task that is no longer live (its phase 1 committed earlier) returns
    ``UNKNOWN``: phase 2 replays in the transaction form. Fails the test
    for a node with no task and for any other phase-1 outcome.
    """
    await session.commit()
    node = (
        await session.execute(
            text("""
                SELECT task_id, task_name FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
    ).fetchone()
    if node is None or node.task_id is None:
        pytest.fail(f'node {task_index} of workflow {workflow_id} has no task')
    task_id = str(node.task_id)
    claimed_at = (
        await session.execute(
            _RUN_NODE_TASK_SQL, {'task': task_id, 'worker': FINISH_WORKER_ID},
        )
    ).scalar_one_or_none()
    await session.commit()
    match claimed_at:
        case datetime():
            return PhaseOneDone(
                task_id=task_id,
                facts=await _phase_one(
                    broker, task_id, str(node.task_name), claimed_at, result,
                ),
            )
        case _:
            return PhaseOneDone(task_id=task_id, facts='UNKNOWN')


async def finish_phase_two(broker: PostgresBroker, done: PhaseOneDone) -> None:
    """Phase 2 as the worker runs it, through the core handle of ``broker``:
    the node finish in the form of the node facts, with the child starts it
    owes and the capacity wake (``finish_workflow_node_async``), then the
    parent advances after the commit (``advance_workflow_parents_async``).
    Fails the test for a failed node finish or parent advance."""
    handle = broker._core_handle  # pyright: ignore[reportPrivateUsage]
    finished = await handle.finish_workflow_node_async(
        core.NodeFinishRequest(task_id=uuid.UUID(done.task_id), facts=done.facts, wake='SEND'),
    )
    match finished:
        case core.NodeFinished(parent_advances=parent_advances):
            pass
        case core.NodeFinishFailed(message=message):
            pytest.fail(f'phase 2 of task {done.task_id}: {message}')
    advanced = await handle.advance_workflow_parents_async(parent_advances)
    match advanced:
        case core.ParentsAdvanced():
            pass
        case core.ParentAdvanceStopped(message=message):
            pytest.fail(f'parent advance after task {done.task_id}: {message}')


async def finish_task(
    session: AsyncSession,
    broker: PostgresBroker,
    workflow_id: str,
    task_index: int,
    result: Result[Any, TaskError],
) -> None:
    """End the task of node ``task_index`` as a worker does: phase 1
    (``finish_phase_one``), then phase 2 (``finish_phase_two``)."""
    await finish_phase_two(
        broker, await finish_phase_one(session, broker, workflow_id, task_index, result),
    )


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
) -> WorkflowSpec[Any]:
    """
    Create a WorkflowSpec with resolved queue/priority for testing.

    This is a test helper that mimics what app.workflow() does, but without
    needing a full Horsies app for every test.
    """
    # Resolve queue and priority for each task (mimics app.workflow() behavior)
    for task in tasks:
        if task.queue is None:
            task.queue = getattr(task.function, 'queue', None) or 'default'
        if task.priority is None:
            task.priority = 100  # DEFAULT mode priority

    spec = WorkflowSpec(
        name=name,
        nodes=tasks,
        on_error=on_error,
        output=output,
        success_policy=success_policy,
        definition_key=f'tests.integration.{name}.v1',
        broker=broker,
    )
    return spec


# =============================================================================
# Send identity helper
# =============================================================================


def compute_test_command_fingerprint(
    task_name: str,
    queue_name: str = 'default',
    priority: int = 100,
    input_json: str = '{}',
    sent_at: datetime | None = None,
    good_until: datetime | None = None,
    task_options: str | None = None,
    retention_class_key: str = 'standard_30d',
    retain_rerun_input: bool = False,
) -> tuple[datetime, bytes]:
    """Compute (sent_at, command_fingerprint) for test task insertion.

    The fingerprint is the version-1 command fingerprint that a send of
    these values stores, with the stored ``input`` text in its kwargs
    element. The returned sent_at is the one to store in the row.
    """
    from tests.command_fingerprint import EnqueueCommandV1

    if sent_at is None:
        sent_at = datetime.now(timezone.utc)
    fingerprint = EnqueueCommandV1(
        task_name=task_name,
        queue_name=queue_name,
        priority=priority,
        positional_args_json=None,
        input_json=input_json,
        good_until=good_until,
        enqueue_delay_seconds=None,
        task_options_json=task_options,
        retention_class_key=retention_class_key,
        retain_rerun_input=retain_rerun_input,
        rerun_of_task_id=None,
        rerun_root_task_id=None,
    ).fingerprint
    return sent_at, fingerprint
