# pyright: reportPrivateUsage=false
"""The task advisory lock goes before the row lock, on every path.

Each move function takes the task advisory lock, then the row lock. A
transaction that takes the row lock first and calls a move function later
can deadlock against one that took the advisory lock first: Postgres then
aborts one of them (SQLSTATE 40P01). These tests hold one side between its
two locks and start the other side, so a wrong order deadlocks every time:

- a stale recovery that holds its locks, against a failure finalize;
- a stale recovery that holds its locks, against the success finalize of a
  workflow task;
- the success of a workflow task (``horsies_finish_workflow_success``) that
  holds its locks in an open transaction, against the stale recovery's move
  (``horsies_fail_stale_task``; the recovery's scan skips a locked row, so
  the move is called directly);
- a monitoring cancel that holds its locks, against a failure finalize.

A free race of a finalize and a stale recovery of the same task then runs
many times, and no deadlock may occur.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any, Final
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.types.result import Ok, is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker
from horsies.core.worker.workflow_success import (
    FinishWorkflowSuccess,
    success_statement,
)
from horsies.monitoring.task_actions import cancel_task
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]

WORKER = 'w-lock-order'
TASK_NAME = 'lock_order_task'
STALE_AFTER_MS: Final = 1_000
WAIT_POLL_S: Final = 0.02
WAIT_POLLS: Final = 250
"""Bounds the wait for a backend to block on a lock: 5 s."""
FREE_RACE_ROUNDS: Final = 50

_APP_CACHE: dict[str, Horsies] = {}


def _app() -> Horsies:
    """An app that registers the task this file runs, built once."""
    cached = _APP_CACHE.get(TASK_NAME)
    if cached is not None:
        return cached
    app = Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql+psycopg://u:p@localhost/db'),
            ),
        ),
    )

    @app.task(task_name=TASK_NAME)
    def lock_order_task() -> TaskResult[JsonValue, TaskError]:
        return TaskResult(ok=None)

    _ = lock_order_task
    _APP_CACHE[TASK_NAME] = app
    return app


def _make_worker(engine: AsyncEngine) -> Worker:
    worker = Worker(
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
        ),
    )
    worker.worker_instance_id = WORKER
    worker._app = _app()
    return worker


@pytest_asyncio.fixture
async def stale_task(
    engine: AsyncEngine,
    clean_workflow_tables: None,  # noqa: ARG001
) -> AsyncIterator[Callable[..., Awaitable[tuple[str, datetime]]]]:
    """A factory of RUNNING rows of ``WORKER`` that stale recovery may fail."""

    async def make(*, is_workflow_task: bool = False) -> tuple[str, datetime]:
        task_id = str(uuid.uuid4())
        claimed_at = datetime.now(timezone.utc) - timedelta(hours=1)
        sent_at, sha = compute_test_enqueue_sha(task_name=TASK_NAME)
        async with AsyncSession(engine) as session:
            await session.execute(
                text("""
                    INSERT INTO horsies_tasks
                        (id, task_name, queue_name, priority, args, kwargs,
                         status, sent_at, created_at, updated_at, claimed,
                         retry_count, max_retries, started_at, enqueue_sha,
                         claimed_by_worker_id, claimed_at, is_workflow_task,
                         retention_class_key, command_fingerprint_version,
                         command_fingerprint, retain_rerun_input,
                         prepared_rerun_input_disposition)
                    VALUES
                        (:id, :task_name, 'default', 100, '[]', '{}',
                         'RUNNING', :sent_at, NOW(), NOW(), FALSE,
                         0, 0, :claimed_at, :sha,
                         :worker, :claimed_at, :is_workflow_task,
                         'standard_30d', 1,
                         sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                         FALSE, 'DECLINED_BY_POLICY')
                """),
                {
                    'id': task_id,
                    'task_name': TASK_NAME,
                    'sent_at': sent_at,
                    'sha': sha,
                    'worker': WORKER,
                    'claimed_at': claimed_at,
                    'is_workflow_task': is_workflow_task,
                },
            )
            if is_workflow_task:
                await _link_node(session, task_id)
            await session.commit()
        return task_id, claimed_at

    yield make


async def _link_node(session: AsyncSession, task_id: str) -> None:
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:wf, 'lock_order_wf', 'RUNNING', 'FAIL', 0, :wf,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'wf': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, created_at)
            VALUES (:id, :wf, 0, 'node-0', :task_name, '[]', '{}', 'default',
                    100, '{}', FALSE, 'all', FALSE, 'RUNNING',
                    CAST(:task_id AS uuid), NOW())
        """),
        {
            'id': str(uuid.uuid4()),
            'wf': workflow_id,
            'task_name': TASK_NAME,
            'task_id': task_id,
        },
    )


def _ok_result() -> str:
    return dumps_json(encode_task_result(TaskResult(ok=None), JsonValue)).unwrap()


async def _fail_by_worker(
    worker: Worker,
    task_id: str,
    claimed_at: datetime,
) -> Any:
    """The finalize of a worker-level failure: one failed-attempt write."""
    return await worker._persist_task_terminal_state(
        task_id=task_id,
        now=datetime.now(timezone.utc),
        ok=False,
        result_json_str='',
        failed_reason='worker lost the child',
        task_name=TASK_NAME,
        queue_name='default',
        is_workflow_task=False,
        claimed_at=claimed_at,
    )


async def _complete_workflow_task(
    worker: Worker,
    task_id: str,
    claimed_at: datetime,
) -> Any:
    """The finalize of a workflow task's success: one statement."""
    return await worker._persist_task_terminal_state(
        task_id=task_id,
        now=datetime.now(timezone.utc),
        ok=True,
        result_json_str=_ok_result(),
        failed_reason=None,
        task_name=TASK_NAME,
        queue_name='default',
        is_workflow_task=True,
        claimed_at=claimed_at,
    )


async def _recover_stale(broker: PostgresBroker) -> Any:
    return await broker.mark_stale_tasks_as_failed(
        stale_threshold_ms=STALE_AFTER_MS,
        finalizing_stale_threshold_ms=STALE_AFTER_MS,
    )


class _Gate:
    """Pauses one call of a wrapped coroutine function until released."""

    def __init__(self, target: Callable[..., Awaitable[Any]]) -> None:
        self._target = target
        self.holding = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.holding.set()
        await self.release.wait()
        return await self._target(*args, **kwargs)


async def _until_a_backend_waits_on_a_lock(
    engine: AsyncEngine, query_part: str
) -> None:
    """Return once a backend whose query contains ``query_part`` waits on a lock."""
    for _ in range(WAIT_POLLS):
        async with engine.connect() as connection:
            waiting = (
                await connection.execute(
                    text("""
                        SELECT count(*) FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND wait_event_type = 'Lock'
                          AND query LIKE '%' || :part || '%'
                          AND pid <> pg_backend_pid()
                    """),
                    {'part': query_part},
                )
            ).scalar_one()
        if waiting > 0:
            return
        await asyncio.sleep(WAIT_POLL_S)
    raise AssertionError(f'no backend waited on a lock running {query_part!r}')


async def _history(engine: AsyncEngine, task_id: str) -> list[tuple[str, str]]:
    async with engine.connect() as connection:
        rows = (
            await connection.execute(
                text(
                    'SELECT status, terminalization_kind FROM horsies_task_history '
                    'WHERE task_id = CAST(:id AS uuid)'
                ),
                {'id': task_id},
            )
        ).all()
    return [(str(row[0]), str(row[1])) for row in rows]


def _deadlocks(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if 'deadlock' in record.getMessage().lower()
    ]


@pytest.mark.asyncio(loop_scope='function')
async def test_a_failure_finalize_waits_for_a_stale_recovery_that_holds_its_locks(
    engine: AsyncEngine,
    broker: PostgresBroker,
    stale_task: Callable[..., Awaitable[tuple[str, datetime]]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Stale recovery holds the task's locks before its move; the failure
    finalize starts and waits; the recovery then moves the row."""
    task_id, claimed_at = await stale_task()
    worker = _make_worker(engine)
    import horsies.core.brokers.postgres as broker_module

    gate = _Gate(broker_module.apply_async)
    monkeypatch.setattr(broker_module, 'apply_async', gate)

    with caplog.at_level(logging.ERROR):
        recovery = asyncio.ensure_future(_recover_stale(broker))
        await asyncio.wait_for(gate.holding.wait(), WAIT_POLL_S * WAIT_POLLS)
        finalize = asyncio.ensure_future(_fail_by_worker(worker, task_id, claimed_at))
        await _until_a_backend_waits_on_a_lock(engine, 'horsies_finish_failed_attempt')
        gate.release.set()
        recovered, finalized = await asyncio.gather(recovery, finalize)

    assert _deadlocks(caplog) == []
    assert recovered == Ok(1)
    assert is_ok(finalized) and finalized.ok_value is None
    assert await _history(engine, task_id) == [('FAILED', 'FAIL_STALE')]


@pytest.mark.asyncio(loop_scope='function')
async def test_a_workflow_success_waits_for_a_stale_recovery_that_holds_its_locks(
    engine: AsyncEngine,
    broker: PostgresBroker,
    stale_task: Callable[..., Awaitable[tuple[str, datetime]]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Stale recovery holds the locks of a workflow task before its move;
    the success finalize starts and waits on the advisory lock; the recovery
    then moves the row, and the success finds it moved by another kind."""
    task_id, claimed_at = await stale_task(is_workflow_task=True)
    worker = _make_worker(engine)
    import horsies.core.brokers.postgres as broker_module

    gate = _Gate(broker_module.apply_async)
    monkeypatch.setattr(broker_module, 'apply_async', gate)

    with caplog.at_level(logging.ERROR):
        recovery = asyncio.ensure_future(_recover_stale(broker))
        await asyncio.wait_for(gate.holding.wait(), WAIT_POLL_S * WAIT_POLLS)
        finalize = asyncio.ensure_future(
            _complete_workflow_task(worker, task_id, claimed_at),
        )
        await _until_a_backend_waits_on_a_lock(
            engine, 'horsies_finish_workflow_success'
        )
        gate.release.set()
        recovered, finalized = await asyncio.gather(recovery, finalize)

    assert _deadlocks(caplog) == []
    assert recovered == Ok(1)
    assert is_ok(finalized) and finalized.ok_value is None
    assert await _history(engine, task_id) == [('FAILED', 'FAIL_STALE')]


async def _fail_stale_move(engine: AsyncEngine, task_id: str) -> str:
    """The stale recovery's move of ``task_id``, in its own transaction."""
    async with engine.begin() as connection:
        outcome = (
            await connection.execute(
                text(
                    'SELECT outcome FROM horsies_fail_stale_task('
                    'CAST(:id AS uuid), :after_ms, :after_ms, :result, '
                    "'WORKER_CRASHED', 'went silent')"
                ),
                {
                    'id': task_id,
                    'after_ms': STALE_AFTER_MS,
                    'result': _ok_result(),
                },
            )
        ).scalar_one()
    return str(outcome)


@pytest.mark.asyncio(loop_scope='function')
async def test_the_stale_move_waits_for_a_workflow_success_that_holds_its_locks(
    engine: AsyncEngine,
    stale_task: Callable[..., Awaitable[tuple[str, datetime]]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The success of a workflow task runs in a transaction that stays open,
    so it holds the advisory lock and the row lock; the stale move starts
    and waits; the commit then releases both, and the stale move finds the
    row moved."""
    task_id, claimed_at = await stale_task(is_workflow_task=True)
    command = FinishWorkflowSuccess(
        task_id=task_id,
        fence=OwnedClaim(worker_id=WORKER, claimed_at=claimed_at),
        result_json=_ok_result(),
    )
    statement = success_statement(command)

    with caplog.at_level(logging.ERROR):
        async with engine.connect() as holder:
            transaction = await holder.begin()
            outcome = (
                (await holder.execute(statement.statement, dict(statement.parameters)))
                .mappings()
                .one()
            )
            assert outcome['outcome'] == 'APPLIED'
            stale_move = asyncio.ensure_future(_fail_stale_move(engine, task_id))
            await _until_a_backend_waits_on_a_lock(engine, 'horsies_fail_stale_task')
            await transaction.commit()
        moved = await stale_move

    assert _deadlocks(caplog) == []
    assert moved == 'SOURCE_STATE_CONFLICT'
    assert await _history(engine, task_id) == [('COMPLETED', 'COMPLETE_LOCKED')]


@pytest.mark.asyncio(loop_scope='function')
async def test_a_failure_finalize_waits_for_a_monitoring_cancel_that_holds_its_locks(
    engine: AsyncEngine,
    broker: PostgresBroker,
    stale_task: Callable[..., Awaitable[tuple[str, datetime]]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A cancel of the RUNNING task holds the task's locks before its move;
    the failure finalize starts and waits; the cancel then moves the row."""
    task_id, claimed_at = await stale_task()
    worker = _make_worker(engine)
    import horsies.monitoring.task_actions as task_actions_module

    gate = _Gate(task_actions_module.apply_async)
    monkeypatch.setattr(task_actions_module, 'apply_async', gate)

    with caplog.at_level(logging.ERROR):
        cancel = asyncio.ensure_future(
            cancel_task(broker, task_id, include_running=True),
        )
        await asyncio.wait_for(gate.holding.wait(), WAIT_POLL_S * WAIT_POLLS)
        finalize = asyncio.ensure_future(_fail_by_worker(worker, task_id, claimed_at))
        await _until_a_backend_waits_on_a_lock(engine, 'horsies_finish_failed_attempt')
        gate.release.set()
        cancelled, finalized = await asyncio.gather(cancel, finalize)

    assert _deadlocks(caplog) == []
    assert is_ok(cancelled)
    assert is_ok(finalized) and finalized.ok_value is None
    assert await _history(engine, task_id) == [('CANCELLED', 'CANCEL_ADMIN')]


@pytest.mark.parametrize('kind', ['failure', 'workflow_completion'])
@pytest.mark.asyncio(loop_scope='function')
async def test_a_free_race_of_finalize_and_stale_recovery_never_deadlocks(
    engine: AsyncEngine,
    broker: PostgresBroker,
    stale_task: Callable[..., Awaitable[tuple[str, datetime]]],
    caplog: pytest.LogCaptureFixture,
    kind: str,
) -> None:
    """The two run at once on the same task, many times: one of them moves
    the row each round, and none of them fails."""
    worker = _make_worker(engine)
    for _ in range(FREE_RACE_ROUNDS):
        match kind:
            case 'failure':
                task_id, claimed_at = await stale_task()
                finalize = _fail_by_worker(worker, task_id, claimed_at)
            case _:
                task_id, claimed_at = await stale_task(is_workflow_task=True)
                finalize = _complete_workflow_task(worker, task_id, claimed_at)
        with caplog.at_level(logging.ERROR):
            finalized, recovered = await asyncio.gather(
                finalize,
                _recover_stale(broker),
            )
        assert is_ok(finalized), finalized
        assert recovered in (Ok(0), Ok(1)), recovered
        assert len(await _history(engine, task_id)) == 1
    assert _deadlocks(caplog) == []
