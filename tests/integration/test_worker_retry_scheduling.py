# pyright: reportPrivateUsage=false
"""The retry of a task error, from the finalize entry point.

``_persist_task_terminal_state`` asks the policy with the retry facts of the
claim, and hands the decision to ``horsies_finish_failed_attempt``,
which writes the retry or the terminal failure under the lock. Each test
enters at the finalize and asserts the row it left.
"""

from __future__ import annotations

import functools
import json
import uuid
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.app import Horsies
from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.lifecycle.outcomes import FailedAttemptOutcome
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import is_ok
from horsies.core.worker.dispatch_claim import DispatchClaim
from horsies.core.worker import failed_attempt as failed_attempt_module
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.failed_attempt import FinishFailedAttempt, RetryAfter
from horsies.core.worker.worker import Worker
from tests.dispatch_claim_support import claim_at, row_claim
from tests.integration.conftest import compute_test_command_fingerprint
from tests.integration.history_seeding import force_terminal, read_attempts
from tests.integration.autocommit_support import (
    open_proxied_worker,
    proxied_url,
    steady_state_turns,
    upstream_of,
)
from tests.integration.round_trip_proxy import RoundTripProxy

pytestmark = [pytest.mark.integration]

# Owner id shared by the insert helper and the worker under test so the
# retry-path ownership guard matches.
TEST_WORKER_ID = 'w-retry-test'
TASK_NAME = 'retry_test'
# The claim generation of every row seeded here, which the fences of the
# worker check.
CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)


@functools.cache
def _app() -> Horsies:
    """An app that registers the task name this file inserts."""
    app = Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql://u:p@localhost/db'),
            ),
        ),
    )

    @app.task(task_name=TASK_NAME)
    def retry_test() -> TaskResult[JsonValue, TaskError]:
        return TaskResult(ok=None)

    _ = retry_test
    return app


def _make_worker(engine: AsyncEngine) -> Worker:
    """A Worker wired to the test DB, whose delayed retry wake is recorded."""
    sf = async_sessionmaker(engine, expire_on_commit=False)
    cfg = WorkerConfig(
        dsn='postgresql://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    worker = Worker(session_factory=sf, listener=MagicMock(), cfg=cfg)
    worker.worker_instance_id = TEST_WORKER_ID
    worker._app = _app()
    worker._spawn_background = MagicMock()  # type: ignore[method-assign]
    return worker


async def _insert_task(
    session: AsyncSession,
    *,
    status: str = 'RUNNING',
    retry_count: int = 0,
    max_retries: int = 3,
    task_options: str | None = None,
    good_until: datetime | None = None,
    claimed_by_worker_id: str = TEST_WORKER_ID,
) -> str:
    """Seed a task with configurable retry fields.

    A terminal status is reached through terminalization's move, so the
    row is born running and then forced terminal.
    """
    task_id = str(uuid.uuid4())
    is_terminal = status in ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    sent_at, fingerprint = compute_test_command_fingerprint(
        task_name=TASK_NAME,
        good_until=good_until,
        task_options=task_options,
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    created_at, updated_at, retry_count, max_retries, started_at,
                    good_until, task_options, claimed_by_worker_id, claimed_at,
                    retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution, enqueued_at,
                    is_workflow_task, claim_expires_at, worker_pid, worker_hostname,
                    worker_process_name
                ) VALUES (
                    :id, :task_name, 'default', 100, '{}', :status, :sent_at, NOW(),
                    NOW(), :retry_count, :max_retries, NOW(), :good_until,
                    :task_options, :claimed_by_worker_id, :claimed_at, 'standard_30d',
                    1, :command_fingerprint, FALSE, 'ISOLATED', NOW(), FALSE,
                    CASE WHEN CAST(:status AS varchar) = 'CLAIMED' THEN NOW() + INTERVAL '5 minutes' END,
                    CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 4242 END,
                    CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 'host-1' END,
                    CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 'child-1' END
                )
        """),
        {
            'id': task_id,
            'task_name': TASK_NAME,
            'sent_at': sent_at,
            'command_fingerprint': fingerprint,
            'status': 'RUNNING' if is_terminal else status,
            'retry_count': retry_count,
            'max_retries': max_retries,
            'good_until': good_until,
            'task_options': task_options,
            'claimed_by_worker_id': claimed_by_worker_id,
            'claimed_at': CLAIMED_AT,
        },
    )
    if is_terminal:
        await force_terminal(session, task_id, status=status)
    await session.commit()
    return task_id


def _make_task_options(
    *,
    auto_retry_for: list[str],
    intervals: list[int] | None = None,
    backoff_strategy: str = 'fixed',
    jitter: bool = False,
) -> str:
    """Build task_options JSON with a retry policy that validates: one
    interval per retry (the row's max_retries column decides when the
    retries are spent)."""
    resolved_intervals = intervals or [60]
    return json.dumps(
        {
            'retry_policy': {
                'max_retries': len(resolved_intervals),
                'auto_retry_for': auto_retry_for,
                'intervals': resolved_intervals,
                'backoff_strategy': backoff_strategy,
                'jitter': jitter,
            },
        }
    )


def _error_result(code: str) -> str:
    envelope = encode_task_result(
        TaskResult(err=TaskError(error_code=code, message=f'{code} happened')),
        JsonValue,
    )
    return dumps_json(envelope).unwrap()


async def _finalize_error(
    worker: Worker, task_id: str, claim: DispatchClaim, code: str,
) -> Any:
    return await worker._persist_task_terminal_state(
        task_id=task_id,
        now=datetime.now(timezone.utc),
        ok=True,
        result_json_str=_error_result(code),
        failed_reason=None,
        task_name=TASK_NAME,
        queue_name='default',
        is_workflow_task=False,
        claim=claim,
    )


async def _row(session: AsyncSession, task_id: str) -> Any:
    """The task as itest_task_rows shows it, live or in history."""
    row = (
        await session.execute(
            text(
                'SELECT status, retry_count, next_retry_at, enqueued_at, '
                'claimed_by_worker_id, error_code '
                'FROM itest_task_rows WHERE id = CAST(:id AS uuid)',
            ),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None, f'Task {task_id} not found'
    return row


@pytest.mark.parametrize(
    ('retry_count', 'max_retries', 'auto_retry_for'),
    [(0, 0, ['ERR']), (3, 3, ['ERR']), (0, 3, None), (0, 3, ['OTHER'])],
    ids=['max_retries_zero', 'retries_spent', 'no_policy', 'code_not_listed'],
)
@pytest.mark.asyncio(loop_scope='function')
async def test_a_declined_retry_fails_the_task(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    retry_count: int,
    max_retries: int,
    auto_retry_for: list[str] | None,
) -> None:
    task_id = await _insert_task(
        session,
        retry_count=retry_count,
        max_retries=max_retries,
        task_options=(
            None
            if auto_retry_for is None
            else _make_task_options(auto_retry_for=auto_retry_for)
        ),
    )
    worker = _make_worker(engine)

    result = await _finalize_error(worker, task_id, await row_claim(session, task_id), 'ERR')

    assert is_ok(result) and result.ok_value is None
    row = await _row(session, task_id)
    assert (row.status, row.error_code) == ('FAILED', 'ERR')
    worker._spawn_background.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio(loop_scope='function')
async def test_a_listed_code_schedules_the_retry(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    opts = _make_task_options(auto_retry_for=['ERR'], intervals=[60])
    task_id = await _insert_task(session, retry_count=1, task_options=opts)
    worker = _make_worker(engine)

    result = await _finalize_error(worker, task_id, await row_claim(session, task_id), 'ERR')

    assert is_ok(result) and result.ok_value is None
    row = await _row(session, task_id)
    assert (row.status, row.retry_count, row.claimed_by_worker_id) == (
        'PENDING',
        2,
        None,
    )
    assert row.next_retry_at is not None
    assert row.enqueued_at == row.next_retry_at
    attempts = await read_attempts(session, task_id)
    assert [(a.attempt, a.will_retry, a.error_code) for a in attempts] == [
        (2, True, 'ERR'),
    ]
    worker._spawn_background.assert_called_once()  # type: ignore[attr-defined]
    assert worker._spawn_background.call_args.kwargs['name'] == (  # type: ignore[attr-defined]
        f'delayed-notify-{task_id}'
    )


@pytest.fixture
def proxy(db_url: str) -> Generator[RoundTripProxy]:
    host, port = upstream_of(db_url)
    started = RoundTripProxy(host, port)
    started.start()
    yield started
    started.stop()


@pytest.mark.asyncio(loop_scope='function')
async def test_the_retry_is_one_statement_with_no_read(
    proxy: RoundTripProxy,
    db_url: str,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """One write and no read of the row: the retry facts come with the claim.
    Counted at the wire, in the steady state of the core's prepared
    statements: the finalize sends one turn, the Bind/Execute of the write."""
    opts = _make_task_options(auto_retry_for=['ERR'])
    opened = await open_proxied_worker(proxied_url(db_url, proxy))
    opened.worker.worker_instance_id = TEST_WORKER_ID
    opened.worker._app = _app()
    opened.worker._spawn_background = MagicMock()  # type: ignore[method-assign]

    async def arrange() -> tuple[str, DispatchClaim]:
        task_id = await _insert_task(session, task_options=opts)
        return task_id, await row_claim(session, task_id)

    async def act(case: tuple[str, DispatchClaim]) -> None:
        task_id, claim = case
        result = await _finalize_error(opened.worker, task_id, claim, 'ERR')
        assert is_ok(result), result

    try:
        (task_id, _), turns = await steady_state_turns(proxy, arrange, act)
    finally:
        await opened.close()

    assert [turn.executes() for turn in turns] == [True], turns
    row = await _row(session, task_id)
    assert (row.status, row.retry_count) == ('PENDING', 1)


@pytest.mark.asyncio(loop_scope='function')
async def test_a_retry_after_good_until_fails_the_task(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    opts = _make_task_options(auto_retry_for=['ERR'], intervals=[3600])
    good_until = datetime.now(tz=timezone.utc) + timedelta(seconds=30)
    task_id = await _insert_task(session, task_options=opts, good_until=good_until)
    worker = _make_worker(engine)

    result = await _finalize_error(worker, task_id, await row_claim(session, task_id), 'ERR')

    assert is_ok(result) and result.ok_value is None
    assert (await _row(session, task_id)).status == 'FAILED'
    worker._spawn_background.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio(loop_scope='function')
async def test_good_until_is_judged_under_the_lock(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """good_until moves before the past between the read and the write: the
    write judges the value it locked and fails the task."""
    opts = _make_task_options(auto_retry_for=['ERR'], intervals=[1])
    good_until = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    task_id = await _insert_task(session, task_options=opts, good_until=good_until)
    worker = _make_worker(engine)
    finish = failed_attempt_module.finish_failed_attempt_in_core

    async def _shrink_then_finish(
        handle: Any,
        command: FinishFailedAttempt,
    ) -> FailedAttemptOutcome:
        assert isinstance(command.retry, RetryAfter)
        async with engine.begin() as other:
            await other.execute(
                text(
                    'UPDATE horsies_tasks SET good_until = NOW() - '
                    "INTERVAL '5 minutes' WHERE id = CAST(:id AS uuid)"
                ),
                {'id': task_id},
            )
        return await finish(handle, command)

    with patch(
        'horsies.core.worker.finalize.finish_failed_attempt_in_core',
        _shrink_then_finish,
    ):
        result = await _finalize_error(worker, task_id, await row_claim(session, task_id), 'ERR')

    assert is_ok(result) and result.ok_value is None
    assert (await _row(session, task_id)).status == 'FAILED'


@pytest.mark.parametrize(
    'state',
    ['FAILED', 'another_worker'],
)
@pytest.mark.asyncio(loop_scope='function')
async def test_a_row_this_claim_does_not_hold_is_left_alone(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    state: str,
) -> None:
    opts = _make_task_options(auto_retry_for=['ERR'], intervals=[1])
    match state:
        case 'FAILED':
            task_id = await _insert_task(session, status='FAILED', task_options=opts)
        case _:
            task_id = await _insert_task(
                session,
                task_options=opts,
                claimed_by_worker_id='w-someone-else',
            )
    before = await _row(session, task_id)
    worker = _make_worker(engine)

    result = await _finalize_error(worker, task_id, claim_at(CLAIMED_AT), 'ERR')

    assert is_ok(result) and result.ok_value is None
    assert await _row(session, task_id) == before
    worker._spawn_background.assert_not_called()  # type: ignore[attr-defined]
