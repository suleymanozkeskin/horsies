# pyright: reportPrivateUsage=false
"""Integration tests for per-task timeout enforcement.

Covers Worker._handle_task_timeout: the parent-side handler invoked when a
dispatched task outruns TaskOptions.timeout_ms. The SIGKILL targets the
row's recorded worker_pid; tests use a nonexistent pid so the suppressed
ProcessLookupError path runs instead of killing anything.
"""

from __future__ import annotations

import json
import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.codec.task_options import serialize_task_options
from horsies.core.models.tasks import OutcomeCode, TaskOptions
from horsies.core.types.result import is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker, _parse_timeout_ms
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]

# Owner id shared by the insert helper and the worker under test so the
# ownership guards match.
TEST_WORKER_ID = 'w-timeout-test'
OTHER_WORKER_ID = 'w-timeout-other'

# A pid that cannot exist (pid_max is far below this on macOS and Linux),
# so os.kill raises ProcessLookupError, which the handler suppresses.
FAKE_DEAD_PID = 2**22 - 1


def _make_worker(engine: AsyncEngine) -> Worker:
    sf = async_sessionmaker(engine, expire_on_commit=False)
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    worker = Worker(session_factory=sf, listener=MagicMock(), cfg=cfg)
    worker.worker_instance_id = TEST_WORKER_ID
    return worker


async def _insert_task(
    session: AsyncSession,
    *,
    status: str = 'RUNNING',
    owner: str = TEST_WORKER_ID,
    max_retries: int = 0,
    retry_count: int = 0,
    task_options: str | None = None,
) -> str:
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(
        task_name='timeout_test', task_options=task_options,
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs, status,
                 sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha, claimed_by_worker_id,
                 worker_pid, task_options)
            VALUES
                (:id, 'timeout_test', 'default', 100, '[]', '{}', :status,
                 :sent_at, NOW(), NOW(), FALSE, :retry_count,
                 :max_retries, NOW(), :sha, :owner,
                 :worker_pid, :task_options)
        """),
        {
            'id': task_id,
            'status': status,
            'sent_at': sent_at,
            'sha': sha,
            'owner': owner,
            'retry_count': retry_count,
            'max_retries': max_retries,
            'worker_pid': FAKE_DEAD_PID,
            'task_options': task_options,
        },
    )
    await session.commit()
    return task_id


async def _get_row(session: AsyncSession, task_id: str) -> Row[Any]:
    row = (
        await session.execute(
            text("""
                SELECT status, error_code, retry_count, claimed_by_worker_id
                FROM horsies_tasks WHERE id = :id
            """),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    return row


@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_marks_running_task_failed(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """RUNNING + ours → FAILED with TASK_TIMEOUT and an attempt row."""
    task_id = await _insert_task(session)
    worker = _make_worker(engine)

    await worker._handle_task_timeout(task_id, 5_000)
    session.expire_all()

    row = await _get_row(session, task_id)
    assert row.status == 'FAILED'
    assert row.error_code == OutcomeCode.TASK_TIMEOUT.value

    attempt = (
        await session.execute(
            text("""
                SELECT outcome, will_retry, error_code
                FROM horsies_task_attempts WHERE task_id = :id
            """),
            {'id': task_id},
        )
    ).fetchone()
    assert attempt is not None
    assert attempt.outcome == 'FAILED'
    assert attempt.will_retry is False
    assert attempt.error_code == OutcomeCode.TASK_TIMEOUT.value


@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_respects_ownership(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A row re-claimed by another worker is left untouched."""
    task_id = await _insert_task(session, owner=OTHER_WORKER_ID)
    worker = _make_worker(engine)

    await worker._handle_task_timeout(task_id, 5_000)
    session.expire_all()

    row = await _get_row(session, task_id)
    assert row.status == 'RUNNING'
    assert row.claimed_by_worker_id == OTHER_WORKER_ID


@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_requeues_claimed_task(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """CLAIMED (user code never started) is requeued, not failed."""
    task_id = await _insert_task(session, status='CLAIMED')
    worker = _make_worker(engine)

    await worker._handle_task_timeout(task_id, 5_000)
    session.expire_all()

    row = await _get_row(session, task_id)
    assert row.status == 'PENDING'
    assert row.claimed_by_worker_id is None


@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_retries_when_opted_in(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """TASK_TIMEOUT in auto_retry_for → retry scheduled, not terminal."""
    options = json.dumps({
        'retry_policy': {
            'max_retries': 3,
            'intervals': [1],
            'backoff_strategy': 'fixed',
            'jitter': False,
            'auto_retry_for': ['TASK_TIMEOUT'],
        },
        'timeout_ms': 5_000,
    })
    task_id = await _insert_task(
        session, max_retries=3, task_options=options,
    )
    worker = _make_worker(engine)

    await worker._handle_task_timeout(task_id, 5_000)
    session.expire_all()

    row = await _get_row(session, task_id)
    assert row.status == 'PENDING'
    assert row.retry_count == 1

    attempt = (
        await session.execute(
            text("""
                SELECT will_retry, error_code
                FROM horsies_task_attempts WHERE task_id = :id
            """),
            {'id': task_id},
        )
    ).fetchone()
    assert attempt is not None
    assert attempt.will_retry is True
    assert attempt.error_code == OutcomeCode.TASK_TIMEOUT.value


class TestTimeoutWireRoundTrip:
    """serialize_task_options must carry timeout_ms to the claim-side parser.

    Regression: the serializer originally emitted only retry_policy and
    good_until, silently dropping timeout_ms before it reached the DB —
    decorator-declared timeouts never fired.
    """

    def test_timeout_round_trips(self) -> None:
        serialized = serialize_task_options(
            TaskOptions(task_name='t', timeout_ms=5_000),
        )
        assert is_ok(serialized)
        assert _parse_timeout_ms(serialized.ok_value, 't') == 5_000

    def test_absent_timeout_round_trips_as_none(self) -> None:
        serialized = serialize_task_options(TaskOptions(task_name='t'))
        assert is_ok(serialized)
        assert _parse_timeout_ms(serialized.ok_value, 't') is None


class TestParseTimeoutMs:
    def test_valid(self) -> None:
        assert _parse_timeout_ms(json.dumps({'timeout_ms': 5000}), 't') == 5000

    def test_absent(self) -> None:
        assert _parse_timeout_ms(json.dumps({'retry_policy': {}}), 't') is None

    def test_none_input(self) -> None:
        assert _parse_timeout_ms(None, 't') is None

    def test_corrupt_json(self) -> None:
        assert _parse_timeout_ms('{not json', 't') is None

    def test_non_dict(self) -> None:
        assert _parse_timeout_ms('[1,2]', 't') is None

    def test_bool_rejected(self) -> None:
        assert _parse_timeout_ms(json.dumps({'timeout_ms': True}), 't') is None

    def test_negative_rejected(self) -> None:
        assert _parse_timeout_ms(json.dumps({'timeout_ms': -5}), 't') is None
