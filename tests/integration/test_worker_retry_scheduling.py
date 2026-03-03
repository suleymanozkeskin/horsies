"""Integration tests for Worker._should_retry_task() and Worker._schedule_retry().

Covers the retry-scheduling branch in the finalization pipeline:
_should_retry_task() decides eligibility, _schedule_retry() persists the
retry state and handles race conditions with the reaper / expiry guard.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.models.tasks import TaskError
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_worker(engine: AsyncEngine) -> Worker:
    """Construct a Worker wired to the test DB."""
    sf = async_sessionmaker(engine, expire_on_commit=False)
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    return Worker(session_factory=sf, listener=MagicMock(), cfg=cfg)


async def _insert_task(
    session: AsyncSession,
    *,
    status: str = 'RUNNING',
    retry_count: int = 0,
    max_retries: int = 3,
    task_options: str | None = None,
    good_until: datetime | None = None,
) -> str:
    """Insert a horsies_tasks row with configurable retry fields."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(
        task_name='retry_test',
        good_until=good_until,
        task_options=task_options,
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs, status, sent_at,
                 created_at, updated_at, claimed, retry_count, max_retries, started_at,
                 good_until, task_options, enqueue_sha)
            VALUES
                (:id, 'retry_test', 'default', 100, '[]', '{}', :status, :sent_at,
                 NOW(), NOW(), FALSE, :retry_count, :max_retries, NOW(),
                 :good_until, :task_options, :enqueue_sha)
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'enqueue_sha': sha,
            'status': status,
            'retry_count': retry_count,
            'max_retries': max_retries,
            'good_until': good_until,
            'task_options': task_options,
        },
    )
    await session.commit()
    return task_id


def _make_task_options(
    *,
    auto_retry_for: list[str],
    intervals: list[int] | None = None,
    backoff_strategy: str = 'fixed',
    jitter: bool = False,
) -> str:
    """Build task_options JSON with retry policy."""
    return json.dumps({
        'retry_policy': {
            'auto_retry_for': auto_retry_for,
            'intervals': intervals or [60],
            'backoff_strategy': backoff_strategy,
            'jitter': jitter,
        },
    })


async def _get_task_row(
    session: AsyncSession,
    task_id: str,
) -> Any:
    """Read task row for post-condition assertions."""
    row = (
        await session.execute(
            text(
                'SELECT status, retry_count, next_retry_at '
                'FROM horsies_tasks WHERE id = :id',
            ),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None, f'Task {task_id} not found'
    return row


# ---------------------------------------------------------------------------
# _should_retry_task tests
# ---------------------------------------------------------------------------


# 6a -----------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_should_retry_no_task_row_returns_false(
    engine: AsyncEngine,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Nonexistent task_id → False."""
    worker = _make_worker(engine)
    error = TaskError(error_code='TEST_ERR', message='test')

    async with worker.sf() as s:
        result = await worker._should_retry_task(str(uuid.uuid4()), error, s)

    assert result is False


# 6b -----------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_should_retry_max_retries_zero_returns_false(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """max_retries=0 → False (no retries configured)."""
    task_id = await _insert_task(session, max_retries=0)
    worker = _make_worker(engine)
    error = TaskError(error_code='TEST_ERR', message='test')

    async with worker.sf() as s:
        result = await worker._should_retry_task(task_id, error, s)

    assert result is False


# 6c -----------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_should_retry_count_exhausted_returns_false(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """retry_count already at max → False."""
    task_id = await _insert_task(session, retry_count=3, max_retries=3)
    worker = _make_worker(engine)
    error = TaskError(error_code='TEST_ERR', message='test')

    async with worker.sf() as s:
        result = await worker._should_retry_task(task_id, error, s)

    assert result is False


# 6d -----------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_should_retry_no_task_options_returns_false(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """task_options=NULL → False (line 1549)."""
    task_id = await _insert_task(session, max_retries=3, task_options=None)
    worker = _make_worker(engine)
    error = TaskError(error_code='TEST_ERR', message='test')

    async with worker.sf() as s:
        result = await worker._should_retry_task(task_id, error, s)

    assert result is False


# 6e -----------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_should_retry_code_matches_returns_true(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """error_code in auto_retry_for + good_until in future → True."""
    opts = _make_task_options(auto_retry_for=['MY_ERR'])
    good_until = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    task_id = await _insert_task(
        session, max_retries=3, task_options=opts, good_until=good_until,
    )
    worker = _make_worker(engine)
    error = TaskError(error_code='MY_ERR', message='test')

    async with worker.sf() as s:
        result = await worker._should_retry_task(task_id, error, s)

    assert result is True


# 6f -----------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_should_retry_code_no_match_returns_false(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """error_code not in auto_retry_for → False."""
    opts = _make_task_options(auto_retry_for=['OTHER_ERR'])
    task_id = await _insert_task(session, max_retries=3, task_options=opts)
    worker = _make_worker(engine)
    error = TaskError(error_code='MY_ERR', message='test')

    async with worker.sf() as s:
        result = await worker._should_retry_task(task_id, error, s)

    assert result is False


# 6g -----------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_should_retry_good_until_expired_returns_false(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """good_until already in the past → False (line 1580)."""
    opts = _make_task_options(auto_retry_for=['MY_ERR'])
    good_until = datetime.now(tz=timezone.utc) - timedelta(minutes=5)
    task_id = await _insert_task(
        session, max_retries=3, task_options=opts, good_until=good_until,
    )
    worker = _make_worker(engine)
    error = TaskError(error_code='MY_ERR', message='test')

    async with worker.sf() as s:
        result = await worker._should_retry_task(task_id, error, s)

    assert result is False


# ---------------------------------------------------------------------------
# _schedule_retry tests
# ---------------------------------------------------------------------------


# 6h-extra: _schedule_retry with nonexistent task → 'reaper_reclaimed' (line 1608)


@pytest.mark.asyncio(loop_scope='function')
async def test_schedule_retry_no_row_returns_reaper_reclaimed(
    engine: AsyncEngine,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Nonexistent task_id → GET_TASK_RETRY_CONFIG_SQL returns no row → 'reaper_reclaimed'."""
    worker = _make_worker(engine)

    async with worker.sf() as s:
        result = await worker._schedule_retry(str(uuid.uuid4()), s)

    assert result == 'reaper_reclaimed'


# 6h-extra: _schedule_retry with NULL task_options → uses default policy (line 1615)


@pytest.mark.asyncio(loop_scope='function')
async def test_schedule_retry_null_task_options_uses_defaults(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """task_options=NULL → retry_policy_data defaults to {}, default intervals used."""
    good_until = datetime.now(tz=timezone.utc) + timedelta(hours=2)
    task_id = await _insert_task(
        session, max_retries=3, task_options=None, good_until=good_until,
    )
    worker = _make_worker(engine)
    worker._spawn_background = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
    worker._schedule_delayed_notification = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]

    async with worker.sf() as s:
        result = await worker._schedule_retry(task_id, s)
        await s.commit()

    assert result == 'scheduled'

    row = await _get_task_row(session, task_id)
    assert row.status == 'PENDING'
    assert row.retry_count == 1


# 6h -----------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_schedule_retry_happy_path_scheduled(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """RUNNING task with valid retry config → 'scheduled', DB updated to PENDING."""
    opts = _make_task_options(auto_retry_for=['ERR'], intervals=[1], jitter=False)
    good_until = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    task_id = await _insert_task(
        session, max_retries=3, task_options=opts, good_until=good_until,
    )
    worker = _make_worker(engine)
    # Prevent real async task + coroutine creation
    worker._spawn_background = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
    worker._schedule_delayed_notification = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]

    async with worker.sf() as s:
        result = await worker._schedule_retry(task_id, s)
        await s.commit()

    assert result == 'scheduled'

    # Verify DB state
    row = await _get_task_row(session, task_id)
    assert row.status == 'PENDING'
    assert row.retry_count == 1
    assert row.next_retry_at is not None

    # Verify delayed notification was spawned
    worker._spawn_background.assert_called_once()


# 6i -----------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_schedule_retry_good_until_exceeded_returns_expired(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """next_retry_at would exceed good_until → 'expired' (Python pre-check)."""
    # Delay=3600s via intervals, good_until=+2s → next_retry_at >> good_until
    opts = _make_task_options(auto_retry_for=['ERR'], intervals=[3600], jitter=False)
    good_until = datetime.now(tz=timezone.utc) + timedelta(seconds=2)
    task_id = await _insert_task(
        session, max_retries=3, task_options=opts, good_until=good_until,
    )
    worker = _make_worker(engine)

    async with worker.sf() as s:
        result = await worker._schedule_retry(task_id, s)

    assert result == 'expired'

    # DB should still be RUNNING (no UPDATE executed)
    row = await _get_task_row(session, task_id)
    assert row.status == 'RUNNING'


# 6j -----------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_schedule_retry_reaper_reclaimed(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Task already FAILED → SCHEDULE SQL guard rejects, postcheck finds
    non-RUNNING → 'reaper_reclaimed'."""
    opts = _make_task_options(auto_retry_for=['ERR'], intervals=[1], jitter=False)
    task_id = await _insert_task(
        session, status='FAILED', max_retries=3, task_options=opts,
    )
    worker = _make_worker(engine)

    async with worker.sf() as s:
        result = await worker._schedule_retry(task_id, s)

    assert result == 'reaper_reclaimed'


# 6k -----------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_schedule_retry_sql_guard_expiry_postcheck_returns_expired(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Simulate TOCTOU race: good_until shortened between CONFIG query and
    SCHEDULE SQL.

    Python pre-check passes (uses original good_until from CONFIG query),
    but SCHEDULE SQL rejects (sees the updated, shorter good_until).
    Postcheck finds RUNNING + next_retry_at >= shortened good_until → 'expired'.
    """
    opts = _make_task_options(auto_retry_for=['ERR'], intervals=[1], jitter=False)
    good_until = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    task_id = await _insert_task(
        session, max_retries=3, task_options=opts, good_until=good_until,
    )
    worker = _make_worker(engine)
    past_good_until = datetime.now(tz=timezone.utc) - timedelta(minutes=5)

    class _InterceptSession:
        """Wraps session to shrink good_until right before the SCHEDULE SQL."""

        def __init__(
            self, real_session: AsyncSession, tid: str, new_gu: datetime,
        ) -> None:
            self._s = real_session
            self._tid = tid
            self._new_gu = new_gu

        async def execute(self, stmt: Any, params: Any = None) -> Any:
            stmt_text = getattr(stmt, 'text', str(stmt))
            # Shrink good_until just before SCHEDULE_TASK_RETRY_SQL
            if "SET status = 'PENDING'" in stmt_text:
                await self._s.execute(
                    text(
                        'UPDATE horsies_tasks SET good_until = :gu WHERE id = :id',
                    ),
                    {'gu': self._new_gu, 'id': self._tid},
                )
            if params is not None:
                return await self._s.execute(stmt, params)
            return await self._s.execute(stmt)

    async with worker.sf() as real_session:
        wrapped = _InterceptSession(real_session, task_id, past_good_until)
        result = await worker._schedule_retry(
            task_id, wrapped,  # type: ignore[arg-type]
        )

    assert result == 'expired'

    # DB should still be RUNNING (SCHEDULE SQL was rejected by guard)
    row = await _get_task_row(session, task_id)
    assert row.status == 'RUNNING'
