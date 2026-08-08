# pyright: reportPrivateUsage=false
"""Integration tests for per-task timeout enforcement.

Covers Worker._handle_task_timeout: the parent-side handler invoked when a
dispatched task outruns TaskOptions.timeout_ms. The SIGKILL targets the
row's recorded worker_pid, confined to live children of the current
executor; these workers have no executor, so the containment skip runs
instead of killing anything.
"""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.codec.task_options import serialize_task_options
from horsies.core.models.tasks import OutcomeCode, TaskOptions
from horsies.core.types.result import Ok, is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker, _parse_timeout_ms
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]

# Owner id shared by the insert helper and the worker under test so the
# ownership guards match.
TEST_WORKER_ID = 'w-timeout-test'
OTHER_WORKER_ID = 'w-timeout-other'

# A pid that cannot exist (pid_max is far below this on macOS and Linux);
# it is also never in the executor's process map, so the containment check
# skips the kill.
FAKE_DEAD_PID = 2**22 - 1


def _make_worker_without_engine() -> Worker:
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    worker = Worker(session_factory=MagicMock(), listener=MagicMock(), cfg=cfg)
    worker.worker_instance_id = TEST_WORKER_ID
    return worker


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
        task_name='timeout_test',
        task_options=task_options,
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs, status,
                 sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha, claimed_by_worker_id,
                 worker_pid, task_options,
                 retention_class_key, command_fingerprint_version,
                 command_fingerprint, retain_rerun_input,
                 prepared_rerun_input_disposition)
            VALUES
                (:id, 'timeout_test', 'default', 100, '[]', '{}', :status,
                 :sent_at, NOW(), NOW(), FALSE, :retry_count,
                 :max_retries, NOW(), :sha, :owner,
                 :worker_pid, :task_options,
                 'standard_30d', 1,
                 sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
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
                SELECT status, error_code, retry_count, claimed_by_worker_id,
                       terminalization_kind
                FROM itest_task_rows WHERE id = CAST(:id AS uuid)
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
    assert row.terminalization_kind == 'FAIL_RUNNING'

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
async def test_timeout_replay_uses_committed_result_without_duplicate_attempt(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A repeated timeout handler replays phase 2 from the terminal row."""
    task_id = await _insert_task(session)
    worker = _make_worker(engine)
    worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
        return_value=Ok(None),
    )

    await worker._handle_task_timeout(task_id, 5_000)
    worker._finalize_workflow_phase.reset_mock()
    await worker._handle_task_timeout(task_id, 5_000)

    worker._finalize_workflow_phase.assert_awaited_once()
    persisted_result = worker._finalize_workflow_phase.await_args.args[1]
    assert persisted_result.is_err()
    assert persisted_result.unwrap_err().error_code == OutcomeCode.TASK_TIMEOUT
    attempt_count = (
        await session.execute(
            text('SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = :id'),
            {'id': task_id},
        )
    ).scalar_one()
    assert attempt_count == 1


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
    options = json.dumps(
        {
            'retry_policy': {
                'max_retries': 3,
                'intervals': [1],
                'backoff_strategy': 'fixed',
                'jitter': False,
                'auto_retry_for': ['TASK_TIMEOUT'],
            },
            'timeout_ms': 5_000,
        }
    )
    task_id = await _insert_task(
        session,
        max_retries=3,
        task_options=options,
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


class TestKillContainment:
    """The timeout SIGKILL only targets live children of the current executor.

    Regression for pid-reuse containment: a concurrent timeout or broken
    pool can restart the executor and reap the child before the kill
    fires, after which the OS may recycle the pid for an unrelated
    process. A raw os.kill on the stale pid would hit that process.
    """

    @staticmethod
    def _kill_recorder(monkeypatch: pytest.MonkeyPatch) -> list[int]:
        import horsies.core.worker.worker as worker_module

        killed: list[int] = []

        def recording_kill(pid: int, sig: int) -> None:
            killed.append(pid)

        monkeypatch.setattr(worker_module.os, 'kill', recording_kill)
        return killed

    def test_pid_in_live_executor_is_killed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        killed = self._kill_recorder(monkeypatch)
        worker = _make_worker_without_engine()
        worker._executor = SimpleNamespace(  # type: ignore[assignment]
            _processes={FAKE_DEAD_PID: object()},
        )

        worker._kill_owned_child(FAKE_DEAD_PID, 't', 5_000)

        assert killed == [FAKE_DEAD_PID]

    def test_stale_pid_not_in_executor_is_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        killed = self._kill_recorder(monkeypatch)
        worker = _make_worker_without_engine()
        worker._executor = SimpleNamespace(_processes={})  # type: ignore[assignment]

        worker._kill_owned_child(FAKE_DEAD_PID, 't', 5_000)

        assert killed == []

    def test_no_executor_skips_kill(self, monkeypatch: pytest.MonkeyPatch) -> None:
        killed = self._kill_recorder(monkeypatch)
        worker = _make_worker_without_engine()

        worker._kill_owned_child(FAKE_DEAD_PID, 't', 5_000)

        assert killed == []


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
