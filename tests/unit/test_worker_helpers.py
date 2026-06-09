"""Tests for pure-logic functions in horsies.core.worker.worker."""

from __future__ import annotations

import asyncio
import os
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from psycopg import InterfaceError, OperationalError

from horsies.core.types.result import Ok, Err
from psycopg.errors import DeadlockDetected, SerializationFailure

from horsies.core.defaults import DEFAULT_CLAIM_LEASE_MS
import logging

from horsies.core.worker.worker import (
    Worker,
    WorkerConfig,
    COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
    COUNT_QUEUE_IN_FLIGHT_SOFT_SQL,
    DELETE_EXPIRED_HEARTBEATS_SQL,
    DELETE_EXPIRED_TASKS_SQL,
    DELETE_EXPIRED_WORKER_STATES_SQL,
    DELETE_EXPIRED_WORKFLOWS_SQL,
    DELETE_EXPIRED_WORKFLOW_TASKS_SQL,
    INSERT_CLAIMER_HEARTBEAT_SQL,
    RENEW_CLAIM_LEASE_SQL,
    UNCLAIM_CLAIMED_TASK_SQL,
    _build_sys_path_roots,
    _dedupe_paths,
    _derive_sys_path_roots_from_file,
    _FinalizeError,
    _FINALIZE_FUTURE_MAX_RETRIES,
    _FINALIZE_PHASE1_MAX_RETRIES,
    _FINALIZE_PHASE2_MAX_RETRIES,
    _FINALIZE_RETRY_BASE_DELAY_S,
    _FINALIZE_RETRY_MAX_DELAY_S,
    _FINALIZE_STAGE_FUTURE,
    _FINALIZE_STAGE_PHASE1,
    _FINALIZE_STAGE_PHASE2,
    _is_retryable_db_error,
    _RequeueOutcome,
)
from horsies.core.models.recovery import RecoveryConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_worker(
    dsn: str = "postgresql+psycopg://u:p@localhost/db",
    psycopg_dsn: str = "postgresql://u:p@localhost/db",
    queues: list[str] | None = None,
    claim_lease_ms: int | None = None,
) -> Worker:
    """Build a Worker with MagicMock session_factory and listener."""
    cfg = WorkerConfig(
        dsn=dsn,
        psycopg_dsn=psycopg_dsn,
        queues=queues or ["default"],
        claim_lease_ms=claim_lease_ms,
    )
    return Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=cfg,
    )


# ---------------------------------------------------------------------------
# 1. _dedupe_paths
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDedupePaths:
    """Tests for _dedupe_paths: order-preserving dedup, skip empties."""

    def test_preserves_order_and_deduplicates(self) -> None:
        assert _dedupe_paths(["a", "b", "a", "c"]) == ["a", "b", "c"]

    def test_filters_empty_strings(self) -> None:
        assert _dedupe_paths(["a", "", "b", "", "c"]) == ["a", "b", "c"]

    def test_empty_input(self) -> None:
        assert _dedupe_paths([]) == []

    def test_all_empty_strings(self) -> None:
        assert _dedupe_paths(["", "", ""]) == []

    def test_single_element(self) -> None:
        assert _dedupe_paths(["x"]) == ["x"]


# ---------------------------------------------------------------------------
# 2. _is_retryable_db_error
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestIsRetryableDbError:
    """Tests for _is_retryable_db_error: match-case on psycopg types."""

    def test_operational_error_returns_true(self) -> None:
        assert _is_retryable_db_error(OperationalError()) is True

    def test_interface_error_returns_true(self) -> None:
        assert _is_retryable_db_error(InterfaceError()) is True

    def test_serialization_failure_returns_true(self) -> None:
        assert _is_retryable_db_error(SerializationFailure()) is True

    def test_deadlock_detected_returns_true(self) -> None:
        assert _is_retryable_db_error(DeadlockDetected()) is True

    def test_value_error_returns_false(self) -> None:
        assert _is_retryable_db_error(ValueError()) is False

    def test_runtime_error_returns_false(self) -> None:
        assert _is_retryable_db_error(RuntimeError()) is False

    def test_generic_exception_returns_false(self) -> None:
        assert _is_retryable_db_error(Exception()) is False


# ---------------------------------------------------------------------------
# 3. _derive_sys_path_roots_from_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDeriveSysPathRootsFromFile:
    """Tests for _derive_sys_path_roots_from_file: parent dir extraction."""

    def test_normal_file_path(self) -> None:
        result = _derive_sys_path_roots_from_file("/foo/bar/baz.py")
        assert result == [os.path.realpath("/foo/bar")]

    def test_relative_file_path(self) -> None:
        result = _derive_sys_path_roots_from_file("some/module.py")
        assert len(result) == 1
        assert os.path.isabs(result[0])

    def test_root_level_file(self) -> None:
        result = _derive_sys_path_roots_from_file("/file.py")
        assert result == ["/"]


# ---------------------------------------------------------------------------
# 4. _build_sys_path_roots
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBuildSysPathRoots:
    """Tests for _build_sys_path_roots: compose and dedupe roots."""

    def test_extra_roots_included_and_made_absolute(self) -> None:
        result = _build_sys_path_roots(
            app_locator="",
            imports=[],
            extra_roots=["./src"],
        )
        assert os.path.abspath("./src") in result

    def test_file_based_app_locator_adds_parent(self) -> None:
        result = _build_sys_path_roots(
            app_locator="/x/app.py:app",
            imports=[],
            extra_roots=[],
        )
        assert os.path.realpath("/x") in result

    def test_module_path_app_locator_skipped(self) -> None:
        result = _build_sys_path_roots(
            app_locator="my.mod:app",
            imports=[],
            extra_roots=[],
        )
        assert result == []

    def test_import_with_py_suffix_adds_parent(self) -> None:
        result = _build_sys_path_roots(
            app_locator="",
            imports=["/y/tasks.py"],
            extra_roots=[],
        )
        assert os.path.realpath("/y") in result

    def test_import_without_py_or_sep_skipped(self) -> None:
        result = _build_sys_path_roots(
            app_locator="",
            imports=["my.tasks"],
            extra_roots=[],
        )
        assert result == []

    def test_duplicates_removed(self) -> None:
        result = _build_sys_path_roots(
            app_locator="/x/app.py:app",
            imports=["/x/tasks.py"],
            extra_roots=[os.path.realpath("/x")],
        )
        # /x appears from extra_roots, app_locator, and import — all resolve
        # to the same realpath, so the list must contain no duplicates.
        assert len(result) == len(set(result))

    def test_empty_inputs(self) -> None:
        result = _build_sys_path_roots(
            app_locator="",
            imports=[],
            extra_roots=[],
        )
        assert result == []


# ---------------------------------------------------------------------------
# 5. Worker._advisory_key_global
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAdvisoryKeyGlobal:
    """Tests for Worker._advisory_key_global: fixed 64-bit key per database.

    Advisory locks are database-scoped, so the key must NOT depend on the
    DSN: workers reaching the same database through different DSN spellings
    (host vs IP, PgBouncer vs direct) must serialize on the same key.
    """

    def test_returns_int(self) -> None:
        w = _make_worker()
        assert isinstance(w._advisory_key_global(), int)

    def test_deterministic(self) -> None:
        w = _make_worker()
        assert w._advisory_key_global() == w._advisory_key_global()

    def test_different_dsn_spellings_share_one_key(self) -> None:
        w1 = _make_worker(psycopg_dsn="postgresql://a@db.internal/db1")
        w2 = _make_worker(psycopg_dsn="postgresql://a@10.0.0.5/db1")
        assert w1._advisory_key_global() == w2._advisory_key_global()

    def test_key_is_dsn_independent(self) -> None:
        w1 = _make_worker(psycopg_dsn="", dsn="postgresql+psycopg://u:p@h/mydb")
        w2 = _make_worker(psycopg_dsn="postgresql://other@elsewhere/db")
        assert w1._advisory_key_global() == w2._advisory_key_global()


# ---------------------------------------------------------------------------
# 6. Worker._compute_claim_expires_at
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestComputeClaimExpiresAt:
    """Tests for Worker._compute_claim_expires_at: always returns datetime."""

    def test_hard_cap_returns_default_lease(self) -> None:
        """Hard-cap mode (claim_lease_ms=None) uses DEFAULT_CLAIM_LEASE_MS."""
        w = _make_worker(claim_lease_ms=None)
        before = datetime.now(timezone.utc)
        result = w._compute_claim_expires_at()
        after = datetime.now(timezone.utc)

        default_ms = DEFAULT_CLAIM_LEASE_MS
        expected_low = before + timedelta(milliseconds=default_ms)
        expected_high = after + timedelta(milliseconds=default_ms)
        assert isinstance(result, datetime)
        assert expected_low <= result <= expected_high

    def test_soft_cap_returns_datetime_close_to_now_plus_lease(self) -> None:
        lease_ms = 5000
        w = _make_worker(claim_lease_ms=lease_ms)
        before = datetime.now(timezone.utc)
        result = w._compute_claim_expires_at()
        after = datetime.now(timezone.utc)

        expected_low = before + timedelta(milliseconds=lease_ms)
        expected_high = after + timedelta(milliseconds=lease_ms)
        assert isinstance(result, datetime)
        assert expected_low <= result <= expected_high

    def test_explicit_override_in_hard_cap_mode(self) -> None:
        """User can set claim_lease_ms in hard-cap mode to override default."""
        w = _make_worker(claim_lease_ms=10_000)
        before = datetime.now(timezone.utc)
        result = w._compute_claim_expires_at()
        after = datetime.now(timezone.utc)

        expected_low = before + timedelta(milliseconds=10_000)
        expected_high = after + timedelta(milliseconds=10_000)
        assert expected_low <= result <= expected_high


# ---------------------------------------------------------------------------
# 7. Worker.stop() shutdown behavior
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWorkerStop:
    """Tests for shutdown behavior of tracked background tasks."""

    @pytest.mark.asyncio
    async def test_stop_drains_finalizers_but_cancels_service_tasks(self) -> None:
        worker = _make_worker()
        worker.listener.close = AsyncMock()

        service_task = worker._spawn_background(asyncio.sleep(999), name='service-test')

        async def _quick_finalizer() -> None:
            await asyncio.sleep(0.01)

        finalizer_task = worker._spawn_background(
            _quick_finalizer(), name='finalizer-test', finalizer=True
        )

        await worker.stop(finalizer_timeout_s=1.0)

        assert service_task.cancelled()
        assert finalizer_task.done()
        assert not finalizer_task.cancelled()
        worker.listener.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_force_cancels_finalizers(self) -> None:
        worker = _make_worker()
        worker.listener.close = AsyncMock()

        never_set = asyncio.Event()
        finalizer_task = worker._spawn_background(
            never_set.wait(), name='finalizer-force', finalizer=True
        )
        await asyncio.sleep(0)

        await worker.stop(force=True, finalizer_timeout_s=1.0)

        assert finalizer_task.cancelled()
        worker.listener.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_timeout_cancels_pending_finalizers(self) -> None:
        worker = _make_worker()
        worker.listener.close = AsyncMock()

        never_set = asyncio.Event()
        finalizer_task = worker._spawn_background(
            never_set.wait(), name='finalizer-timeout', finalizer=True
        )
        await asyncio.sleep(0)

        await worker.stop(finalizer_timeout_s=0.01)

        assert finalizer_task.cancelled()
        worker.listener.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_finalizer_err_result_is_routed_to_finalize_handler(self) -> None:
        worker = _make_worker()
        worker._handle_finalize_error = AsyncMock()  # type: ignore[method-assign]

        async def _failing_finalize() -> Err[RuntimeError]:
            return Err(RuntimeError('finalize result error'))

        task = worker._spawn_background(
            _failing_finalize(),
            name='finalizer-result-err',
            finalizer=True,
        )
        await task
        await asyncio.sleep(0.01)

        worker._handle_finalize_error.assert_awaited_once()


# ---------------------------------------------------------------------------
# 8. Worker._reaper_loop heartbeat retention
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestReaperHeartbeatRetention:
    """Tests for heartbeat retention cleanup in the reaper loop."""

    @pytest.mark.asyncio
    async def test_reaper_prunes_expired_heartbeats(self, monkeypatch: pytest.MonkeyPatch) -> None:
        worker = _make_worker()
        worker.cfg.recovery_config = RecoveryConfig(
            auto_requeue_stale_claimed=False,
            auto_fail_stale_running=False,
            check_interval_ms=1_000,
            worker_state_retention_hours=None,
            terminal_record_retention_hours=None,
        )

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.commit = AsyncMock()
        delete_result = MagicMock(rowcount=3)

        async def _execute(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            if stmt is DELETE_EXPIRED_HEARTBEATS_SQL:
                worker._stop.set()
                return delete_result
            return MagicMock(rowcount=0)

        session.execute = AsyncMock(side_effect=_execute)

        created_brokers: list[Any] = []

        class _FakeBroker:
            def __init__(self, config: Any, **kwargs: Any):
                self.config = config
                self.assume_initialized = kwargs.get('assume_initialized')
                self.app = None
                self.session_factory = MagicMock(return_value=session)
                self.close_async = AsyncMock(return_value=Ok(None))
                created_brokers.append(self)

        recover_mock = AsyncMock(return_value=0)
        monkeypatch.setattr(
            'horsies.core.brokers.postgres.PostgresBroker', _FakeBroker
        )
        monkeypatch.setattr(
            'horsies.core.workflows.recovery.recover_stuck_workflows', recover_mock
        )

        await worker._reaper_loop()

        assert any(
            call.args and call.args[0] is DELETE_EXPIRED_HEARTBEATS_SQL
            for call in session.execute.await_args_list
        )
        assert recover_mock.await_count >= 1
        assert len(created_brokers) == 1
        assert created_brokers[0].assume_initialized is True
        created_brokers[0].close_async.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reaper_reuses_worker_broker_when_available(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When worker broker exists, reaper should reuse it and not construct/close temp broker."""
        worker = _make_worker()
        worker.cfg.recovery_config = RecoveryConfig(
            auto_requeue_stale_claimed=False,
            auto_fail_stale_running=False,
            check_interval_ms=1_000,
            heartbeat_retention_hours=12,
            worker_state_retention_hours=None,
            terminal_record_retention_hours=None,
        )

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.commit = AsyncMock()

        async def _execute(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            if stmt is DELETE_EXPIRED_HEARTBEATS_SQL:
                worker._stop.set()
                return MagicMock(rowcount=2)
            return MagicMock(rowcount=0)

        session.execute = AsyncMock(side_effect=_execute)

        worker_broker = MagicMock()
        worker_broker.session_factory = MagicMock(return_value=session)
        worker_broker.close_async = AsyncMock(return_value=Ok(None))
        worker.broker = worker_broker

        class _UnexpectedBroker:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                raise AssertionError('Reaper should reuse worker broker, not create temp broker')

        recover_mock = AsyncMock(return_value=0)
        monkeypatch.setattr(
            'horsies.core.brokers.postgres.PostgresBroker', _UnexpectedBroker
        )
        monkeypatch.setattr(
            'horsies.core.workflows.recovery.recover_stuck_workflows', recover_mock
        )

        await worker._reaper_loop()

        recover_mock.assert_awaited()
        worker_broker.close_async.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reaper_prunes_worker_state_and_terminal_rows(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        worker = _make_worker()
        worker.cfg.recovery_config = RecoveryConfig(
            auto_requeue_stale_claimed=False,
            auto_fail_stale_running=False,
            check_interval_ms=1_000,
            heartbeat_retention_hours=12,
            worker_state_retention_hours=48,
            terminal_record_retention_hours=72,
        )

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.commit = AsyncMock()

        expected_rowcounts = {
            DELETE_EXPIRED_HEARTBEATS_SQL: 5,
            DELETE_EXPIRED_WORKER_STATES_SQL: 7,
            DELETE_EXPIRED_WORKFLOW_TASKS_SQL: 11,
            DELETE_EXPIRED_WORKFLOWS_SQL: 13,
            DELETE_EXPIRED_TASKS_SQL: 17,
        }

        async def _execute(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            if stmt in expected_rowcounts:
                if stmt is DELETE_EXPIRED_TASKS_SQL:
                    worker._stop.set()
                return MagicMock(rowcount=expected_rowcounts[stmt])
            return MagicMock(rowcount=0)

        session.execute = AsyncMock(side_effect=_execute)

        created_brokers: list[Any] = []

        class _FakeBroker:
            def __init__(self, config: Any, **kwargs: Any):
                self.config = config
                self.assume_initialized = kwargs.get('assume_initialized')
                self.app = None
                self.session_factory = MagicMock(return_value=session)
                self.close_async = AsyncMock(return_value=Ok(None))
                created_brokers.append(self)

        recover_mock = AsyncMock(return_value=0)
        monkeypatch.setattr(
            'horsies.core.brokers.postgres.PostgresBroker', _FakeBroker
        )
        monkeypatch.setattr(
            'horsies.core.workflows.recovery.recover_stuck_workflows', recover_mock
        )

        await worker._reaper_loop()

        executed_statements = [
            call.args[0]
            for call in session.execute.await_args_list
            if call.args
        ]
        for statement in expected_rowcounts:
            assert statement in executed_statements

        assert session.commit.await_count >= 1
        assert recover_mock.await_count >= 1
        assert len(created_brokers) == 1
        assert created_brokers[0].assume_initialized is True
        created_brokers[0].close_async.assert_awaited_once()


# ---------------------------------------------------------------------------
# 9. Claim pipeline: RETURNING payload + lease hardening
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestClaimBatchLockedReturnsPayload:
    """_claim_batch_locked returns dispatch-ready row dicts from RETURNING."""

    @pytest.mark.asyncio
    async def test_claim_returns_row_dicts(self) -> None:
        """CLAIM_SQL RETURNING provides id, task_name, args, kwargs as dicts."""
        worker = _make_worker()

        fake_rows = [
            ('task-1', 'my_app.add', '[]', '{}'),
            ('task-2', 'my_app.mul', '[1, 2]', '{"x": 3}'),
        ]

        fake_result = MagicMock()
        fake_result.keys.return_value = ['id', 'task_name', 'args', 'kwargs']
        fake_result.fetchall.return_value = fake_rows

        session = AsyncMock()
        session.execute = AsyncMock(return_value=fake_result)

        rows = await worker._claim_batch_locked(session, 'default', 5)

        assert len(rows) == 2
        assert rows[0] == {
            'id': 'task-1',
            'task_name': 'my_app.add',
            'args': '[]',
            'kwargs': '{}',
        }
        assert rows[1] == {
            'id': 'task-2',
            'task_name': 'my_app.mul',
            'args': '[1, 2]',
            'kwargs': '{"x": 3}',
        }

    @pytest.mark.asyncio
    async def test_claim_returns_empty_when_nothing_available(self) -> None:
        worker = _make_worker()

        fake_result = MagicMock()
        fake_result.keys.return_value = ['id', 'task_name', 'args', 'kwargs']
        fake_result.fetchall.return_value = []

        session = AsyncMock()
        session.execute = AsyncMock(return_value=fake_result)

        rows = await worker._claim_batch_locked(session, 'default', 5)
        assert rows == []

    @pytest.mark.asyncio
    async def test_claim_passes_claim_lease_ms(self) -> None:
        """Claim passes a finite lease duration for DB-clock expiry."""
        worker = _make_worker(claim_lease_ms=None)

        fake_result = MagicMock()
        fake_result.keys.return_value = ['id', 'task_name', 'args', 'kwargs']
        fake_result.fetchall.return_value = []

        session = AsyncMock()
        session.execute = AsyncMock(return_value=fake_result)

        await worker._claim_batch_locked(session, 'default', 1)

        call_args = session.execute.call_args
        params = call_args[0][1]
        assert params['claim_lease_ms'] == DEFAULT_CLAIM_LEASE_MS


@pytest.mark.unit
class TestClaimerHeartbeatRenewsLease:
    """Claimer heartbeat loop renews claim_expires_at alongside heartbeats."""

    @pytest.mark.asyncio
    async def test_heartbeat_loop_executes_lease_renewal(self) -> None:
        worker = _make_worker()
        worker.cfg.recovery_config = RecoveryConfig(
            claimer_heartbeat_interval_ms=1_000,
        )

        executed_stmts: list[Any] = []

        async def _execute(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            executed_stmts.append(stmt)
            # Stop after first full cycle (heartbeat + renewal)
            if len(executed_stmts) >= 2:
                worker._stop.set()
            return MagicMock(rowcount=0)

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(side_effect=_execute)
        session.commit = AsyncMock()
        worker.sf = MagicMock(return_value=session)

        await worker._claimer_heartbeat_loop()

        assert INSERT_CLAIMER_HEARTBEAT_SQL in executed_stmts
        assert RENEW_CLAIM_LEASE_SQL in executed_stmts

        # Verify renewal params contain a finite lease duration.
        renewal_calls = [
            c for c in session.execute.call_args_list
            if c[0][0] is RENEW_CLAIM_LEASE_SQL
        ]
        assert len(renewal_calls) >= 1
        renewal_params = renewal_calls[0][0][1]
        assert renewal_params['claim_lease_ms'] == DEFAULT_CLAIM_LEASE_MS


# ---------------------------------------------------------------------------
# 10. _requeue_claimed_task: typed _RequeueOutcome paths
# ---------------------------------------------------------------------------


def _make_session_mock(
    *,
    rowcount: int = 0,
    execute_side_effect: Exception | None = None,
) -> AsyncMock:
    """Build an AsyncMock session context for _requeue_claimed_task tests."""
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.commit = AsyncMock()

    result_mock = MagicMock(rowcount=rowcount)
    if execute_side_effect is not None:
        session.execute = AsyncMock(side_effect=execute_side_effect)
    else:
        session.execute = AsyncMock(return_value=result_mock)

    return session


@pytest.mark.unit
class TestRequeueClaimedTask:
    """Tests for _requeue_claimed_task: all three _RequeueOutcome paths."""

    @pytest.mark.asyncio
    async def test_requeue_success_returns_requeued(self) -> None:
        """When DB UPDATE matches a row, outcome is REQUEUED."""
        worker = _make_worker()
        session = _make_session_mock(rowcount=1)
        worker.sf = MagicMock(return_value=session)

        outcome = await worker._requeue_claimed_task('task-1', 'test reason')

        assert outcome is _RequeueOutcome.REQUEUED
        session.execute.assert_awaited_once()
        call_args = session.execute.call_args[0]
        assert call_args[0] is UNCLAIM_CLAIMED_TASK_SQL
        assert call_args[1]['id'] == 'task-1'
        assert call_args[1]['wid'] == worker.worker_instance_id
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_requeue_guard_fail_returns_not_owner(self) -> None:
        """When DB UPDATE matches zero rows, outcome is NOT_OWNER_OR_NOT_CLAIMED."""
        worker = _make_worker()
        session = _make_session_mock(rowcount=0)
        worker.sf = MagicMock(return_value=session)

        outcome = await worker._requeue_claimed_task('task-2', 'owner mismatch')

        assert outcome is _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_requeue_db_error_returns_db_error(self) -> None:
        """When session.execute raises, outcome is DB_ERROR — exception does NOT propagate."""
        worker = _make_worker()
        session = _make_session_mock(execute_side_effect=OperationalError('connection lost'))
        worker.sf = MagicMock(return_value=session)

        outcome = await worker._requeue_claimed_task('task-3', 'db failure')

        assert outcome is _RequeueOutcome.DB_ERROR
        # No exception escaped — the method returned cleanly

    @pytest.mark.asyncio
    async def test_requeue_db_error_does_not_commit(self) -> None:
        """On DB error, commit must not be called (exception fires before commit)."""
        worker = _make_worker()
        session = _make_session_mock(execute_side_effect=RuntimeError('boom'))
        worker.sf = MagicMock(return_value=session)

        await worker._requeue_claimed_task('task-4', 'no commit expected')

        session.commit.assert_not_awaited()


# ---------------------------------------------------------------------------
# 11. _finalize_after call site D: _RequeueOutcome integration
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFinalizeAfterRequeueOutcome:
    """Tests for the except-Exception branch in _finalize_after that uses
    _requeue_claimed_task and builds _FinalizeError with requeue_outcome."""

    @pytest.mark.asyncio
    async def test_future_transient_error_requeue_success_not_retryable(self) -> None:
        """Transient connection error + REQUEUED → retryable=False.

        The task is safely back in the queue, so finalize should NOT retry.
        """
        worker = _make_worker()

        worker._recover_worker_future_failure = AsyncMock(  # type: ignore[method-assign]
            return_value=_RequeueOutcome.REQUEUED,
        )

        # Future that raises a retryable connection error
        fut: asyncio.Future[tuple[bool, str, str | None]] = asyncio.Future()
        fut.set_exception(OperationalError('connection reset'))

        result = await worker._finalize_after(fut, 'task-10')

        assert isinstance(result, Err)
        err: _FinalizeError = result.err_value
        assert err.retryable is False
        assert err.data is not None
        assert err.data['requeue_outcome'] == 'REQUEUED'

    @pytest.mark.asyncio
    async def test_future_transient_error_requeue_db_error_retryable(self) -> None:
        """Transient connection error + DB_ERROR → retryable=True.

        The task is NOT safely requeued, so finalize should retry.
        """
        worker = _make_worker()

        worker._recover_worker_future_failure = AsyncMock(  # type: ignore[method-assign]
            return_value=_RequeueOutcome.DB_ERROR,
        )

        fut: asyncio.Future[tuple[bool, str, str | None]] = asyncio.Future()
        fut.set_exception(OperationalError('original connection error'))

        result = await worker._finalize_after(fut, 'task-11')

        assert isinstance(result, Err)
        err: _FinalizeError = result.err_value
        assert err.retryable is True
        assert err.data is not None
        assert err.data['requeue_outcome'] == 'DB_ERROR'

    @pytest.mark.asyncio
    async def test_future_transient_error_requeue_not_owner_not_retryable(self) -> None:
        """Transient connection error + NOT_OWNER_OR_NOT_CLAIMED → retryable=False.

        The DB update succeeded and this worker no longer owns an in-flight row.
        """
        worker = _make_worker()

        worker._recover_worker_future_failure = AsyncMock(  # type: ignore[method-assign]
            return_value=_RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED,
        )

        fut: asyncio.Future[tuple[bool, str, str | None]] = asyncio.Future()
        fut.set_exception(OperationalError('transient'))

        result = await worker._finalize_after(fut, 'task-12')

        assert isinstance(result, Err)
        err: _FinalizeError = result.err_value
        assert err.retryable is False
        assert err.data is not None
        assert err.data['requeue_outcome'] == 'NOT_OWNER_OR_NOT_CLAIMED'

    @pytest.mark.asyncio
    async def test_future_non_retryable_error_always_not_retryable(self) -> None:
        """Non-retryable future exception → retryable=False regardless of requeue outcome."""
        worker = _make_worker()

        worker._recover_worker_future_failure = AsyncMock(  # type: ignore[method-assign]
            return_value=_RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED,
        )

        fut: asyncio.Future[tuple[bool, str, str | None]] = asyncio.Future()
        fut.set_exception(ValueError('bad data'))

        result = await worker._finalize_after(fut, 'task-13')

        assert isinstance(result, Err)
        err: _FinalizeError = result.err_value
        assert err.retryable is False
        assert err.data is not None
        assert err.data['requeue_outcome'] == 'NOT_OWNER_OR_NOT_CLAIMED'


# ---------------------------------------------------------------------------
# 12. _handle_broken_pool / _dispatch_one: requeue DB error containment
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRequeueDbErrorContainment:
    """Fire-and-forget call sites must not propagate DB errors and must log CRITICAL."""

    @pytest.mark.asyncio
    async def test_handle_broken_pool_db_error_logs_critical(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """_handle_broken_pool logs CRITICAL on DB_ERROR and still restarts executor."""
        worker = _make_worker()

        worker._recover_worker_future_failure = AsyncMock(  # type: ignore[method-assign]
            return_value=_RequeueOutcome.DB_ERROR,
        )
        worker._restart_executor = AsyncMock()  # type: ignore[method-assign]

        _logger = logging.getLogger('horsies.worker')
        _logger.propagate = True
        try:
            with caplog.at_level(logging.CRITICAL, logger='horsies.worker'):
                await worker._handle_broken_pool('task-20', BrokenProcessPool('pool died'))
        finally:
            _logger.propagate = False

        worker._restart_executor.assert_awaited_once()
        critical_messages = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(critical_messages) == 1
        assert 'task-20' in critical_messages[0].message
        assert 'DB_ERROR' in critical_messages[0].message

    @pytest.mark.asyncio
    async def test_dispatch_one_executor_unavailable_db_error_logs_critical(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """_dispatch_one (executor unavailable path) logs CRITICAL on DB_ERROR."""
        worker = _make_worker()
        worker._executor = None

        # _restart_executor keeps executor as None (simulating restart failure)
        worker._restart_executor = AsyncMock()  # type: ignore[method-assign]

        worker._recover_worker_future_failure = AsyncMock(  # type: ignore[method-assign]
            return_value=_RequeueOutcome.DB_ERROR,
        )

        _logger = logging.getLogger('horsies.worker')
        _logger.propagate = True
        try:
            with caplog.at_level(logging.CRITICAL, logger='horsies.worker'):
                await worker._dispatch_one('task-21', 'my_app.add', '[]', '{}')
        finally:
            _logger.propagate = False

        critical_messages = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(critical_messages) == 1
        assert 'task-21' in critical_messages[0].message
        assert 'DB_ERROR' in critical_messages[0].message

    @pytest.mark.asyncio
    async def test_dispatch_one_submit_exception_db_error_logs_critical(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """_dispatch_one (run_in_executor exception path) logs CRITICAL on DB_ERROR."""
        worker = _make_worker()
        worker._executor = MagicMock()

        # Make run_in_executor raise a non-BrokenProcessPool exception synchronously
        loop = asyncio.get_running_loop()
        original_run = loop.run_in_executor

        def _patched_run(executor: Any, fn: Any, *args: Any) -> Any:
            raise RuntimeError('submit failed')

        loop.run_in_executor = _patched_run  # type: ignore[assignment]

        worker._recover_worker_future_failure = AsyncMock(  # type: ignore[method-assign]
            return_value=_RequeueOutcome.DB_ERROR,
        )

        _logger = logging.getLogger('horsies.worker')
        _logger.propagate = True
        try:
            with caplog.at_level(logging.CRITICAL, logger='horsies.worker'):
                await worker._dispatch_one('task-22', 'my_app.add', '[]', '{}')
        finally:
            _logger.propagate = False
            loop.run_in_executor = original_run  # type: ignore[assignment]

        critical_messages = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(critical_messages) == 1
        assert 'task-22' in critical_messages[0].message
        assert 'DB_ERROR' in critical_messages[0].message

    @pytest.mark.asyncio
    async def test_handle_broken_pool_requeue_success_no_critical(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """No CRITICAL log when requeue succeeds in _handle_broken_pool."""
        worker = _make_worker()

        worker._recover_worker_future_failure = AsyncMock(  # type: ignore[method-assign]
            return_value=_RequeueOutcome.REQUEUED,
        )
        worker._restart_executor = AsyncMock()  # type: ignore[method-assign]

        _logger = logging.getLogger('horsies.worker')
        _logger.propagate = True
        try:
            with caplog.at_level(logging.CRITICAL, logger='horsies.worker'):
                await worker._handle_broken_pool('task-23', BrokenProcessPool('pool died'))
        finally:
            _logger.propagate = False

        critical_messages = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(critical_messages) == 0


# ---------------------------------------------------------------------------
# 13. Lease renewal SQL: age guard predicate
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLeaseRenewalAgeGuard:
    """RENEW_CLAIM_LEASE_SQL must filter by claimed_at age to prevent
    indefinite renewal of orphaned CLAIMED tasks."""

    def test_renew_sql_contains_age_guard_predicate(self) -> None:
        """SQL must include claimed_at guard referencing max_claim_age_ms param."""
        from horsies.core.worker.sql import RENEW_CLAIM_LEASE_SQL

        sql_text = RENEW_CLAIM_LEASE_SQL.text
        normalised = ' '.join(sql_text.split())
        assert ':max_claim_age_ms' in normalised
        assert 'claimed_at' in normalised
        assert "INTERVAL '1 millisecond'" in normalised

    def test_renew_sql_still_scoped_to_owner(self) -> None:
        """Age guard does not replace the owner scope — both must be present."""
        from horsies.core.worker.sql import RENEW_CLAIM_LEASE_SQL

        sql_text = RENEW_CLAIM_LEASE_SQL.text
        assert ':wid' in sql_text
        assert "status = 'CLAIMED'" in sql_text

    @pytest.mark.asyncio
    async def test_heartbeat_loop_passes_max_claim_age_ms(self) -> None:
        """Heartbeat loop must pass max_claim_renew_age_ms to RENEW_CLAIM_LEASE_SQL."""
        worker = _make_worker()
        worker.cfg.recovery_config = RecoveryConfig(
            claimer_heartbeat_interval_ms=1_000,
        )
        worker.cfg.max_claim_renew_age_ms = 180_000

        executed_stmts: list[Any] = []
        executed_params: list[Any] = []

        async def _execute(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            executed_stmts.append(stmt)
            if args:
                executed_params.append(args[0])
            # Stop after first full cycle
            if len(executed_stmts) >= 2:
                worker._stop.set()
            return MagicMock(rowcount=0)

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(side_effect=_execute)
        session.commit = AsyncMock()
        worker.sf = MagicMock(return_value=session)

        await worker._claimer_heartbeat_loop()

        assert RENEW_CLAIM_LEASE_SQL in executed_stmts

        # Find the RENEW_CLAIM_LEASE_SQL call params
        renewal_calls = [
            c for c in session.execute.call_args_list
            if c[0][0] is RENEW_CLAIM_LEASE_SQL
        ]
        assert len(renewal_calls) >= 1
        renewal_params = renewal_calls[0][0][1]
        assert 'max_claim_age_ms' in renewal_params
        assert renewal_params['max_claim_age_ms'] == 180_000


# ---------------------------------------------------------------------------
# 14. _handle_finalize_error — retry routing logic
# ---------------------------------------------------------------------------


def _make_finalize_error(
    *,
    stage: str = _FINALIZE_STAGE_PHASE1,
    task_id: str = 'task-fe-1',
    retryable: bool = True,
    message: str = 'test error',
    error_code: str = 'TEST_ERR',
    data: dict[str, Any] | None = None,
) -> _FinalizeError:
    return _FinalizeError(
        error_code=error_code,
        message=message,
        stage=stage,
        task_id=task_id,
        retryable=retryable,
        data=data,
    )


from typing import Any


@pytest.mark.unit
class TestHandleFinalizeError:
    """Tests for Worker._handle_finalize_error — retry routing logic."""

    _LOGGER_NAME = 'horsies.worker'

    # --- U-2a: non-_FinalizeError payload ---

    @pytest.mark.asyncio
    async def test_non_finalize_error_payload_logs_and_returns(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Non-_FinalizeError payload → logs error, no attempts touched."""
        worker = _make_worker()
        worker._spawn_background = MagicMock()  # type: ignore[assignment]
        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger=self._LOGGER_NAME):
                await worker._handle_finalize_error('not a _FinalizeError')
        finally:
            _logger.propagate = False

        assert 'Unexpected finalize error payload type' in caplog.text
        assert worker._spawn_background.call_count == 0
        assert worker._finalize_retry_attempts == {}

    # --- U-2b: unknown stage ---

    @pytest.mark.asyncio
    async def test_unknown_stage_logs_and_returns(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Unknown stage value → logs error with stage info, returns."""
        worker = _make_worker()
        worker._spawn_background = MagicMock()  # type: ignore[assignment]
        err = _make_finalize_error(stage='phase99_unknown')
        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger=self._LOGGER_NAME):
                await worker._handle_finalize_error(err)
        finally:
            _logger.propagate = False

        assert 'phase99_unknown' in caplog.text
        assert worker._spawn_background.call_count == 0

    # --- U-2c: non-retryable error ---

    @pytest.mark.asyncio
    async def test_non_retryable_clears_attempts_no_spawn(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Non-retryable error → logs, clears attempts, no retry spawned."""
        worker = _make_worker()
        worker._spawn_background = MagicMock()  # type: ignore[assignment]
        err = _make_finalize_error(retryable=False)
        key = (err.task_id, err.stage)
        worker._finalize_retry_attempts[key] = 2
        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger=self._LOGGER_NAME):
                await worker._handle_finalize_error(err)
        finally:
            _logger.propagate = False

        assert 'non-retryable' in caplog.text.lower()
        assert key not in worker._finalize_retry_attempts
        assert worker._spawn_background.call_count == 0

    # --- U-2d: retryable, under max_attempts → spawns retry ---

    @pytest.mark.asyncio
    async def test_retryable_under_max_increments_and_spawns(self) -> None:
        """Retryable error under max attempts → increments counter, spawns retry."""
        worker = _make_worker()
        spawn_count = 0

        def _capture_spawn(coro: Any, *, name: str, **kwargs: Any) -> MagicMock:
            nonlocal spawn_count
            spawn_count += 1
            coro.close()
            return MagicMock()

        worker._spawn_background = _capture_spawn  # type: ignore[assignment]
        err = _make_finalize_error(retryable=True, stage=_FINALIZE_STAGE_PHASE1)

        await worker._handle_finalize_error(err)

        key = (err.task_id, _FINALIZE_STAGE_PHASE1)
        assert worker._finalize_retry_attempts[key] == 1
        assert spawn_count == 1

    # --- U-2e: retryable, at max_attempts → CRITICAL, no spawn ---

    @pytest.mark.asyncio
    async def test_retryable_at_max_attempts_logs_critical_no_spawn(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Retryable at max attempts → logs CRITICAL, clears attempts, no spawn."""
        worker = _make_worker()
        worker._spawn_background = MagicMock()  # type: ignore[assignment]
        err = _make_finalize_error(retryable=True, stage=_FINALIZE_STAGE_PHASE1)
        key = (err.task_id, _FINALIZE_STAGE_PHASE1)
        worker._finalize_retry_attempts[key] = _FINALIZE_PHASE1_MAX_RETRIES
        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.CRITICAL, logger=self._LOGGER_NAME):
                await worker._handle_finalize_error(err)
        finally:
            _logger.propagate = False

        assert 'retries exhausted' in caplog.text.lower()
        assert key not in worker._finalize_retry_attempts
        assert worker._spawn_background.call_count == 0

    @pytest.mark.asyncio
    async def test_phase2_at_max_attempts_uses_phase2_limit(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Phase2 uses its own max retries (5), not phase1's (3)."""
        worker = _make_worker()
        spawn_count = 0

        def _capture_spawn(coro: Any, *, name: str, **kwargs: Any) -> MagicMock:
            nonlocal spawn_count
            spawn_count += 1
            coro.close()
            return MagicMock()

        worker._spawn_background = _capture_spawn  # type: ignore[assignment]
        err = _make_finalize_error(retryable=True, stage=_FINALIZE_STAGE_PHASE2)
        key = (err.task_id, _FINALIZE_STAGE_PHASE2)

        # At phase1 max (3) but under phase2 max (5) → should still spawn
        worker._finalize_retry_attempts[key] = _FINALIZE_PHASE1_MAX_RETRIES
        assert _FINALIZE_PHASE1_MAX_RETRIES < _FINALIZE_PHASE2_MAX_RETRIES

        await worker._handle_finalize_error(err)
        assert spawn_count == 1

    @pytest.mark.asyncio
    async def test_future_at_max_attempts_uses_future_limit(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Future-stage errors use their own bounded retry limit."""
        worker = _make_worker()
        worker._spawn_background = MagicMock()  # type: ignore[assignment]
        err = _make_finalize_error(retryable=True, stage=_FINALIZE_STAGE_FUTURE)
        key = (err.task_id, _FINALIZE_STAGE_FUTURE)
        worker._finalize_retry_attempts[key] = _FINALIZE_FUTURE_MAX_RETRIES
        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.CRITICAL, logger=self._LOGGER_NAME):
                await worker._handle_finalize_error(err)
        finally:
            _logger.propagate = False

        assert 'retries exhausted' in caplog.text.lower()
        assert key not in worker._finalize_retry_attempts
        assert worker._spawn_background.call_count == 0

    # --- U-2f: phase1 retryable → spawns _retry_finalize_phase1 ---

    @pytest.mark.asyncio
    async def test_phase1_retryable_spawns_phase1_retry(self) -> None:
        """Phase1 retryable error spawns _retry_finalize_phase1."""
        worker = _make_worker()
        spawned_names: list[str] = []

        def _capture_spawn(coro: Any, *, name: str, **kwargs: Any) -> MagicMock:
            spawned_names.append(name)
            # Close the coroutine to avoid RuntimeWarning
            coro.close()
            return MagicMock()

        worker._spawn_background = _capture_spawn  # type: ignore[assignment]
        err = _make_finalize_error(retryable=True, stage=_FINALIZE_STAGE_PHASE1)

        await worker._handle_finalize_error(err)

        assert len(spawned_names) == 1
        assert 'finalize-retry-phase1' in spawned_names[0]

    # --- U-2g: phase2 retryable → spawns _retry_finalize_phase2 ---

    @pytest.mark.asyncio
    async def test_phase2_retryable_spawns_phase2_retry(self) -> None:
        """Phase2 retryable error spawns _retry_finalize_phase2."""
        worker = _make_worker()
        spawned_names: list[str] = []

        def _capture_spawn(coro: Any, *, name: str, **kwargs: Any) -> MagicMock:
            spawned_names.append(name)
            coro.close()
            return MagicMock()

        worker._spawn_background = _capture_spawn  # type: ignore[assignment]
        err = _make_finalize_error(retryable=True, stage=_FINALIZE_STAGE_PHASE2)

        await worker._handle_finalize_error(err)

        assert len(spawned_names) == 1
        assert 'finalize-retry-phase2' in spawned_names[0]

    @pytest.mark.asyncio
    async def test_future_retryable_spawns_future_retry(self) -> None:
        """Future-stage retryable error spawns _retry_finalize_future."""
        worker = _make_worker()
        spawned_names: list[str] = []

        def _capture_spawn(coro: Any, *, name: str, **kwargs: Any) -> MagicMock:
            spawned_names.append(name)
            coro.close()
            return MagicMock()

        worker._spawn_background = _capture_spawn  # type: ignore[assignment]
        err = _make_finalize_error(retryable=True, stage=_FINALIZE_STAGE_FUTURE)

        await worker._handle_finalize_error(err)

        assert len(spawned_names) == 1
        assert 'finalize-retry-future' in spawned_names[0]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('stage', 'expected_name'),
        [
            (_FINALIZE_STAGE_PHASE1, 'finalize-retry-phase1'),
            (_FINALIZE_STAGE_PHASE2, 'finalize-retry-phase2'),
            (_FINALIZE_STAGE_FUTURE, 'finalize-retry-future'),
        ],
    )
    async def test_retryable_errors_spawn_retry_as_finalizer(
        self,
        stage: str,
        expected_name: str,
    ) -> None:
        """Finalize retry coroutines must drain in Worker.stop(), not be service tasks."""
        worker = _make_worker()
        spawned: list[tuple[str, dict[str, Any]]] = []

        def _capture_spawn(coro: Any, *, name: str, **kwargs: Any) -> MagicMock:
            spawned.append((name, kwargs))
            coro.close()
            return MagicMock()

        worker._spawn_background = _capture_spawn  # type: ignore[assignment]
        err = _make_finalize_error(retryable=True, stage=stage)

        await worker._handle_finalize_error(err)

        assert spawned == [(f'{expected_name}-{err.task_id}', {'finalizer': True})]

    # --- U-2h: backoff delay grows with attempt number, capped ---

    @pytest.mark.asyncio
    async def test_backoff_delay_doubles_per_attempt(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Delay doubles per attempt: 0.5, 1.0, 2.0, 4.0, ... capped at 15s."""
        worker = _make_worker()
        err = _make_finalize_error(
            retryable=True,
            stage=_FINALIZE_STAGE_PHASE2,
            task_id='task-backoff',
        )
        logged_delays: list[float] = []

        def _capture_spawn(coro: Any, *, name: str, **kwargs: Any) -> MagicMock:
            coro.close()
            return MagicMock()

        worker._spawn_background = _capture_spawn  # type: ignore[assignment]
        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            for _ in range(_FINALIZE_PHASE2_MAX_RETRIES):
                with caplog.at_level(logging.WARNING, logger=self._LOGGER_NAME):
                    await worker._handle_finalize_error(err)

                for record in caplog.records:
                    if 'Finalize retry scheduled' in record.message and err.task_id in record.message:
                        parts = record.message.split(' in ')
                        if len(parts) >= 2:
                            delay_str = parts[-1].split('s:')[0]
                            logged_delays.append(float(delay_str))
                caplog.clear()
        finally:
            _logger.propagate = False

        assert len(logged_delays) == _FINALIZE_PHASE2_MAX_RETRIES

        # Verify delays double: attempt 1→0.5, 2→1.0, 3→2.0, 4→4.0, 5→8.0
        for i, delay in enumerate(logged_delays):
            attempt_no = i + 1
            expected = min(
                _FINALIZE_RETRY_MAX_DELAY_S,
                _FINALIZE_RETRY_BASE_DELAY_S * (2 ** (attempt_no - 1)),
            )
            assert delay == expected, (
                f'attempt {attempt_no}: expected {expected}s, got {delay}s'
            )


# ---------------------------------------------------------------------------
# 15. _retry_finalize_future / _retry_finalize_phase1 / _retry_finalize_phase2 — orchestration
# ---------------------------------------------------------------------------


_SENTINEL_FINALIZE_ERR = _FinalizeError(
    error_code='PHASE_ERR',
    message='phase failed',
    stage=_FINALIZE_STAGE_PHASE1,
    task_id='task-rf-1',
    retryable=True,
)

_SENTINEL_TASK_RESULT = MagicMock(name='TaskResult')


@pytest.mark.unit
class TestRetryFinalizeFuture:
    """Tests for Worker._retry_finalize_future."""

    def _make_err(self, *, task_id: str = 'task-rff-1') -> _FinalizeError:
        return _FinalizeError(
            error_code='TEST',
            message='future failed',
            stage=_FINALIZE_STAGE_FUTURE,
            task_id=task_id,
            retryable=True,
        )

    @pytest.mark.asyncio
    async def test_stop_set_returns_without_requeue(self) -> None:
        """When stop is set during sleep, no requeue is attempted."""
        worker = _make_worker()
        worker._stop.set()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        worker._recover_worker_future_failure = AsyncMock()  # type: ignore[assignment]

        await worker._retry_finalize_future(self._make_err(), 1.0)

        worker._recover_worker_future_failure.assert_not_called()

    @pytest.mark.asyncio
    async def test_requeue_success_clears_future_attempts(self) -> None:
        """Successful requeue clears future-stage retry attempts."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        worker._recover_worker_future_failure = AsyncMock(  # type: ignore[assignment]
            return_value=_RequeueOutcome.REQUEUED,
        )
        err = self._make_err()
        key = (err.task_id, _FINALIZE_STAGE_FUTURE)
        worker._finalize_retry_attempts[key] = 2

        await worker._retry_finalize_future(err, 0.0)

        worker._recover_worker_future_failure.assert_awaited_once()
        assert key not in worker._finalize_retry_attempts

    @pytest.mark.asyncio
    async def test_requeue_db_error_delegates_to_handle_finalize_error(self) -> None:
        """Repeated DB_ERROR requeue failures stay on the future-stage retry path."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        worker._recover_worker_future_failure = AsyncMock(  # type: ignore[assignment]
            return_value=_RequeueOutcome.DB_ERROR,
        )
        worker._handle_finalize_error = AsyncMock()  # type: ignore[assignment]
        err = self._make_err()

        await worker._retry_finalize_future(err, 0.0)

        worker._handle_finalize_error.assert_awaited_once()
        retry_err = worker._handle_finalize_error.await_args.args[0]
        assert isinstance(retry_err, _FinalizeError)
        assert retry_err.stage == _FINALIZE_STAGE_FUTURE
        assert retry_err.retryable is True
        assert retry_err.data is not None
        assert retry_err.data['requeue_outcome'] == 'DB_ERROR'

    @pytest.mark.asyncio
    async def test_requeue_not_owner_clears_future_attempts(self) -> None:
        """Ownership miss means this worker has no in-flight row to recover."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        worker._recover_worker_future_failure = AsyncMock(  # type: ignore[assignment]
            return_value=_RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED,
        )
        worker._handle_finalize_error = AsyncMock()  # type: ignore[assignment]
        err = self._make_err()
        key = (err.task_id, _FINALIZE_STAGE_FUTURE)
        worker._finalize_retry_attempts[key] = 1

        await worker._retry_finalize_future(err, 0.0)

        worker._handle_finalize_error.assert_not_called()
        assert key not in worker._finalize_retry_attempts


@pytest.mark.unit
class TestRetryFinalizePhase1:
    """Tests for Worker._retry_finalize_phase1."""

    def _make_err(
        self,
        *,
        outcome: dict[str, Any] | None = None,
        task_id: str = 'task-rf-1',
    ) -> _FinalizeError:
        data = {'outcome': outcome} if outcome is not None else None
        return _FinalizeError(
            error_code='TEST',
            message='test',
            stage=_FINALIZE_STAGE_PHASE1,
            task_id=task_id,
            retryable=True,
            data=data,
        )

    # --- U-3a: stop set → early return ---

    @pytest.mark.asyncio
    async def test_stop_set_returns_without_db_call(self) -> None:
        """When stop is set during sleep, no DB calls are made."""
        worker = _make_worker()
        worker._stop.set()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        worker._persist_task_terminal_state = AsyncMock()  # type: ignore[assignment]

        err = self._make_err(outcome={'ok': True, 'result_json_str': '{}'})
        await worker._retry_finalize_phase1(err, 1.0)

        worker._persist_task_terminal_state.assert_not_called()

    # --- U-3b: missing outcome dict ---

    @pytest.mark.asyncio
    async def test_missing_outcome_logs_error_clears_attempts(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Missing outcome dict → logs error, clears phase1 attempts."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        err = self._make_err()  # data=None → no outcome
        key = (err.task_id, _FINALIZE_STAGE_PHASE1)
        worker._finalize_retry_attempts[key] = 2

        _logger = logging.getLogger('horsies.worker')
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger='horsies.worker'):
                await worker._retry_finalize_phase1(err, 0.0)
        finally:
            _logger.propagate = False

        assert 'missing outcome payload' in caplog.text.lower()
        assert key not in worker._finalize_retry_attempts

    # --- U-3c: missing result_json_str ---

    @pytest.mark.asyncio
    async def test_missing_result_json_str_logs_error_clears_attempts(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Outcome present but result_json_str not a string → logs error, clears."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        err = self._make_err(outcome={'ok': True, 'result_json_str': 123})
        key = (err.task_id, _FINALIZE_STAGE_PHASE1)
        worker._finalize_retry_attempts[key] = 1

        _logger = logging.getLogger('horsies.worker')
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger='horsies.worker'):
                await worker._retry_finalize_phase1(err, 0.0)
        finally:
            _logger.propagate = False

        assert 'missing result_json_str' in caplog.text.lower()
        assert key not in worker._finalize_retry_attempts

    # --- U-3d: happy path ---

    @pytest.mark.asyncio
    async def test_happy_path_calls_persist_and_workflow_clears_both(self) -> None:
        """Happy path: persist succeeds with result, workflow succeeds → clears both stages."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        worker._persist_task_terminal_state = AsyncMock(  # type: ignore[assignment]
            return_value=Ok(_SENTINEL_TASK_RESULT),
        )
        worker._finalize_workflow_phase = AsyncMock(  # type: ignore[assignment]
            return_value=Ok(None),
        )
        err = self._make_err(outcome={
            'ok': True,
            'result_json_str': '{"value": 1}',
            'failed_reason': None,
        })
        key_p1 = (err.task_id, _FINALIZE_STAGE_PHASE1)
        key_p2 = (err.task_id, _FINALIZE_STAGE_PHASE2)
        worker._finalize_retry_attempts[key_p1] = 2
        worker._finalize_retry_attempts[key_p2] = 1

        await worker._retry_finalize_phase1(err, 0.0)

        worker._persist_task_terminal_state.assert_awaited_once()
        worker._finalize_workflow_phase.assert_awaited_once_with(
            err.task_id,
            _SENTINEL_TASK_RESULT,
            queue_name='default',
            is_workflow_task=True,
        )
        assert key_p1 not in worker._finalize_retry_attempts
        assert key_p2 not in worker._finalize_retry_attempts

    @pytest.mark.asyncio
    async def test_happy_path_preserves_finalize_context(self) -> None:
        """Phase-1 replay forwards queue/workflow metadata to phase 2."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        worker._persist_task_terminal_state = AsyncMock(  # type: ignore[assignment]
            return_value=Ok(_SENTINEL_TASK_RESULT),
        )
        worker._finalize_workflow_phase = AsyncMock(  # type: ignore[assignment]
            return_value=Ok(None),
        )
        err = _FinalizeError(
            error_code='TEST',
            message='test',
            stage=_FINALIZE_STAGE_PHASE1,
            task_id='task-rf-context',
            retryable=True,
            data={
                'outcome': {
                    'ok': True,
                    'result_json_str': '{"value": 1}',
                    'failed_reason': None,
                },
                'queue_name': 'critical',
                'is_workflow_task': False,
            },
        )

        await worker._retry_finalize_phase1(err, 0.0)

        worker._finalize_workflow_phase.assert_awaited_once_with(
            err.task_id,
            _SENTINEL_TASK_RESULT,
            queue_name='critical',
            is_workflow_task=False,
        )

    # --- U-3e: _persist fails → delegates ---

    @pytest.mark.asyncio
    async def test_persist_err_delegates_to_handle_finalize_error(self) -> None:
        """When _persist returns Err, delegates to _handle_finalize_error."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        worker._persist_task_terminal_state = AsyncMock(  # type: ignore[assignment]
            return_value=Err(_SENTINEL_FINALIZE_ERR),
        )
        worker._handle_finalize_error = AsyncMock()  # type: ignore[assignment]
        err = self._make_err(outcome={
            'ok': True,
            'result_json_str': '{}',
        })

        await worker._retry_finalize_phase1(err, 0.0)

        worker._handle_finalize_error.assert_awaited_once()
        retry_err = worker._handle_finalize_error.await_args.args[0]
        assert isinstance(retry_err, _FinalizeError)
        assert retry_err.error_code == _SENTINEL_FINALIZE_ERR.error_code
        assert retry_err.stage == _SENTINEL_FINALIZE_ERR.stage
        assert retry_err.data == {
            'queue_name': 'default',
            'is_workflow_task': True,
        }

    # --- U-3f: _persist Ok(None) → clears phase1 only ---

    @pytest.mark.asyncio
    async def test_persist_ok_none_clears_phase1_only(self) -> None:
        """When _persist returns Ok(None) (skip path), clears phase1 only."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        worker._persist_task_terminal_state = AsyncMock(  # type: ignore[assignment]
            return_value=Ok(None),
        )
        worker._finalize_workflow_phase = AsyncMock()  # type: ignore[assignment]
        err = self._make_err(outcome={
            'ok': True,
            'result_json_str': '{}',
        })
        key_p1 = (err.task_id, _FINALIZE_STAGE_PHASE1)
        key_p2 = (err.task_id, _FINALIZE_STAGE_PHASE2)
        worker._finalize_retry_attempts[key_p1] = 2
        worker._finalize_retry_attempts[key_p2] = 1

        await worker._retry_finalize_phase1(err, 0.0)

        worker._finalize_workflow_phase.assert_not_called()
        assert key_p1 not in worker._finalize_retry_attempts
        assert key_p2 in worker._finalize_retry_attempts  # not cleared

    # --- U-3g: phase2 fails → delegates ---

    @pytest.mark.asyncio
    async def test_workflow_phase_err_delegates_to_handle_finalize_error(self) -> None:
        """When _finalize_workflow_phase returns Err, delegates to handler."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        phase2_err = _FinalizeError(
            error_code='WF_ERR',
            message='workflow failed',
            stage=_FINALIZE_STAGE_PHASE2,
            task_id='task-rf-1',
            retryable=True,
        )
        worker._persist_task_terminal_state = AsyncMock(  # type: ignore[assignment]
            return_value=Ok(_SENTINEL_TASK_RESULT),
        )
        worker._finalize_workflow_phase = AsyncMock(  # type: ignore[assignment]
            return_value=Err(phase2_err),
        )
        worker._handle_finalize_error = AsyncMock()  # type: ignore[assignment]
        err = self._make_err(outcome={
            'ok': True,
            'result_json_str': '{}',
        })

        await worker._retry_finalize_phase1(err, 0.0)

        worker._handle_finalize_error.assert_awaited_once_with(phase2_err)


@pytest.mark.unit
class TestRetryFinalizePhase2:
    """Tests for Worker._retry_finalize_phase2."""

    def _make_err(self, *, task_id: str = 'task-rf2-1') -> _FinalizeError:
        return _FinalizeError(
            error_code='TEST',
            message='test',
            stage=_FINALIZE_STAGE_PHASE2,
            task_id=task_id,
            retryable=True,
        )

    # --- U-3h: stop set → early return ---

    @pytest.mark.asyncio
    async def test_stop_set_returns_without_load(self) -> None:
        """When stop is set during sleep, no load or workflow calls made."""
        worker = _make_worker()
        worker._stop.set()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        worker._load_persisted_task_result = AsyncMock()  # type: ignore[assignment]

        err = self._make_err()
        await worker._retry_finalize_phase2(err, 1.0)

        worker._load_persisted_task_result.assert_not_called()

    # --- U-3i: _load fails → delegates ---

    @pytest.mark.asyncio
    async def test_load_err_delegates_to_handle_finalize_error(self) -> None:
        """When _load_persisted_task_result returns Err, delegates to handler."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        load_err = _FinalizeError(
            error_code='LOAD_ERR',
            message='load failed',
            stage=_FINALIZE_STAGE_PHASE2,
            task_id='task-rf2-1',
            retryable=True,
        )
        worker._load_persisted_task_result = AsyncMock(  # type: ignore[assignment]
            return_value=Err(load_err),
        )
        worker._handle_finalize_error = AsyncMock()  # type: ignore[assignment]

        err = self._make_err()
        await worker._retry_finalize_phase2(err, 0.0)

        worker._handle_finalize_error.assert_awaited_once_with(load_err)

    # --- U-3j: happy path ---

    @pytest.mark.asyncio
    async def test_happy_path_calls_workflow_clears_phase2(self) -> None:
        """Happy path: load succeeds, workflow succeeds → clears phase2 attempts."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        worker._load_persisted_task_result = AsyncMock(  # type: ignore[assignment]
            return_value=Ok(_SENTINEL_TASK_RESULT),
        )
        worker._finalize_workflow_phase = AsyncMock(  # type: ignore[assignment]
            return_value=Ok(None),
        )
        err = self._make_err()
        key = (err.task_id, _FINALIZE_STAGE_PHASE2)
        worker._finalize_retry_attempts[key] = 3

        await worker._retry_finalize_phase2(err, 0.0)

        worker._finalize_workflow_phase.assert_awaited_once_with(
            err.task_id,
            _SENTINEL_TASK_RESULT,
            queue_name='default',
            is_workflow_task=True,
        )
        assert key not in worker._finalize_retry_attempts

    @pytest.mark.asyncio
    async def test_happy_path_preserves_finalize_context(self) -> None:
        """Phase-2 replay forwards queue/workflow metadata."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        worker._load_persisted_task_result = AsyncMock(  # type: ignore[assignment]
            return_value=Ok(_SENTINEL_TASK_RESULT),
        )
        worker._finalize_workflow_phase = AsyncMock(  # type: ignore[assignment]
            return_value=Ok(None),
        )
        err = _FinalizeError(
            error_code='TEST',
            message='test',
            stage=_FINALIZE_STAGE_PHASE2,
            task_id='task-rf2-context',
            retryable=True,
            data={'queue_name': 'critical', 'is_workflow_task': False},
        )

        await worker._retry_finalize_phase2(err, 0.0)

        worker._finalize_workflow_phase.assert_awaited_once_with(
            err.task_id,
            _SENTINEL_TASK_RESULT,
            queue_name='critical',
            is_workflow_task=False,
        )

    # --- U-3k: _finalize_workflow fails → delegates ---

    @pytest.mark.asyncio
    async def test_workflow_err_delegates_to_handle_finalize_error(self) -> None:
        """When _finalize_workflow_phase returns Err, delegates to handler."""
        worker = _make_worker()
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        wf_err = _FinalizeError(
            error_code='WF_ERR',
            message='workflow failed',
            stage=_FINALIZE_STAGE_PHASE2,
            task_id='task-rf2-1',
            retryable=True,
        )
        worker._load_persisted_task_result = AsyncMock(  # type: ignore[assignment]
            return_value=Ok(_SENTINEL_TASK_RESULT),
        )
        worker._finalize_workflow_phase = AsyncMock(  # type: ignore[assignment]
            return_value=Err(wf_err),
        )
        worker._handle_finalize_error = AsyncMock()  # type: ignore[assignment]

        err = self._make_err()
        await worker._retry_finalize_phase2(err, 0.0)

        worker._handle_finalize_error.assert_awaited_once_with(wf_err)


# ---------------------------------------------------------------------------
# 16. Lifecycle utilities
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRequestStop:
    """Tests for Worker.request_stop."""

    # --- U-4a ---

    def test_request_stop_sets_event(self) -> None:
        """request_stop() sets the internal _stop event."""
        worker = _make_worker()
        assert not worker._stop.is_set()
        worker.request_stop()
        assert worker._stop.is_set()


@pytest.mark.unit
class TestSleepWithStop:
    """Tests for Worker._sleep_with_stop."""

    # --- U-4b ---

    @pytest.mark.asyncio
    async def test_sleeps_full_duration_when_stop_not_set(self) -> None:
        """When stop is never set, sleeps the full duration then returns."""
        worker = _make_worker()
        start = asyncio.get_event_loop().time()
        await worker._sleep_with_stop(0.05)
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed >= 0.04  # allow small tolerance

    # --- U-4c ---

    @pytest.mark.asyncio
    async def test_returns_early_when_stop_set(self) -> None:
        """When stop is set during sleep, returns before full duration."""
        worker = _make_worker()

        async def _set_stop_soon() -> None:
            await asyncio.sleep(0.02)
            worker._stop.set()

        asyncio.create_task(_set_stop_soon())
        start = asyncio.get_event_loop().time()
        await worker._sleep_with_stop(5.0)  # would hang without stop
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed < 1.0


@pytest.mark.unit
class TestRestartExecutor:
    """Tests for Worker._restart_executor."""

    # --- U-4d ---

    @pytest.mark.asyncio
    async def test_stop_set_is_noop(self) -> None:
        """When stop is set, _restart_executor does nothing."""
        worker = _make_worker()
        worker._stop.set()
        worker._create_executor = MagicMock()  # type: ignore[assignment]

        await worker._restart_executor('test reason')

        worker._create_executor.assert_not_called()

    # --- U-4e ---

    @pytest.mark.asyncio
    async def test_executor_none_creates_new(self) -> None:
        """When executor is None, creates a new one without shutdown."""
        worker = _make_worker()
        worker._executor = None
        mock_executor = MagicMock()
        worker._create_executor = MagicMock(return_value=mock_executor)  # type: ignore[assignment]
        worker._warm_executor = AsyncMock()  # type: ignore[assignment]

        await worker._restart_executor('test reason')

        worker._create_executor.assert_called_once_with(
            avoid_parent_fd_inheritance=False
        )
        worker._warm_executor.assert_awaited_once()
        assert worker._executor is mock_executor

    @pytest.mark.asyncio
    async def test_executor_none_uses_spawn_safe_context_after_parent_db_opens(
        self,
    ) -> None:
        """Replacement executors avoid inheriting parent DB sockets after startup."""
        worker = _make_worker()
        worker._parent_db_sockets_open = True
        worker._executor = None
        mock_executor = MagicMock()
        worker._create_executor = MagicMock(return_value=mock_executor)  # type: ignore[assignment]
        worker._warm_executor = AsyncMock()  # type: ignore[assignment]

        await worker._restart_executor('test reason')

        worker._create_executor.assert_called_once_with(
            avoid_parent_fd_inheritance=True
        )
        worker._warm_executor.assert_awaited_once()
        assert worker._executor is mock_executor

    # --- U-4f ---

    @pytest.mark.asyncio
    async def test_executor_exists_shuts_down_and_recreates(self) -> None:
        """When executor exists, shuts it down then creates a new one."""
        worker = _make_worker()
        old_executor = MagicMock()
        new_executor = MagicMock()
        worker._executor = old_executor
        worker._create_executor = MagicMock(return_value=new_executor)  # type: ignore[assignment]
        worker._warm_executor = AsyncMock()  # type: ignore[assignment]

        await worker._restart_executor('broken pool')

        old_executor.shutdown.assert_called_once_with(wait=True, cancel_futures=True)
        worker._create_executor.assert_called_once_with(
            avoid_parent_fd_inheritance=False
        )
        worker._warm_executor.assert_awaited_once()
        assert worker._executor is new_executor

    # --- U-4g ---

    @pytest.mark.asyncio
    async def test_shutdown_error_still_creates_new(self) -> None:
        """When shutdown raises, still creates a new executor."""
        worker = _make_worker()
        old_executor = MagicMock()
        old_executor.shutdown.side_effect = RuntimeError('shutdown boom')
        new_executor = MagicMock()
        worker._executor = old_executor
        worker._create_executor = MagicMock(return_value=new_executor)  # type: ignore[assignment]
        worker._warm_executor = AsyncMock()  # type: ignore[assignment]

        await worker._restart_executor('broken pool')

        assert worker._executor is new_executor

    @pytest.mark.asyncio
    async def test_concurrent_restarts_share_one_replacement(self) -> None:
        """Concurrent restart requests should not create orphaned replacement executors."""
        worker = _make_worker()
        old_executor = MagicMock()
        worker._executor = old_executor
        replacements = [MagicMock(name=f'executor-{idx}') for idx in range(12)]
        worker._create_executor = MagicMock(side_effect=replacements)  # type: ignore[assignment]
        worker._warm_executor = AsyncMock()  # type: ignore[assignment]

        await asyncio.gather(
            *(
                worker._restart_executor(
                    f'broken pool {idx}',
                    failed_executor=old_executor,
                )
                for idx in range(12)
            )
        )

        old_executor.shutdown.assert_called_once_with(wait=True, cancel_futures=True)
        worker._create_executor.assert_called_once()
        worker._warm_executor.assert_awaited_once()
        assert worker._executor is replacements[0]

    @pytest.mark.asyncio
    async def test_stale_restart_request_does_not_replace_current_executor(self) -> None:
        """A late handler for an old broken executor should not restart its replacement."""
        worker = _make_worker()
        old_executor = MagicMock()
        worker._executor = old_executor
        first_replacement = MagicMock(name='first-replacement')
        second_replacement = MagicMock(name='second-replacement')
        worker._create_executor = MagicMock(  # type: ignore[assignment]
            side_effect=[first_replacement, second_replacement]
        )
        worker._warm_executor = AsyncMock()  # type: ignore[assignment]

        await worker._restart_executor('first broken pool', failed_executor=old_executor)
        await worker._restart_executor('late broken pool', failed_executor=old_executor)

        old_executor.shutdown.assert_called_once_with(wait=True, cancel_futures=True)
        worker._create_executor.assert_called_once()
        worker._warm_executor.assert_awaited_once()
        assert worker._executor is first_replacement


@pytest.mark.unit
class TestCleanupAfterFailedStart:
    """Tests for Worker._cleanup_after_failed_start."""

    # --- U-4h ---

    @pytest.mark.asyncio
    async def test_cleans_listener_and_executor(self) -> None:
        """Closes listener and shuts down executor."""
        worker = _make_worker()
        worker.listener = AsyncMock()
        mock_executor = MagicMock()
        worker._executor = mock_executor

        await worker._cleanup_after_failed_start()

        worker.listener.close.assert_awaited_once()
        mock_executor.shutdown.assert_called_once_with(wait=True, cancel_futures=True)
        assert worker._executor is None

    # --- U-4i ---

    @pytest.mark.asyncio
    async def test_listener_close_error_logged(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Listener close error is logged, does not propagate."""
        worker = _make_worker()
        worker.listener = AsyncMock()
        worker.listener.close.side_effect = RuntimeError('listener boom')
        worker._executor = None

        _logger = logging.getLogger('horsies.worker')
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger='horsies.worker'):
                await worker._cleanup_after_failed_start()
        finally:
            _logger.propagate = False

        assert 'listener' in caplog.text.lower()

    # --- U-4j ---

    @pytest.mark.asyncio
    async def test_executor_shutdown_error_logged(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Executor shutdown error is logged, does not propagate."""
        worker = _make_worker()
        worker.listener = AsyncMock()
        mock_executor = MagicMock()
        mock_executor.shutdown.side_effect = RuntimeError('executor boom')
        worker._executor = mock_executor

        _logger = logging.getLogger('horsies.worker')
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger='horsies.worker'):
                await worker._cleanup_after_failed_start()
        finally:
            _logger.propagate = False

        assert 'executor' in caplog.text.lower()


@pytest.mark.unit
class TestHandleRetryableStartError:
    """Tests for Worker._handle_retryable_start_error."""

    def _make_backoff(self, *, can_retry: bool = True) -> MagicMock:
        backoff = MagicMock()
        backoff.can_retry.return_value = can_retry
        backoff.next_delay_seconds.return_value = 0.01
        backoff.attempts = 1
        backoff.max_attempts = 3
        return backoff

    # --- U-4k ---

    @pytest.mark.asyncio
    async def test_cannot_retry_raises(self) -> None:
        """When can_retry is False, re-raises the exception."""
        worker = _make_worker()
        backoff = self._make_backoff(can_retry=False)

        with pytest.raises(RuntimeError, match='start failed'):
            try:
                raise RuntimeError('start failed')
            except RuntimeError as exc:
                await worker._handle_retryable_start_error(exc, backoff)

    # --- U-4l ---

    @pytest.mark.asyncio
    async def test_can_retry_cleans_up_and_sleeps(self) -> None:
        """When can_retry is True, cleans up and sleeps."""
        worker = _make_worker()
        worker._cleanup_after_failed_start = AsyncMock()  # type: ignore[assignment]
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]
        backoff = self._make_backoff(can_retry=True)
        exc = RuntimeError('start failed')

        await worker._handle_retryable_start_error(exc, backoff)

        worker._cleanup_after_failed_start.assert_awaited_once()
        worker._sleep_with_stop.assert_awaited_once()


@pytest.mark.unit
class TestStartWithResilienceConfig:
    """Tests for Worker._start_with_resilience_config."""

    # --- U-4m ---

    @pytest.mark.asyncio
    async def test_timeout_triggers_retry(self) -> None:
        """asyncio.TimeoutError on start() triggers retry via _handle_retryable_start_error."""
        worker = _make_worker()
        call_count = 0

        async def _start_side_effect() -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise asyncio.TimeoutError()
            # Second call succeeds

        worker.start = AsyncMock(side_effect=_start_side_effect)  # type: ignore[assignment]
        worker._handle_retryable_start_error = AsyncMock()  # type: ignore[assignment]

        await worker._start_with_resilience_config()

        assert call_count == 2
        worker._handle_retryable_start_error.assert_awaited_once()

    # --- U-4n ---

    @pytest.mark.asyncio
    async def test_retryable_connection_error_triggers_retry(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retryable connection error triggers retry."""
        worker = _make_worker()
        call_count = 0

        async def _start_side_effect() -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OperationalError('connection refused')

        worker.start = AsyncMock(side_effect=_start_side_effect)  # type: ignore[assignment]
        worker._handle_retryable_start_error = AsyncMock()  # type: ignore[assignment]
        monkeypatch.setattr(
            'horsies.core.worker.worker.is_retryable_connection_error',
            lambda exc: isinstance(exc, OperationalError),
        )

        await worker._start_with_resilience_config()

        assert call_count == 2
        worker._handle_retryable_start_error.assert_awaited_once()

    # --- U-4o ---

    @pytest.mark.asyncio
    async def test_non_retryable_error_raises(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Non-retryable error propagates immediately."""
        worker = _make_worker()
        worker.start = AsyncMock(side_effect=ValueError('bad config'))  # type: ignore[assignment]
        monkeypatch.setattr(
            'horsies.core.worker.worker.is_retryable_connection_error',
            lambda exc: False,
        )

        with pytest.raises(ValueError, match='bad config'):
            await worker._start_with_resilience_config()

    # --- U-4p ---

    @pytest.mark.asyncio
    async def test_success_returns_immediately(self) -> None:
        """Successful start() returns without retries."""
        worker = _make_worker()
        worker.start = AsyncMock()  # type: ignore[assignment]
        worker._handle_retryable_start_error = AsyncMock()  # type: ignore[assignment]

        await worker._start_with_resilience_config()

        worker.start.assert_awaited_once()
        worker._handle_retryable_start_error.assert_not_called()


@pytest.mark.unit
class TestWorkerStartConnectionOrdering:
    """Tests for fork-safe worker startup ordering."""

    @pytest.mark.asyncio
    async def test_warms_children_before_parent_listener_starts(self) -> None:
        worker = _make_worker()
        events: list[str] = []

        worker._preload_modules_main = MagicMock()  # type: ignore[assignment]
        worker._create_executor = MagicMock(  # type: ignore[assignment]
            side_effect=lambda **_kwargs: events.append('create-executor')
            or MagicMock()
        )

        async def _warm() -> None:
            events.append('warm-executor')

        async def _listener_start():
            events.append('listener-start')
            return Ok(None)

        worker._warm_executor = AsyncMock(side_effect=_warm)  # type: ignore[assignment]
        worker.listener.start = AsyncMock(side_effect=_listener_start)
        worker.listener.listen_many = AsyncMock(
            return_value=Ok([asyncio.Queue(), asyncio.Queue()])
        )
        worker.listener.listen = AsyncMock(return_value=Ok(asyncio.Queue()))

        def _close_spawned(coro, **kwargs):  # type: ignore[no-untyped-def]
            coro.close()
            return MagicMock()

        worker._spawn_background = MagicMock(side_effect=_close_spawned)  # type: ignore[assignment]

        await worker.start()

        assert events == ['create-executor', 'warm-executor', 'listener-start']


# ---------------------------------------------------------------------------
# 17. Reaper loop — uncovered match arms
# ---------------------------------------------------------------------------

from horsies.core.brokers.result_types import BrokerOperationError, BrokerErrorCode


def _make_reaper_worker(
    monkeypatch: pytest.MonkeyPatch,
    *,
    auto_requeue: bool = False,
    auto_fail: bool = False,
    requeue_results: list[Any] | None = None,
    mark_failed_results: list[Any] | None = None,
    broker_close_result: Any = None,
) -> Worker:
    """Build a worker + fake broker for reaper loop tests.

    Results lists are consumed in order. The worker is stopped after
    the last result is consumed for the active operation.
    """
    worker = _make_worker()
    worker.cfg.recovery_config = RecoveryConfig(
        auto_requeue_stale_claimed=auto_requeue,
        auto_fail_stale_running=auto_fail,
        check_interval_ms=1_000,
        heartbeat_retention_hours=None,
        worker_state_retention_hours=None,
        terminal_record_retention_hours=None,
    )

    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.commit = AsyncMock()
    session.execute = AsyncMock(return_value=MagicMock(rowcount=0))

    _requeue_results = list(requeue_results or [Ok(0)])
    _requeue_idx = 0

    async def _requeue_side_effect(**kwargs: Any) -> Any:
        nonlocal _requeue_idx
        idx = min(_requeue_idx, len(_requeue_results) - 1)
        _requeue_idx += 1
        result = _requeue_results[idx]
        if _requeue_idx >= len(_requeue_results):
            worker._stop.set()
        return result

    _mark_results = list(mark_failed_results or [Ok(0)])
    _mark_idx = 0

    async def _mark_failed_side_effect(**kwargs: Any) -> Any:
        nonlocal _mark_idx
        idx = min(_mark_idx, len(_mark_results) - 1)
        _mark_idx += 1
        result = _mark_results[idx]
        if _mark_idx >= len(_mark_results):
            worker._stop.set()
        return result

    close_rv = broker_close_result if broker_close_result is not None else Ok(None)
    created_brokers: list[Any] = []

    class _FakeBroker:
        def __init__(self, config: Any, **kwargs: Any) -> None:
            created_brokers.append(self)
            self.config = config
            self.assume_initialized = kwargs.get('assume_initialized')
            self.session_factory = MagicMock(return_value=session)
            self.close_async = AsyncMock(return_value=close_rv)
            self.requeue_stale_claimed = _requeue_side_effect
            self.mark_stale_tasks_as_failed = _mark_failed_side_effect
            self.expire_pending_tasks = AsyncMock(return_value=Ok(0))
            self.app: Any = None

    monkeypatch.setattr(
        'horsies.core.brokers.postgres.PostgresBroker', _FakeBroker,
    )
    monkeypatch.setattr(
        'horsies.core.workflows.recovery.recover_stuck_workflows',
        AsyncMock(return_value=0),
    )

    worker._test_created_brokers = created_brokers  # type: ignore[attr-defined]
    return worker


def _broker_err(*, retryable: bool = False, message: str = 'test error') -> Err[BrokerOperationError]:
    return Err(BrokerOperationError(
        code=BrokerErrorCode.CLEANUP_FAILED,
        message=message,
        retryable=retryable,
    ))


@pytest.mark.unit
class TestReaperMatchArms:
    """Tests for reaper loop match arms: requeue/mark_failed Ok/transient/permanent paths."""

    _LOGGER_NAME = 'horsies.worker'

    # --- U-6a: requeue Ok(>0) → resets counter, logs ---

    @pytest.mark.asyncio
    async def test_requeue_ok_resets_counter_and_logs(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Ok(n>0) from requeue resets permanent failure counter and logs info."""
        worker = _make_reaper_worker(
            monkeypatch,
            auto_requeue=True,
            requeue_results=[Ok(5)],
        )

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.INFO, logger=self._LOGGER_NAME):
                await worker._reaper_loop()
        finally:
            _logger.propagate = False

        assert 'requeued 5 stale' in caplog.text.lower()

    # --- U-6b: requeue transient Err → resets counter ---

    @pytest.mark.asyncio
    async def test_requeue_transient_err_resets_counter(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Transient Err from requeue resets permanent failure counter."""
        worker = _make_reaper_worker(
            monkeypatch,
            auto_requeue=True,
            requeue_results=[_broker_err(retryable=True, message='transient db')],
        )

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.WARNING, logger=self._LOGGER_NAME):
                await worker._reaper_loop()
        finally:
            _logger.propagate = False

        assert 'transient' in caplog.text.lower()

    # --- U-6c: requeue permanent Err 3x → disables ---

    @pytest.mark.asyncio
    async def test_requeue_permanent_err_3x_disables(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """3 consecutive permanent errors disables requeue operation."""
        perm = _broker_err(retryable=False, message='schema drift')
        worker = _make_reaper_worker(
            monkeypatch,
            auto_requeue=True,
            requeue_results=[perm, perm, perm],
        )

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger=self._LOGGER_NAME):
                await worker._reaper_loop()
        finally:
            _logger.propagate = False

        assert 'disabled' in caplog.text.lower()
        critical_records = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(critical_records) >= 1

    # --- U-6d: mark_stale_tasks_as_failed: same three arms ---

    @pytest.mark.asyncio
    async def test_mark_failed_ok_logs_warning(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Ok(n>0) from mark_stale_tasks_as_failed logs warning."""
        worker = _make_reaper_worker(
            monkeypatch,
            auto_fail=True,
            mark_failed_results=[Ok(3)],
        )

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.WARNING, logger=self._LOGGER_NAME):
                await worker._reaper_loop()
        finally:
            _logger.propagate = False

        assert 'marked 3 stale running' in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_mark_failed_transient_err_resets_counter(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Transient Err from mark_stale_tasks_as_failed resets counter."""
        worker = _make_reaper_worker(
            monkeypatch,
            auto_fail=True,
            mark_failed_results=[_broker_err(retryable=True, message='transient mark')],
        )

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.WARNING, logger=self._LOGGER_NAME):
                await worker._reaper_loop()
        finally:
            _logger.propagate = False

        assert 'transient' in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_mark_failed_permanent_err_3x_disables(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """3 consecutive permanent errors disables mark_failed operation."""
        perm = _broker_err(retryable=False, message='perm mark')
        worker = _make_reaper_worker(
            monkeypatch,
            auto_fail=True,
            mark_failed_results=[perm, perm, perm],
        )

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger=self._LOGGER_NAME):
                await worker._reaper_loop()
        finally:
            _logger.propagate = False

        assert 'disabled' in caplog.text.lower()
        critical_records = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(critical_records) >= 1

    # --- U-6e: no worker broker → fallback to temp ---

    @pytest.mark.asyncio
    async def test_missing_worker_broker_falls_back_to_temp(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When worker.broker is absent, reaper constructs a worker-sized temp broker."""
        worker = _make_reaper_worker(
            monkeypatch,
            auto_requeue=True,
            requeue_results=[Ok(0)],
        )

        await worker._reaper_loop()

        assert worker.broker is None
        created = worker._test_created_brokers  # type: ignore[attr-defined]
        assert len(created) == 1
        assert created[0].config.pool_size == worker.cfg.parent_pool_size
        assert created[0].config.max_overflow == worker.cfg.parent_max_overflow
        created[0].close_async.assert_awaited_once()

    # --- U-6f: loop-level exception → logged, continues ---

    @pytest.mark.asyncio
    async def test_loop_level_exception_logged_continues(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Unexpected exception in loop body is logged and loop continues."""
        worker = _make_worker()
        worker.cfg.recovery_config = RecoveryConfig(
            auto_requeue_stale_claimed=True,
            auto_fail_stale_running=False,
            check_interval_ms=1_000,
            heartbeat_retention_hours=None,
            worker_state_retention_hours=None,
            terminal_record_retention_hours=None,
        )

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.commit = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock(rowcount=0))

        loop_call_count = 0

        async def _requeue_with_boom(**kwargs: Any) -> Any:
            nonlocal loop_call_count
            loop_call_count += 1
            if loop_call_count == 1:
                raise RuntimeError('unexpected boom')
            worker._stop.set()
            return Ok(0)

        class _BoomBroker:
            def __init__(self, config: Any, **_kwargs: Any) -> None:
                self.session_factory = MagicMock(return_value=session)
                self.close_async = AsyncMock(return_value=Ok(None))
                self.requeue_stale_claimed = _requeue_with_boom
                self.mark_stale_tasks_as_failed = AsyncMock(return_value=Ok(0))
                self.app: Any = None

        monkeypatch.setattr(
            'horsies.core.brokers.postgres.PostgresBroker', _BoomBroker,
        )
        monkeypatch.setattr(
            'horsies.core.workflows.recovery.recover_stuck_workflows',
            AsyncMock(return_value=0),
        )

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger=self._LOGGER_NAME):
                await worker._reaper_loop()
        finally:
            _logger.propagate = False

        assert 'reaper loop error' in caplog.text.lower()
        assert loop_call_count >= 2  # continued after error

    # --- U-6g: CancelledError → clean exit ---

    @pytest.mark.asyncio
    async def test_cancelled_error_clean_exit(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """CancelledError in reaper loop results in clean exit with log."""
        worker = _make_worker()
        worker.cfg.recovery_config = RecoveryConfig(
            auto_requeue_stale_claimed=True,
            auto_fail_stale_running=False,
            check_interval_ms=1_000,
            heartbeat_retention_hours=None,
            worker_state_retention_hours=None,
            terminal_record_retention_hours=None,
        )

        async def _cancel_on_requeue(**kwargs: Any) -> Any:
            raise asyncio.CancelledError()

        class _CancelBroker:
            def __init__(self, config: Any, **_kwargs: Any) -> None:
                self.session_factory = MagicMock()
                self.close_async = AsyncMock(return_value=Ok(None))
                self.requeue_stale_claimed = _cancel_on_requeue
                self.mark_stale_tasks_as_failed = AsyncMock(return_value=Ok(0))
                self.app: Any = None

        monkeypatch.setattr(
            'horsies.core.brokers.postgres.PostgresBroker', _CancelBroker,
        )
        monkeypatch.setattr(
            'horsies.core.workflows.recovery.recover_stuck_workflows',
            AsyncMock(return_value=0),
        )

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.INFO, logger=self._LOGGER_NAME):
                await worker._reaper_loop()
        finally:
            _logger.propagate = False

        assert 'cancelled' in caplog.text.lower()

    # --- U-6h: temp broker close error → logged ---

    @pytest.mark.asyncio
    async def test_temp_broker_close_error_logged(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When temp broker close returns Err, it's logged."""
        close_err = Err(BrokerOperationError(
            code=BrokerErrorCode.CLOSE_FAILED,
            message='close failed',
            retryable=False,
        ))
        worker = _make_reaper_worker(
            monkeypatch,
            auto_requeue=True,
            requeue_results=[Ok(0)],
            broker_close_result=close_err,
        )

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger=self._LOGGER_NAME):
                await worker._reaper_loop()
        finally:
            _logger.propagate = False

        assert 'closing reaper broker' in caplog.text.lower()


# ---------------------------------------------------------------------------
# 18. _preload_modules_main — import orchestration
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPreloadModulesMain:
    """Tests for Worker._preload_modules_main."""

    _LOGGER_NAME = 'horsies.worker'

    def _patch_preload(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, MagicMock]:
        """Patch all external dependencies of _preload_modules_main.

        Returns a dict of mocks keyed by name.
        """
        mock_app = MagicMock()
        mock_app.get_discovered_task_modules.return_value = ['discovered.module']
        mock_app.list_tasks.return_value = ['task_a', 'task_b']

        mocks = {
            'locate_app': MagicMock(return_value=mock_app),
            'set_current_app': MagicMock(),
            'import_by_path': MagicMock(),
            'import_module': MagicMock(),
            'build_sys_path_roots': MagicMock(return_value=['/fake/root']),
            'app': mock_app,
        }

        monkeypatch.setattr(
            'horsies.core.worker.worker._locate_app', mocks['locate_app'],
        )
        monkeypatch.setattr(
            'horsies.core.worker.worker.set_current_app', mocks['set_current_app'],
        )
        monkeypatch.setattr(
            'horsies.core.worker.worker.import_by_path', mocks['import_by_path'],
        )
        monkeypatch.setattr(
            'horsies.core.worker.worker.import_module', mocks['import_module'],
        )
        monkeypatch.setattr(
            'horsies.core.worker.worker._build_sys_path_roots', mocks['build_sys_path_roots'],
        )
        return mocks

    # --- U-5a: locates app, sets current_app and self._app ---

    def test_locates_app_and_sets_current(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Calls _locate_app, set_current_app, and assigns self._app."""
        worker = _make_worker()
        mocks = self._patch_preload(monkeypatch)

        worker._preload_modules_main()

        mocks['locate_app'].assert_called_once_with(worker.cfg.app_locator)
        mocks['set_current_app'].assert_called_once_with(mocks['app'])
        assert worker._app is mocks['app']

    # --- U-5b: suppress_sends(True) before imports, False after ---

    def test_suppress_sends_bracket(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """suppress_sends(True) called before imports, suppress_sends(False) after."""
        worker = _make_worker()
        mocks = self._patch_preload(monkeypatch)
        call_order: list[str] = []
        mocks['app'].suppress_sends.side_effect = lambda v: call_order.append(
            f'suppress({v})',
        )
        mocks['import_module'].side_effect = lambda m: call_order.append(
            f'import({m})',
        )

        worker._preload_modules_main()

        assert call_order[0] == 'suppress(True)'
        assert call_order[-1] == 'suppress(False)'
        # Imports happen between suppress(True) and suppress(False)
        import_calls = [c for c in call_order if c.startswith('import(')]
        assert len(import_calls) > 0

    # --- U-5c: .py paths via import_by_path, dotted via import_module ---

    def test_py_file_uses_import_by_path_dotted_uses_import_module(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """.py paths routed to import_by_path, dotted names to import_module."""
        worker = _make_worker()
        worker.cfg.imports = ['tasks/my_tasks.py', 'myapp.tasks']
        mocks = self._patch_preload(monkeypatch)
        # Disable discovered modules to isolate cfg.imports
        mocks['app'].get_discovered_task_modules.return_value = []

        worker._preload_modules_main()

        # import_by_path called for the .py file
        mocks['import_by_path'].assert_called_once()
        path_arg = mocks['import_by_path'].call_args[0][0]
        assert path_arg.endswith('my_tasks.py')

        # import_module called for the dotted name
        mocks['import_module'].assert_called_once_with('myapp.tasks')

    # --- U-5d: includes app.get_discovered_task_modules() ---

    def test_includes_discovered_task_modules(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Discovered modules from app are included in imports."""
        worker = _make_worker()
        worker.cfg.imports = []
        mocks = self._patch_preload(monkeypatch)
        mocks['app'].get_discovered_task_modules.return_value = ['auto.discovered']

        worker._preload_modules_main()

        mocks['import_module'].assert_called_once_with('auto.discovered')

    # --- U-5e: import error → logged + re-raised ---

    def test_import_error_logged_and_reraised(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Import error is logged and re-raised to abort startup."""
        worker = _make_worker()
        worker.cfg.imports = ['bad.module']
        mocks = self._patch_preload(monkeypatch)
        mocks['app'].get_discovered_task_modules.return_value = []
        mocks['import_module'].side_effect = ImportError('no such module')

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger=self._LOGGER_NAME):
                with pytest.raises(ImportError, match='no such module'):
                    worker._preload_modules_main()
        finally:
            _logger.propagate = False

        assert 'failed during preload' in caplog.text.lower()

    # --- U-5f: suppress_sends exception → swallowed ---

    def test_suppress_sends_exception_stops_startup(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Exception in suppress_sends is fatal — importing without suppression
        could fire tasks accidentally, so startup must abort."""
        worker = _make_worker()
        mocks = self._patch_preload(monkeypatch)
        mocks['app'].suppress_sends.side_effect = RuntimeError('suppress broken')

        with pytest.raises(RuntimeError, match='suppress broken'):
            worker._preload_modules_main()

        mocks['locate_app'].assert_called_once()


# ---------------------------------------------------------------------------
# 19. Monitoring — psutil, worker state, heartbeat loops
# ---------------------------------------------------------------------------

from horsies.core.worker.worker import (
    _collect_psutil_metrics,
    INSERT_WORKER_STATE_SQL,
)


@pytest.mark.unit
class TestCollectPsutilMetrics:
    """Tests for _collect_psutil_metrics."""

    # --- U-7a ---

    def test_returns_three_floats(self) -> None:
        """Returns (rss_mb, mem_pct, cpu_pct) as floats."""
        result = _collect_psutil_metrics()

        assert isinstance(result, tuple)
        assert len(result) == 3
        rss_mb, mem_pct, cpu_pct = result
        assert isinstance(rss_mb, float)
        assert isinstance(mem_pct, float)
        assert isinstance(cpu_pct, float)
        assert rss_mb > 0  # process must use some memory


@pytest.mark.unit
class TestUpdateWorkerState:
    """Tests for Worker._update_worker_state."""

    _LOGGER_NAME = 'horsies.worker'

    # --- U-7b ---

    @pytest.mark.asyncio
    async def test_inserts_row_with_correct_params(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Executes INSERT_WORKER_STATE_SQL with expected parameters."""
        worker = _make_worker()
        worker._started_at = datetime(2025, 1, 1, tzinfo=timezone.utc)

        monkeypatch.setattr(
            'horsies.core.worker.worker._collect_psutil_metrics',
            lambda: (100.0, 5.5, 12.3),
        )
        worker._count_only_running_for_worker = AsyncMock(return_value=3)  # type: ignore[assignment]
        worker._count_claimed_for_worker = AsyncMock(return_value=7)  # type: ignore[assignment]

        executed_stmts: list[Any] = []
        executed_params: list[Any] = []

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.commit = AsyncMock()

        async def _capture_execute(stmt: Any, params: Any = None) -> MagicMock:
            executed_stmts.append(stmt)
            if params:
                executed_params.append(params)
            return MagicMock(rowcount=1)

        session.execute = _capture_execute
        worker.sf = MagicMock(return_value=session)

        await worker._update_worker_state()

        assert INSERT_WORKER_STATE_SQL in executed_stmts
        assert len(executed_params) == 1
        params = executed_params[0]
        assert params['wid'] == worker.worker_instance_id
        assert params['running'] == 3
        assert params['claimed'] == 7
        assert params['mem_mb'] == 100.0
        assert params['mem_pct'] == 5.5
        assert params['cpu_pct'] == 12.3

    # --- U-7c ---

    @pytest.mark.asyncio
    async def test_exception_logged_no_propagation(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Exception in _update_worker_state is logged, does not propagate."""
        worker = _make_worker()
        monkeypatch.setattr(
            'horsies.core.worker.worker._collect_psutil_metrics',
            MagicMock(side_effect=RuntimeError('psutil boom')),
        )

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger=self._LOGGER_NAME):
                await worker._update_worker_state()  # should not raise
        finally:
            _logger.propagate = False

        assert 'failed to update worker state' in caplog.text.lower()


@pytest.mark.unit
class TestWorkerStateHeartbeatLoop:
    """Tests for Worker._worker_state_heartbeat_loop."""

    # --- U-7d ---

    @pytest.mark.asyncio
    async def test_runs_update_and_respects_stop(self) -> None:
        """Calls _update_worker_state then exits when stop is set."""
        worker = _make_worker()
        update_count = 0

        async def _fake_update() -> None:
            nonlocal update_count
            update_count += 1
            worker._stop.set()  # stop after first update

        worker._update_worker_state = _fake_update  # type: ignore[assignment]

        await worker._worker_state_heartbeat_loop()

        assert update_count >= 1

    @pytest.mark.asyncio
    async def test_cancelled_error_clean_exit(self) -> None:
        """CancelledError in loop exits cleanly."""
        worker = _make_worker()

        async def _cancel_update() -> None:
            raise asyncio.CancelledError()

        worker._update_worker_state = _cancel_update  # type: ignore[assignment]

        # Should not raise
        await worker._worker_state_heartbeat_loop()


@pytest.mark.unit
class TestClaimerHeartbeatLoopErrorPaths:
    """Tests for _claimer_heartbeat_loop error/cancel paths (U-7e, U-7f)."""

    _LOGGER_NAME = 'horsies.worker'

    # --- U-7e ---

    @pytest.mark.asyncio
    async def test_error_logged_loop_continues(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Exception in heartbeat body is logged, loop continues."""
        worker = _make_worker()
        call_count = 0

        async def _execute(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError('db boom')
            # Second call: stop the worker
            worker._stop.set()
            return MagicMock(rowcount=0)

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(side_effect=_execute)
        session.commit = AsyncMock()
        worker.sf = MagicMock(return_value=session)

        # Use minimal sleep interval
        worker.cfg.recovery_config = RecoveryConfig(
            claimer_heartbeat_interval_ms=1_000,
        )

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger=self._LOGGER_NAME):
                await worker._claimer_heartbeat_loop()
        finally:
            _logger.propagate = False

        assert 'claimer heartbeat error' in caplog.text.lower()
        assert call_count >= 2  # continued after error

    # --- U-7f ---

    @pytest.mark.asyncio
    async def test_cancelled_error_clean_exit(self) -> None:
        """CancelledError in claimer heartbeat exits cleanly."""
        worker = _make_worker()

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(side_effect=asyncio.CancelledError())
        worker.sf = MagicMock(return_value=session)

        # Should not raise
        await worker._claimer_heartbeat_loop()


# ---------------------------------------------------------------------------
# 20. TestRunForever (U-8a – U-8d)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRunForever:
    """Tests for Worker.run_forever: main orchestration loop."""

    _LOGGER_NAME = 'horsies.worker'

    # --- U-8a ---

    @pytest.mark.asyncio
    async def test_calls_start_enters_loop_then_stop(self) -> None:
        """Resilience start called, loop body executed, stop exits cleanly."""
        worker = _make_worker()
        resilience_called = False

        async def _fake_start() -> None:
            nonlocal resilience_called
            resilience_called = True

        call_count = 0

        async def _fake_claim_and_dispatch() -> bool:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                worker._stop.set()
            return False

        async def _fake_wait(poll_interval_ms: int) -> None:
            return

        async def _fake_stop(**kwargs: object) -> None:
            return

        worker._start_with_resilience_config = _fake_start  # type: ignore[assignment]
        worker._claim_and_dispatch_all = _fake_claim_and_dispatch  # type: ignore[assignment]
        worker._wait_for_any_notify = _fake_wait  # type: ignore[assignment]
        worker.stop = _fake_stop  # type: ignore[assignment]

        await worker.run_forever()

        assert resilience_called
        assert call_count >= 2

    # --- U-8b ---

    @pytest.mark.asyncio
    async def test_retryable_error_backoff_and_retry(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Retryable connection error triggers backoff then retry."""
        worker = _make_worker()

        async def _fake_start() -> None:
            return

        call_count = 0

        async def _fake_claim_and_dispatch() -> bool:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OperationalError('connection lost')
            # Second pass: stop
            worker._stop.set()
            return False

        async def _fake_wait(poll_interval_ms: int) -> None:
            return

        async def _fake_stop(**kwargs: object) -> None:
            return

        worker._start_with_resilience_config = _fake_start  # type: ignore[assignment]
        worker._claim_and_dispatch_all = _fake_claim_and_dispatch  # type: ignore[assignment]
        worker._wait_for_any_notify = _fake_wait  # type: ignore[assignment]
        worker.stop = _fake_stop  # type: ignore[assignment]
        # Use a fast sleep to avoid test slowness
        worker._sleep_with_stop = AsyncMock()  # type: ignore[assignment]

        _logger = logging.getLogger(self._LOGGER_NAME)
        _logger.propagate = True
        try:
            with caplog.at_level(logging.ERROR, logger=self._LOGGER_NAME):
                await worker.run_forever()
        finally:
            _logger.propagate = False

        assert call_count >= 2  # retried after error
        assert 'retrying' in caplog.text.lower()

    # --- U-8c ---

    @pytest.mark.asyncio
    async def test_non_retryable_error_raises_and_stops(self) -> None:
        """Non-retryable error propagates; stop() called in finally."""
        worker = _make_worker()
        stop_called = False

        async def _fake_start() -> None:
            return

        async def _fake_claim_and_dispatch() -> bool:
            raise ValueError('non-retryable')

        async def _fake_stop(**kwargs: object) -> None:
            nonlocal stop_called
            stop_called = True

        worker._start_with_resilience_config = _fake_start  # type: ignore[assignment]
        worker._claim_and_dispatch_all = _fake_claim_and_dispatch  # type: ignore[assignment]
        worker.stop = _fake_stop  # type: ignore[assignment]

        with pytest.raises(ValueError, match='non-retryable'):
            await worker.run_forever()

        assert stop_called

    # --- U-8d ---

    @pytest.mark.asyncio
    async def test_stop_set_before_loop_exits_cleanly(self) -> None:
        """If stop is set during resilience start, loop never entered."""
        worker = _make_worker()
        loop_entered = False

        async def _fake_start() -> None:
            worker._stop.set()

        async def _fake_claim_and_dispatch() -> bool:
            nonlocal loop_entered
            loop_entered = True
            return False

        async def _fake_stop(**kwargs: object) -> None:
            return

        worker._start_with_resilience_config = _fake_start  # type: ignore[assignment]
        worker._claim_and_dispatch_all = _fake_claim_and_dispatch  # type: ignore[assignment]
        worker.stop = _fake_stop  # type: ignore[assignment]

        await worker.run_forever()

        assert not loop_entered


# ---------------------------------------------------------------------------
# 21. TestWaitForAnyNotify (U-8e – U-8g)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWaitForAnyNotify:
    """Tests for Worker._wait_for_any_notify: NOTIFY coalescing."""

    # --- U-8e ---

    @pytest.mark.asyncio
    async def test_stop_signal_cancels_pending_returns(self) -> None:
        """When stop is already set, cancels all queue tasks and returns."""
        worker = _make_worker()
        # Set up queue/global attributes as start() would
        q1: asyncio.Queue[object] = asyncio.Queue()
        worker._queues = [q1]
        worker._global = asyncio.Queue()
        worker._stop.set()

        # Should return immediately without hanging
        await worker._wait_for_any_notify(poll_interval_ms=5_000)

    # --- U-8f ---

    @pytest.mark.asyncio
    async def test_timeout_no_done_cancels_and_returns(self) -> None:
        """Timeout with no queue activity cancels pending and returns."""
        worker = _make_worker()
        q1: asyncio.Queue[object] = asyncio.Queue()
        worker._queues = [q1]
        worker._global = asyncio.Queue()

        # Very short timeout → nothing will resolve
        await worker._wait_for_any_notify(poll_interval_ms=1)

    # --- U-8g ---

    @pytest.mark.asyncio
    async def test_notification_received_drains_burst(self) -> None:
        """Notification arrives → drains queued items up to coalesce_notifies."""
        worker = _make_worker()
        worker.cfg.coalesce_notifies = 5

        q1: asyncio.Queue[object] = asyncio.Queue()
        # Pre-fill items: one will be consumed by asyncio.wait, rest by burst drain
        for i in range(4):
            q1.put_nowait(f'notify-{i}')

        worker._queues = [q1]
        worker._global = asyncio.Queue()

        await worker._wait_for_any_notify(poll_interval_ms=5_000)

        # All items should have been drained (1 by wait + up to 5 by drain)
        assert q1.empty()


# ---------------------------------------------------------------------------
# 22. TestClaimAndDispatchBranches (U-9a – U-9h)
# ---------------------------------------------------------------------------


def _make_claim_worker(
    *,
    queues: list[str] | None = None,
    prefetch_buffer: int = 0,
    max_claim_per_worker: int = 0,
    processes: int = 2,
    queue_priorities: dict[str, int] | None = None,
    queue_max_concurrency: dict[str, int] | None = None,
    cluster_wide_cap: int | None = None,
    max_claim_batch: int = 0,
) -> Worker:
    """Build a Worker pre-configured for claim-and-dispatch testing."""
    cfg = WorkerConfig(
        dsn="postgresql+psycopg://u:p@localhost/db",
        psycopg_dsn="postgresql://u:p@localhost/db",
        queues=queues or ["default"],
        prefetch_buffer=prefetch_buffer,
        max_claim_per_worker=max_claim_per_worker,
        processes=processes,
        queue_priorities=queue_priorities or {},
        queue_max_concurrency=queue_max_concurrency or {},
        cluster_wide_cap=cluster_wide_cap,
        max_claim_batch=max_claim_batch,
    )
    return Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=cfg,
    )


@pytest.mark.unit
class TestClaimAndDispatchBranches:
    """Tests for _claim_and_dispatch_all scattered branches (U-9)."""

    # --- U-9a ---

    @pytest.mark.asyncio
    async def test_softcap_mode_uses_running_count_only(self) -> None:
        """With prefetch_buffer > 0, calls _count_only_running (not in_flight)."""
        worker = _make_claim_worker(
            prefetch_buffer=2,
            processes=2,
        )
        # Mock _count_claimed: under the limit
        worker._count_claimed_for_worker = AsyncMock(return_value=0)  # type: ignore[assignment]
        # Mock _count_only_running (soft-cap path)
        worker._count_only_running_for_worker = AsyncMock(return_value=0)  # type: ignore[assignment]
        # Mock _count_in_flight (hard-cap path — should NOT be called)
        worker._count_in_flight_for_worker = AsyncMock(return_value=0)  # type: ignore[assignment]

        # Session mock for advisory lock + commit
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        # No rows returned by claim
        claim_result = MagicMock()
        claim_result.keys.return_value = ['id', 'task_name', 'args', 'kwargs']
        claim_result.fetchall.return_value = []
        session.execute = AsyncMock(return_value=claim_result)
        session.commit = AsyncMock()
        worker.sf = MagicMock(return_value=session)

        result = await worker._claim_and_dispatch_all()

        assert result is False
        worker._count_only_running_for_worker.assert_awaited_once()
        worker._count_in_flight_for_worker.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_softcap_budget_subtracts_existing_claimed_rows(self) -> None:
        """Soft-cap auto-claim must not exceed processes + prefetch_buffer."""
        worker = _make_claim_worker(
            prefetch_buffer=16,
            processes=8,
            max_claim_batch=0,
        )
        worker._count_claimed_for_worker = AsyncMock(return_value=16)  # type: ignore[assignment]
        worker._count_only_running_for_worker = AsyncMock(return_value=8)  # type: ignore[assignment]
        worker._count_in_flight_for_worker = AsyncMock(return_value=24)  # type: ignore[assignment]
        worker._claim_batch_locked = AsyncMock()  # type: ignore[assignment]

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(return_value=MagicMock())
        session.commit = AsyncMock()
        worker.sf = MagicMock(return_value=session)

        result = await worker._claim_and_dispatch_all()

        assert result is False
        worker._claim_batch_locked.assert_not_awaited()

    # --- U-9b ---

    @pytest.mark.asyncio
    async def test_max_claim_per_worker_guard_returns_false(self) -> None:
        """claimed_count >= max_claimed → returns False immediately."""
        worker = _make_claim_worker(max_claim_per_worker=3)
        # Already at the limit
        worker._count_claimed_for_worker = AsyncMock(return_value=3)  # type: ignore[assignment]

        result = await worker._claim_and_dispatch_all()

        assert result is False

    # --- U-9c ---

    @pytest.mark.asyncio
    async def test_queue_priority_sorting(self) -> None:
        """Custom queue_priorities → queues processed in priority order."""
        worker = _make_claim_worker(
            queues=["high", "low", "mid"],
            queue_priorities={"high": 1, "mid": 50, "low": 100},
            processes=10,
            max_claim_batch=10,
        )
        worker._count_claimed_for_worker = AsyncMock(return_value=0)  # type: ignore[assignment]
        worker._count_in_flight_for_worker = AsyncMock(return_value=0)  # type: ignore[assignment]

        claimed_queues: list[str] = []

        async def _fake_claim_batch(s: object, qname: str, limit: int) -> list[dict[str, object]]:
            claimed_queues.append(qname)
            return []  # no rows, but tracks order

        worker._claim_batch_locked = _fake_claim_batch  # type: ignore[assignment]

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(return_value=MagicMock())
        session.commit = AsyncMock()
        worker.sf = MagicMock(return_value=session)

        await worker._claim_and_dispatch_all()

        assert claimed_queues == ["high", "mid", "low"]

    # --- U-9d ---

    @pytest.mark.asyncio
    async def test_global_inflight_row_none_defaults_to_zero(self) -> None:
        """cluster_wide_cap set, fetchone() returns None → in_flight_global = 0."""
        worker = _make_claim_worker(
            cluster_wide_cap=10,
            processes=4,
        )
        worker._count_claimed_for_worker = AsyncMock(return_value=0)  # type: ignore[assignment]
        worker._count_in_flight_for_worker = AsyncMock(return_value=0)  # type: ignore[assignment]

        call_count = 0

        async def _execute(stmt: object, params: object = None) -> MagicMock:
            nonlocal call_count
            call_count += 1
            result = MagicMock()
            if call_count == 1:
                # Advisory lock — no return needed
                return result
            if call_count == 2:
                # COUNT_GLOBAL_IN_FLIGHT_SQL → None row
                result.fetchone.return_value = None
                return result
            # Claim SQL — no rows
            result.keys.return_value = ['id', 'task_name', 'args', 'kwargs']
            result.fetchall.return_value = []
            return result

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(side_effect=_execute)
        session.commit = AsyncMock()
        worker.sf = MagicMock(return_value=session)

        result = await worker._claim_and_dispatch_all()

        # Should not crash from None row — defaults to 0, continues
        assert result is False

    # --- U-9e ---

    @pytest.mark.asyncio
    async def test_queue_concurrency_soft_vs_hard_sql(self) -> None:
        """Verifies correct SQL is selected based on hard/soft cap mode."""
        for prefetch, expected_sql in [
            (0, COUNT_QUEUE_IN_FLIGHT_HARD_SQL),
            (2, COUNT_QUEUE_IN_FLIGHT_SOFT_SQL),
        ]:
            worker = _make_claim_worker(
                queues=["q1"],
                prefetch_buffer=prefetch,
                processes=4,
                queue_priorities={"q1": 1},
                queue_max_concurrency={"q1": 10},
            )
            worker._count_claimed_for_worker = AsyncMock(return_value=0)  # type: ignore[assignment]
            if prefetch == 0:
                worker._count_in_flight_for_worker = AsyncMock(return_value=0)  # type: ignore[assignment]
            else:
                worker._count_only_running_for_worker = AsyncMock(return_value=0)  # type: ignore[assignment]

            executed_stmts: list[object] = []

            async def _execute(stmt: object, params: object = None) -> MagicMock:
                executed_stmts.append(stmt)
                result = MagicMock()
                result.fetchone.return_value = SimpleNamespace(cnt=0)
                result.keys.return_value = ['id', 'task_name', 'args', 'kwargs']
                result.fetchall.return_value = []
                return result

            session = AsyncMock()
            session.__aenter__ = AsyncMock(return_value=session)
            session.__aexit__ = AsyncMock(return_value=None)
            session.execute = AsyncMock(side_effect=_execute)
            session.commit = AsyncMock()
            worker.sf = MagicMock(return_value=session)

            await worker._claim_and_dispatch_all()

            assert expected_sql in executed_stmts, (
                f'Expected {expected_sql!r} for prefetch={prefetch}, '
                f'got stmts: {executed_stmts}'
            )

    # --- U-9f ---

    @pytest.mark.asyncio
    async def test_budget_exhaustion_mid_queue_stops_claiming(self) -> None:
        """total_remaining reaches 0 mid-iteration → breaks queue loop."""
        worker = _make_claim_worker(
            queues=["q1", "q2"],
            processes=1,  # budget = 1
        )
        worker._count_claimed_for_worker = AsyncMock(return_value=0)  # type: ignore[assignment]
        worker._count_in_flight_for_worker = AsyncMock(return_value=0)  # type: ignore[assignment]

        claimed_queues: list[str] = []

        async def _fake_claim_batch(s: object, qname: str, limit: int) -> list[dict[str, object]]:
            claimed_queues.append(qname)
            # First queue claims 1 task (exhausting budget)
            if qname == "q1":
                return [{'id': 'task-1', 'task_name': 'fn', 'args': None, 'kwargs': None}]
            return []

        worker._claim_batch_locked = _fake_claim_batch  # type: ignore[assignment]
        worker._filter_nonrunnable_workflow_tasks = AsyncMock(  # type: ignore[assignment]
            side_effect=lambda rows: rows,
        )
        worker._dispatch_one = AsyncMock()  # type: ignore[assignment]

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(return_value=MagicMock())
        session.commit = AsyncMock()
        worker.sf = MagicMock(return_value=session)

        result = await worker._claim_and_dispatch_all()

        assert result is True
        # q2 should NOT have been claimed because budget was exhausted
        assert claimed_queues == ["q1"]

    # --- U-9g ---

    @pytest.mark.asyncio
    async def test_count_only_running_for_worker_row_none(self) -> None:
        """fetchone() returns None → returns 0."""
        worker = _make_worker()

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        result_mock = MagicMock()
        result_mock.fetchone.return_value = None
        session.execute = AsyncMock(return_value=result_mock)
        worker.sf = MagicMock(return_value=session)

        count = await worker._count_only_running_for_worker()

        assert count == 0

    # --- U-9h ---

    @pytest.mark.asyncio
    async def test_count_running_in_queue_row_none(self) -> None:
        """fetchone() returns None → returns 0."""
        worker = _make_worker()

        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        result_mock = MagicMock()
        result_mock.fetchone.return_value = None
        session.execute = AsyncMock(return_value=result_mock)
        worker.sf = MagicMock(return_value=session)

        count = await worker._count_running_in_queue('some_queue')

        assert count == 0


# ---------------------------------------------------------------------------
# Ping responder: _handle_ping
# ---------------------------------------------------------------------------


def _ping_session(worker: Worker) -> AsyncMock:
    """Attach a mock session to the worker and return it for assertions."""
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    worker.sf = MagicMock(return_value=session)
    return session


def _encode_ping(
    *,
    reply_channel: str,
    target_worker_id: str | None,
    correlation_id: str = 'corr-1',
) -> str:
    from horsies.core.codec.json_io import dumps_json
    from horsies.core.models.health import WorkerPingRequest

    req = WorkerPingRequest(
        correlation_id=correlation_id,
        reply_channel=reply_channel,
        target_worker_id=target_worker_id,
    )
    return dumps_json(req.model_dump()).ok_value


@pytest.mark.unit
class TestFailedStartCleanup:
    """A failed start must not leak background loops into a retry."""

    @pytest.mark.asyncio
    async def test_cleanup_cancels_service_tasks(self) -> None:
        """_cleanup_after_failed_start cancels spawned loops and clears the set."""
        worker = _make_worker()
        worker.listener.close = AsyncMock()

        async def _never() -> None:
            await asyncio.sleep(3600)

        task = worker._spawn_background(_never(), name='dummy-loop')
        assert worker._service_tasks  # spawned

        await worker._cleanup_after_failed_start()

        assert task.cancelled()
        assert worker._service_tasks == set()
        # _stop stays unset so the resilience loop is free to retry.
        assert not worker._stop.is_set()


@pytest.mark.unit
class TestHandlePing:
    """Worker replies on broadcast / matching target and stays silent otherwise."""

    @pytest.mark.asyncio
    async def test_broadcast_replies_with_pong(self) -> None:
        from horsies.core.codec.json_io import loads_json
        from horsies.core.models.health import WorkerPongPayload

        worker = _make_worker()
        session = _ping_session(worker)
        payload = _encode_ping(reply_channel='reply-ch', target_worker_id=None)

        await worker._handle_ping(payload)

        session.execute.assert_awaited_once()
        _, kwargs = session.execute.call_args
        params = kwargs if kwargs else session.execute.call_args.args[1]
        assert params['ch'] == 'reply-ch'
        pong = WorkerPongPayload.model_validate(loads_json(params['p']).ok_value)
        assert pong.correlation_id == 'corr-1'
        assert pong.worker_id == worker.worker_instance_id

    @pytest.mark.asyncio
    async def test_matching_target_replies(self) -> None:
        worker = _make_worker()
        session = _ping_session(worker)
        payload = _encode_ping(
            reply_channel='reply-ch', target_worker_id=worker.worker_instance_id
        )

        await worker._handle_ping(payload)

        session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_other_target_stays_silent(self) -> None:
        worker = _make_worker()
        session = _ping_session(worker)
        payload = _encode_ping(
            reply_channel='reply-ch', target_worker_id='someone-else'
        )

        await worker._handle_ping(payload)

        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_malformed_payload_is_dropped(self) -> None:
        worker = _make_worker()
        session = _ping_session(worker)

        await worker._handle_ping('not json')
        await worker._handle_ping('{"correlation_id": "c"}')  # missing reply_channel

        session.execute.assert_not_awaited()
