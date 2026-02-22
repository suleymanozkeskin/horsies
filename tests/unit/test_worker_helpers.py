"""Tests for pure-logic functions in horsies.core.worker.worker."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from psycopg import InterfaceError, OperationalError

from horsies.core.types.result import Ok, Err
from psycopg.errors import DeadlockDetected, SerializationFailure

from horsies.core.defaults import DEFAULT_CLAIM_LEASE_MS
from horsies.core.worker.worker import (
    Worker,
    WorkerConfig,
    CLAIM_SQL,
    DELETE_EXPIRED_HEARTBEATS_SQL,
    DELETE_EXPIRED_TASKS_SQL,
    DELETE_EXPIRED_WORKER_STATES_SQL,
    DELETE_EXPIRED_WORKFLOWS_SQL,
    DELETE_EXPIRED_WORKFLOW_TASKS_SQL,
    INSERT_CLAIMER_HEARTBEAT_SQL,
    RENEW_CLAIM_LEASE_SQL,
    _build_sys_path_roots,
    _dedupe_paths,
    _derive_sys_path_roots_from_file,
    _is_retryable_db_error,
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
    """Tests for Worker._advisory_key_global: deterministic 64-bit hash."""

    def test_returns_int(self) -> None:
        w = _make_worker()
        assert isinstance(w._advisory_key_global(), int)

    def test_deterministic(self) -> None:
        w = _make_worker()
        assert w._advisory_key_global() == w._advisory_key_global()

    def test_different_dsns_produce_different_keys(self) -> None:
        w1 = _make_worker(psycopg_dsn="postgresql://a@host/db1")
        w2 = _make_worker(psycopg_dsn="postgresql://b@host/db2")
        assert w1._advisory_key_global() != w2._advisory_key_global()

    def test_falls_back_to_dsn_when_psycopg_dsn_empty(self) -> None:
        w = _make_worker(psycopg_dsn="", dsn="postgresql+psycopg://u:p@h/mydb")
        result = w._advisory_key_global()
        assert isinstance(result, int)


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
            def __init__(self, config: Any):
                self.config = config
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
        created_brokers[0].close_async.assert_awaited_once()

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
            def __init__(self, config: Any):
                self.config = config
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
    async def test_claim_passes_claim_expires_at(self) -> None:
        """claim_expires_at is always a datetime (never None)."""
        worker = _make_worker(claim_lease_ms=None)

        fake_result = MagicMock()
        fake_result.keys.return_value = ['id', 'task_name', 'args', 'kwargs']
        fake_result.fetchall.return_value = []

        session = AsyncMock()
        session.execute = AsyncMock(return_value=fake_result)

        await worker._claim_batch_locked(session, 'default', 1)

        call_args = session.execute.call_args
        params = call_args[0][1]
        assert isinstance(params['claim_expires_at'], datetime)


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

        # Verify renewal params contain a datetime (not None)
        renewal_calls = [
            c for c in session.execute.call_args_list
            if c[0][0] is RENEW_CLAIM_LEASE_SQL
        ]
        assert len(renewal_calls) >= 1
        renewal_params = renewal_calls[0][0][1]
        assert isinstance(renewal_params['new_expires_at'], datetime)
