"""Unit tests for child_runner.py — full coverage.

Sections:
A. _serialization_error_response
B. _debug_imports_log
C. _child_initializer
D. _heartbeat_worker
E. _get_workflow_status_for_task
F. _handle_workflow_stop_before_start (wildcard branch)
G. _update_workflow_task_running_with_retry
I. _run_task_entry error paths
J. Workflow injection deserialization errors in _run_task_entry
"""

from __future__ import annotations

import json
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator
from unittest.mock import MagicMock, patch, call

import pytest

from horsies.core.codec.json_io import SerializationError
from horsies.core.models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import Err, Ok
from horsies.core.worker.child_runner import (
    _debug_imports_log,
    _get_workflow_status_for_task,
    _handle_workflow_stop_before_start,
    _heartbeat_worker,
    _is_retryable_db_error,
    _run_task_entry,
    _serialization_error_response,
    _update_workflow_task_running_with_retry,
)


# ---------------------------------------------------------------------------
# Shared fakes
# ---------------------------------------------------------------------------


class _FakeCursor:
    """Minimal cursor mock supporting execute / fetchone."""

    def __init__(self, fetchone_return: Any = None) -> None:
        self._fetchone_return = fetchone_return
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        self.queries.append((sql, params))

    def fetchone(self) -> Any:
        return self._fetchone_return

    def close(self) -> None:
        pass


class _FakeConn:
    """Minimal connection mock tracking commit / rollback."""

    def __init__(self, cursor: _FakeCursor | None = None) -> None:
        self._cursor = cursor or _FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, **kwargs: Any) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _FakePool:
    """Minimal pool mock yielding a _FakeConn via context manager."""

    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    @contextmanager
    def connection(self) -> Generator[_FakeConn, None, None]:
        yield self._conn


class _FakeRow:
    """Minimal named-tuple-like row returned by fetchone."""

    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


# ---------------------------------------------------------------------------
# Helper to parse a TaskResult from serialized JSON produced by child_runner
# ---------------------------------------------------------------------------


def _parse_task_result(json_str: str) -> dict[str, Any]:
    """Parse a serialized TaskResult JSON into a dict for assertions."""
    return json.loads(json_str)  # type: ignore[no-any-return]


# ===================================================================
# A. _serialization_error_response
# ===================================================================


@pytest.mark.unit
class TestSerializationErrorResponse:
    """_serialization_error_response builds correct error tuples."""

    def test_returns_true_ok_with_error_payload(self) -> None:
        error = SerializationError('bad json input')
        ok, payload, reason = _serialization_error_response('my_task', error)

        assert ok is True
        assert reason == 'SerializationError: bad json input'

        parsed = _parse_task_result(payload)
        err_data = parsed.get('err') or parsed.get('error')
        assert err_data is not None
        assert err_data['error_code'] == {'__builtin_task_code__': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value}

    def test_includes_task_name_in_data(self) -> None:
        error = SerializationError('corrupt')
        _, payload, _ = _serialization_error_response('process_order', error)

        parsed = _parse_task_result(payload)
        err_data = parsed.get('err') or parsed.get('error')
        assert err_data['data']['task_name'] == 'process_order'


# ===================================================================
# B. _debug_imports_log
# ===================================================================


@pytest.mark.unit
class TestDebugImportsLog:
    """_debug_imports_log only logs when HORSIES_DEBUG_IMPORTS=1."""

    def test_logs_when_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv('HORSIES_DEBUG_IMPORTS', '1')
        with patch('horsies.core.worker.child_runner.logger') as mock_logger:
            _debug_imports_log('test message')
            mock_logger.debug.assert_called_once_with('test message')

    def test_no_log_when_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv('HORSIES_DEBUG_IMPORTS', raising=False)
        with patch('horsies.core.worker.child_runner.logger') as mock_logger:
            _debug_imports_log('test message')
            mock_logger.debug.assert_not_called()

    def test_no_log_when_env_is_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv('HORSIES_DEBUG_IMPORTS', '0')
        with patch('horsies.core.worker.child_runner.logger') as mock_logger:
            _debug_imports_log('test message')
            mock_logger.debug.assert_not_called()


# ===================================================================
# C. _child_initializer
# ===================================================================


@pytest.mark.unit
class TestChildInitializer:
    """_child_initializer wires up the child process environment."""

    def _make_patches(
        self,
        *,
        app_tasks: dict[str, Any] | None = None,
        discovered_modules: list[str] | None = None,
        suppress_raises: bool = False,
        discovered_raises: bool = False,
        keys_list_raises: bool = False,
    ) -> dict[str, Any]:
        """Build a dict of mock objects for all _child_initializer dependencies."""
        mock_app = MagicMock()
        if app_tasks is not None:
            mock_app.tasks.keys_list.return_value = list(app_tasks.keys())
            mock_app.tasks.keys.return_value = app_tasks.keys()
        else:
            mock_app.tasks.keys_list.return_value = []
            mock_app.tasks.keys.return_value = {}.keys()

        if discovered_modules is not None:
            mock_app.get_discovered_task_modules.return_value = discovered_modules
        else:
            mock_app.get_discovered_task_modules.return_value = []

        if suppress_raises:
            mock_app.suppress_sends.side_effect = RuntimeError('suppress failed')
        if discovered_raises:
            mock_app.get_discovered_task_modules.side_effect = RuntimeError('discovery failed')
        if keys_list_raises:
            mock_app.tasks.keys_list.side_effect = RuntimeError('keys_list failed')

        return {'app': mock_app}

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_module')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.signal.signal')
    @patch('horsies.core.logging.configure_logging')
    def test_happy_path(
        self,
        mock_set_level: MagicMock,
        mock_signal: MagicMock,
        mock_import_path: MagicMock,
        mock_import_module: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_pool: MagicMock,
    ) -> None:
        from horsies.core.worker.child_runner import _child_initializer

        mocks = self._make_patches(app_tasks={'add': True})
        mock_locate_app.return_value = mocks['app']

        _child_initializer(
            app_locator='mymod:app',
            imports=['extra_mod'],
            sys_path_roots=[],
            loglevel=20,
            database_url='postgresql://localhost/test',
        )

        mock_signal.assert_called_once()
        mock_set_level.assert_called_once_with(20)
        mock_locate_app.assert_called_once_with('mymod:app')
        mock_set_current.assert_called_once_with(mocks['app'])
        mock_import_module.assert_any_call('extra_mod')
        mock_init_pool.assert_called_once_with(
            'postgresql://localhost/test',
            connect_kwargs=None,
            min_size=0,
            max_size=2,
            check_on_checkout=True,
        )

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.signal.signal')
    @patch('horsies.core.logging.configure_logging')
    def test_child_pool_bounds_are_forwarded(
        self,
        mock_set_level: MagicMock,
        mock_signal: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_pool: MagicMock,
    ) -> None:
        from horsies.core.worker.child_runner import _child_initializer

        mocks = self._make_patches(app_tasks={'add': True})
        mock_locate_app.return_value = mocks['app']

        _child_initializer(
            app_locator='mymod:app',
            imports=[],
            sys_path_roots=[],
            loglevel=20,
            database_url='postgresql://localhost/test',
            connect_kwargs={'keepalives': 1, 'prepare_threshold': None},
            child_pool_min_size=0,
            child_pool_max_size=1,
            child_pool_check=False,
        )

        mock_init_pool.assert_called_once_with(
            'postgresql://localhost/test',
            connect_kwargs={'keepalives': 1, 'prepare_threshold': None},
            min_size=0,
            max_size=1,
            check_on_checkout=False,
        )

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.signal.signal')
    @patch('horsies.core.logging.configure_logging')
    def test_child_initializer_discards_inherited_broker(
        self,
        mock_set_level: MagicMock,
        mock_signal: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_pool: MagicMock,
    ) -> None:
        from horsies.core.worker.child_runner import _child_initializer

        mocks = self._make_patches(app_tasks={'add': True})
        inherited_broker = MagicMock()
        inherited_broker.async_engine.sync_engine.dispose = MagicMock()
        mocks['app']._broker = inherited_broker
        mock_locate_app.return_value = mocks['app']

        _child_initializer(
            app_locator='mymod:app',
            imports=[],
            sys_path_roots=[],
            loglevel=20,
            database_url='postgresql://localhost/test',
        )

        inherited_broker.async_engine.sync_engine.dispose.assert_called_once_with(
            close=False
        )
        assert mocks['app']._broker is None
        mocks['app'].set_role.assert_called_once_with('worker')

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.signal.signal')
    @patch('horsies.core.logging.configure_logging')
    def test_suppress_sends_failure_propagates(
        self,
        mock_set_level: MagicMock,
        mock_signal: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_pool: MagicMock,
    ) -> None:
        from horsies.core.worker.child_runner import _child_initializer

        mocks = self._make_patches(suppress_raises=True)
        mock_locate_app.return_value = mocks['app']

        with pytest.raises(RuntimeError, match='suppress failed'):
            _child_initializer(
                app_locator='mymod:app',
                imports=[],
                sys_path_roots=[],
                loglevel=20,
                database_url='postgresql://localhost/test',
            )
        mock_init_pool.assert_not_called()

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.signal.signal')
    @patch('horsies.core.logging.configure_logging')
    def test_discovered_task_modules_failure_propagates(
        self,
        mock_set_level: MagicMock,
        mock_signal: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_pool: MagicMock,
    ) -> None:
        from horsies.core.worker.child_runner import _child_initializer

        mocks = self._make_patches(discovered_raises=True)
        mock_locate_app.return_value = mocks['app']

        with pytest.raises(RuntimeError, match='discovery failed'):
            _child_initializer(
                app_locator='mymod:app',
                imports=[],
                sys_path_roots=[],
                loglevel=20,
                database_url='postgresql://localhost/test',
            )
        mock_init_pool.assert_not_called()

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.signal.signal')
    @patch('horsies.core.logging.configure_logging')
    def test_keys_list_failure_propagates(
        self,
        mock_set_level: MagicMock,
        mock_signal: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_pool: MagicMock,
    ) -> None:
        from horsies.core.worker.child_runner import _child_initializer

        mocks = self._make_patches(keys_list_raises=True, app_tasks={'t1': 1})
        mock_locate_app.return_value = mocks['app']

        with pytest.raises(RuntimeError, match='keys_list failed'):
            _child_initializer(
                app_locator='mymod:app',
                imports=[],
                sys_path_roots=[],
                loglevel=20,
                database_url='postgresql://localhost/test',
            )

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.import_module')
    @patch('horsies.core.worker.child_runner.signal.signal')
    @patch('horsies.core.logging.configure_logging')
    @patch('horsies.core.worker.child_runner.os.path.samefile', return_value=False)
    def test_file_path_import_uses_import_by_path(
        self,
        mock_samefile: MagicMock,
        mock_set_level: MagicMock,
        mock_signal: MagicMock,
        mock_import_module: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_pool: MagicMock,
    ) -> None:
        from horsies.core.worker.child_runner import _child_initializer

        mocks = self._make_patches()
        mock_locate_app.return_value = mocks['app']

        _child_initializer(
            app_locator='/tmp/myapp.py:app',
            imports=['/tmp/tasks.py'],
            sys_path_roots=[],
            loglevel=20,
            database_url='postgresql://localhost/test',
        )
        # /tmp/tasks.py should be imported via import_by_path, not import_module
        assert mock_import_path.called
        # samefile was checked to avoid re-importing the app module
        mock_samefile.assert_called()

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.import_module')
    @patch('horsies.core.worker.child_runner.signal.signal')
    @patch('horsies.core.logging.configure_logging')
    @patch('horsies.core.worker.child_runner.os.path.samefile', return_value=True)
    def test_file_path_import_skips_app_module(
        self,
        mock_samefile: MagicMock,
        mock_set_level: MagicMock,
        mock_signal: MagicMock,
        mock_import_module: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_pool: MagicMock,
    ) -> None:
        """When import file matches app file, import_by_path is NOT called."""
        from horsies.core.worker.child_runner import _child_initializer

        mocks = self._make_patches()
        mock_locate_app.return_value = mocks['app']

        _child_initializer(
            app_locator='/tmp/myapp.py:app',
            imports=['/tmp/myapp.py'],
            sys_path_roots=[],
            loglevel=20,
            database_url='postgresql://localhost/test',
        )
        # import_by_path should NOT be called because samefile returns True
        mock_import_path.assert_not_called()


# ===================================================================
# C2. _initialize_worker_pool connect kwargs
# ===================================================================


@pytest.mark.unit
class TestInitializeWorkerPoolConnectKwargs:
    """connect_kwargs (TCP keepalives, PgBouncer knob) reach the pool.

    Regression for idle child-pool connections reaped mid-query (GH #100).
    """

    @contextmanager
    def _reset_pool(self) -> Generator[None, None, None]:
        import horsies.core.worker.child_pool as child_pool

        child_pool._worker_pool = None
        try:
            yield
        finally:
            child_pool._worker_pool = None

    def test_connect_kwargs_forwarded_to_pool(self) -> None:
        from horsies.core.worker.child_pool import _initialize_worker_pool

        connect_kwargs = {
            'keepalives': 1,
            'keepalives_idle': 30,
            'prepare_threshold': None,
        }
        with self._reset_pool(), patch(
            'horsies.core.worker.child_pool.ConnectionPool'
        ) as mock_pool, patch(
            'horsies.core.worker.child_pool.atexit.register'
        ):
            _initialize_worker_pool(
                'postgresql://localhost/test',
                connect_kwargs=connect_kwargs,
            )

        assert mock_pool.call_args.kwargs['kwargs'] == connect_kwargs

    def test_no_connect_kwargs_yields_empty_pool_kwargs(self) -> None:
        from horsies.core.worker.child_pool import _initialize_worker_pool

        with self._reset_pool(), patch(
            'horsies.core.worker.child_pool.ConnectionPool'
        ) as mock_pool, patch(
            'horsies.core.worker.child_pool.atexit.register'
        ):
            _initialize_worker_pool('postgresql://localhost/test')

        assert mock_pool.call_args.kwargs['kwargs'] == {}


# ===================================================================
# D. _heartbeat_worker
# ===================================================================


@pytest.mark.unit
class TestHeartbeatWorker:
    """_heartbeat_worker loop and failure paths."""

    def test_stops_on_event_set(self) -> None:
        """Heartbeat thread exits without beating when stop_event is set.

        The first heartbeat is inserted by the RUNNING transition
        transaction, not by this thread, so a pre-stopped loop sends
        nothing.
        """
        cursor = _FakeCursor()
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)
        stop = threading.Event()

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ):
            stop.set()
            _heartbeat_worker(
                task_id='t-1',
                database_url='unused',
                stop_event=stop,
                sender_worker_id='w-1',
                heartbeat_interval_ms=100,
            )

        assert cursor.queries == []

    def test_continues_after_send_failure(self) -> None:
        """Heartbeat thread keeps trying after a transient send failure."""
        stop = threading.Event()

        call_count = 0

        def _failing_pool() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call succeeds (initial heartbeat)
                cursor = _FakeCursor()
                conn = _FakeConn(cursor)
                return _FakePool(conn)
            if call_count == 2:
                raise RuntimeError('db down')
            stop.set()
            cursor = _FakeCursor()
            conn = _FakeConn(cursor)
            return _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            side_effect=_failing_pool,
        ):
            _heartbeat_worker(
                task_id='t-1',
                database_url='unused',
                stop_event=stop,
                sender_worker_id='w-1',
                heartbeat_interval_ms=50,  # Short interval for fast test
            )

        assert call_count >= 3


# ===================================================================
# E. _get_workflow_status_for_task
# ===================================================================


@pytest.mark.unit
class TestGetWorkflowStatusForTask:
    """_get_workflow_status_for_task edge cases."""

    def test_row_is_none_returns_none(self) -> None:
        cursor = _FakeCursor(fetchone_return=None)
        result = _get_workflow_status_for_task(cursor, 'task-1')  # type: ignore[arg-type]
        assert result is None

    def test_status_is_string_returns_it(self) -> None:
        row = _FakeRow(status='RUNNING')
        cursor = _FakeCursor(fetchone_return=row)
        result = _get_workflow_status_for_task(cursor, 'task-1')  # type: ignore[arg-type]
        assert result == 'RUNNING'

    def test_status_not_string_returns_none(self) -> None:
        row = _FakeRow(status=42)
        cursor = _FakeCursor(fetchone_return=row)
        result = _get_workflow_status_for_task(cursor, 'task-1')  # type: ignore[arg-type]
        assert result is None


# ===================================================================
# F. _handle_workflow_stop_before_start — wildcard + existing
# ===================================================================


@pytest.mark.unit
class TestHandleWorkflowStopBeforeStart:
    """_handle_workflow_stop_before_start for all status branches."""

    def test_cancelled_marks_terminal(self) -> None:
        from horsies.core.lifecycle.commands import CancelOwnedNode
        from horsies.core.lifecycle.outcomes import Applied

        cursor = _FakeCursor()
        conn = _FakeConn(cursor)

        def _after_node_update(*_args: Any) -> MagicMock:
            assert len(cursor.queries) == 1
            assert "SET status = 'SKIPPED'" in cursor.queries[0][0]
            return MagicMock(spec=Applied)

        with patch(
            'horsies.core.worker.child_runner.apply_sync',
            side_effect=_after_node_update,
        ) as apply:
            result = _handle_workflow_stop_before_start(
                cursor, conn, 'task-1', 'CANCELLED', 'worker-1',  # type: ignore[arg-type]
            )
        assert result == (False, '', 'WORKFLOW_STOPPED')
        assert conn.commits == 1
        sql_blob = '\n'.join(q[0] for q in cursor.queries)
        assert "SET status = 'SKIPPED'" in sql_blob
        command = apply.call_args.args[1]
        assert isinstance(command, CancelOwnedNode)
        assert command.task_id == 'task-1'
        assert command.accepts_requeued_pending is True

    def test_paused_cancels_claimed_task_and_resets_node(self) -> None:
        from horsies.core.lifecycle.commands import AbandonOwnedNode
        from horsies.core.lifecycle.outcomes import Applied

        cursor = _FakeCursor()
        conn = _FakeConn(cursor)

        def _before_node_update(*_args: Any) -> MagicMock:
            assert cursor.queries == []
            return MagicMock(spec=Applied)

        with patch(
            'horsies.core.worker.child_runner.apply_sync',
            side_effect=_before_node_update,
        ) as apply:
            result = _handle_workflow_stop_before_start(
                cursor, conn, 'task-2', 'PAUSED', 'worker-1',  # type: ignore[arg-type]
            )
        assert result == (False, '', 'WORKFLOW_STOPPED')
        assert conn.commits == 1
        sql_blob = '\n'.join(q[0] for q in cursor.queries)
        assert "SET status = 'READY'" in sql_blob
        assert "status IN ('ENQUEUED', 'RUNNING')" in sql_blob
        command = apply.call_args.args[1]
        assert isinstance(command, AbandonOwnedNode)
        assert command.task_id == 'task-2'

    def test_unknown_status_returns_workflow_check_failed(self) -> None:
        cursor = _FakeCursor()
        conn = _FakeConn(cursor)
        result = _handle_workflow_stop_before_start(
            cursor, conn, 'task-3', 'UNKNOWN_STATUS', 'worker-1',  # type: ignore[arg-type]
        )
        assert result == (False, '', 'WORKFLOW_CHECK_FAILED')
        # No SQL should have been executed for the unknown branch
        assert len(cursor.queries) == 0
        assert conn.commits == 0

    def test_both_branches_carry_the_claim_generation_fence(self) -> None:
        """Claimed pre-start cancels fence on (worker_id, claimed_at).

        Without the pair, a row requeued and re-claimed since this child was
        handed its dispatch is still CLAIMED and would be terminalized out
        from under the new claim generation.
        """
        generation = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
        for status in ('PAUSED', 'CANCELLED'):
            cursor = _FakeCursor()
            conn = _FakeConn(cursor)
            from horsies.core.lifecycle.outcomes import Applied

            with patch(
                'horsies.core.worker.child_runner.apply_sync',
                return_value=MagicMock(spec=Applied),
            ) as apply:
                _handle_workflow_stop_before_start(
                    cursor, conn, 'task-4', status, 'worker-7',  # type: ignore[arg-type]
                    generation,
                )
            command = apply.call_args.args[1]
            assert command.fence.worker_id == 'worker-7', status
            assert command.fence.claimed_at == generation, status

    def test_cancelled_branch_still_cancels_a_requeued_pending_row(self) -> None:
        """PENDING has no claim, so the generation fence must not gate it.

        A row the reaper requeued between dispatch and this check carries no
        claimed_by_worker_id. Gating it on the worker would silently stop
        cancelling tasks whose workflow is already CANCELLED.
        """
        from horsies.core.lifecycle.commands import CancelOwnedNode
        from horsies.core.lifecycle.outcomes import Applied

        cursor = _FakeCursor()
        conn = _FakeConn(cursor)
        with patch(
            'horsies.core.worker.child_runner.apply_sync',
            return_value=MagicMock(spec=Applied),
        ) as apply:
            _handle_workflow_stop_before_start(
                cursor, conn, 'task-5', 'CANCELLED', 'worker-7',  # type: ignore[arg-type]
            )
        command = apply.call_args.args[1]
        assert isinstance(command, CancelOwnedNode)
        assert command.accepts_requeued_pending is True


# ===================================================================
# G. _update_workflow_task_running_with_retry
# ===================================================================


@pytest.mark.unit
class TestUpdateWorkflowTaskRunningWithRetry:
    """Retry logic for _update_workflow_task_running_with_retry."""

    def test_success_on_first_attempt(self) -> None:
        cursor = _FakeCursor()
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ):
            result = _update_workflow_task_running_with_retry('task-1')

        assert result is True
        assert conn.commits == 1
        assert len(cursor.queries) == 1

    def test_retryable_error_retries_then_succeeds(self) -> None:
        """OperationalError on first call, success on second."""
        from psycopg import OperationalError

        cursor = _FakeCursor()
        conn = _FakeConn(cursor)
        pool_ok = _FakePool(conn)

        call_count = 0

        def _pool_factory() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OperationalError('transient')
            return pool_ok

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            side_effect=_pool_factory,
        ), patch('horsies.core.worker.child_runner.time.sleep'):
            result = _update_workflow_task_running_with_retry('task-1')

        assert result is True
        assert call_count == 2
        assert conn.commits == 1

    def test_retryable_error_exhausts_all_attempts(self) -> None:
        """OperationalError on all attempts → returns False."""
        from psycopg import OperationalError

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            side_effect=OperationalError('permanent'),
        ), patch('horsies.core.worker.child_runner.time.sleep'):
            result = _update_workflow_task_running_with_retry('task-1')

        assert result is False

    def test_non_retryable_error_fails_immediately(self) -> None:
        """Non-retryable error (e.g. ValueError) → returns False without retry."""
        call_count = 0

        def _pool_factory() -> Any:
            nonlocal call_count
            call_count += 1
            raise ValueError('schema error')

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            side_effect=_pool_factory,
        ):
            result = _update_workflow_task_running_with_retry('task-1')

        assert result is False
        assert call_count == 1  # No retries for non-retryable

    def test_retryable_sleeps_with_backoff(self) -> None:
        """Verify sleep is called with increasing delays on retries."""
        from psycopg import OperationalError

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            side_effect=OperationalError('fail'),
        ), patch('horsies.core.worker.child_runner.time.sleep') as mock_sleep:
            _update_workflow_task_running_with_retry('task-1')

        # backoff_seconds = (0.0, 0.25, 0.75) — sleep only called when delay > 0
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(0.25)
        mock_sleep.assert_any_call(0.75)


# ===================================================================
# I. _run_task_entry error paths
# ===================================================================


def _make_run_task_patches(
    *,
    ownership: Any = None,
    task_fn: Any = None,
    task_missing: bool = False,
) -> dict[str, Any]:
    """Build common patches for _run_task_entry tests.

    Returns a dict of patch targets → values suitable for use with `patch`.
    """
    patches: dict[str, Any] = {
        'horsies.core.worker.child_runner._confirm_ownership_and_set_running': MagicMock(
            return_value=ownership,
        ),
        'horsies.core.worker.child_runner._start_heartbeat_thread': MagicMock(),
    }

    mock_app = MagicMock()
    if task_missing:
        mock_app.tasks.__getitem__ = MagicMock(
            side_effect=KeyError('no_such_task'),
        )
    elif task_fn is not None:
        mock_task = MagicMock()
        mock_task.__call__ = MagicMock(return_value=task_fn())
        mock_task._original_fn = task_fn
        mock_task._fn = task_fn
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)
    else:
        mock_task = MagicMock()
        mock_task.__call__ = MagicMock(return_value=TaskResult(ok='hello'))
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

    patches['horsies.core.worker.child_runner.get_current_app'] = MagicMock(
        return_value=mock_app,
    )
    return patches


@pytest.fixture()
def _run_entry_defaults() -> dict[str, Any]:
    """Default args for _run_task_entry."""
    return {
        'task_name': 'my_task',
        'args_json': '[]',
        'kwargs_json': '{}',
        'task_id': 'tid-1',
        'database_url': 'postgresql://localhost/test',
        'master_worker_id': 'w-1',
        'runner_heartbeat_interval_ms': 100,
    }


@pytest.mark.unit
class TestRunTaskEntryErrorPaths:
    """Error paths in _run_task_entry."""

    def test_ownership_blocks_execution(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """When ownership returns a tuple, _run_task_entry returns it."""
        sentinel = (False, '', 'CLAIM_LOST')
        patches = _make_run_task_patches(ownership=sentinel)

        with _apply_patches(patches):
            result = _run_task_entry(**_run_entry_defaults)

        assert result == sentinel

    def test_task_resolution_failure(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Unknown task name → WORKER_RESOLUTION_ERROR."""
        patches = _make_run_task_patches(task_missing=True)

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert 'KeyError' in (reason or '')
        parsed = _parse_task_result(payload)
        err = parsed.get('err') or parsed.get('error')
        assert err['error_code'] == {'__builtin_task_code__': OperationalErrorCode.WORKER_RESOLUTION_ERROR.value}

    def test_args_json_invalid(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Malformed args_json → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['args_json'] = '{{{invalid json'

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason
        parsed = _parse_task_result(payload)
        err = parsed.get('err') or parsed.get('error')
        assert err['error_code'] == {'__builtin_task_code__': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value}

    def test_args_json_not_a_list(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """args_json is valid JSON but not a list → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['args_json'] = '"a string"'

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason

    # test_positional_args_rejected_before_rehydration removed: the
    # legacy ``rehydrate_value`` decoder no longer exists (strict-serde
    # phase 7 deleted it). Positional args are still rejected at the
    # raw-JSON inspection layer (see ``child_runner`` args block); that
    # behaviour is exercised by ``test_positional_args_rejected`` in
    # this class.
        assert ok is True
        assert reason is not None and 'SerializationError' in reason
        parsed = _parse_task_result(payload)
        err = parsed.get('err') or parsed.get('error')
        assert (
            err['error_code']
            == {'__builtin_task_code__': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value}
        )
        msg = err.get('message') or ''
        assert 'positional' in msg.lower()

    def test_kwargs_json_invalid(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Malformed kwargs_json → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['kwargs_json'] = '{{{bad'

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason

    def test_kwargs_json_not_a_dict(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """kwargs_json is valid JSON but not a dict → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['kwargs_json'] = '[1, 2]'

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason

    def test_kwargs_json_null_treated_as_empty(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """kwargs_json=None (DB NULL) is accepted as empty kwargs.

        Regression: ``.send()`` leaves kwargs_json=None for no-kwargs
        calls (task_decorator.py emits NULL when ``kwargs_dict`` is
        falsy); the child runner must mirror the args-NULL handling and
        treat NULL kwargs_json as ``{}`` rather than rejecting it as
        WORKER_SERIALIZATION_ERROR. Without this branch every no-kwargs
        e2e task (healthcheck, return_none_task, etc.) fails at the
        worker, deadlocking ``_make_ready_check`` in run_worker.
        """
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_ok_type = int
        mock_task.return_value = 42
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)
        _run_entry_defaults['kwargs_json'] = None

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        parsed = _parse_task_result(payload)
        err = parsed.get('err') or parsed.get('error')
        assert err is None, (
            f'expected no error for NULL kwargs_json, got: {err}'
        )

    def test_task_returns_none(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Task returning None → TASK_EXCEPTION."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.return_value = None
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason == 'Task returned None'
        parsed = _parse_task_result(payload)
        err = parsed.get('err') or parsed.get('error')
        assert err['error_code'] == {'__builtin_task_code__': OperationalErrorCode.TASK_EXCEPTION.value}

    def test_task_raises_exception(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Task raising an exception → TASK_EXCEPTION with ok=True."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.side_effect = ValueError('boom')
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is None  # exception path sets reason=None
        parsed = _parse_task_result(payload)
        err = parsed.get('err') or parsed.get('error')
        assert err['error_code'] == {'__builtin_task_code__': OperationalErrorCode.TASK_EXCEPTION.value}
        assert 'ValueError' in err['message']
        assert 'boom' in err['message']

    def test_task_returns_plain_value_wrapped(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Task returning a plain value → wrapped into TaskResult(ok=value).

        Strict-serde phase 5: encoding routes through
        ``encode_task_result(out, task.task_ok_type)``, so the mock task
        must declare a real ``task_ok_type`` (pydantic-resolvable).
        """
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_ok_type = int
        mock_task.return_value = 42
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is None
        parsed = _parse_task_result(payload)
        # Phase 5 wire format: ``__h_task_result__`` envelope.
        assert parsed.get('__h_task_result__') is True
        assert parsed.get('ok') == 42

    def test_unserializable_value_surfaces_as_serialization_error(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Task returning unserializable value → WORKER_SERIALIZATION_ERROR.

        Regression: ``encode_task_result(out, ok_type)`` raises
        ``pydantic_core.PydanticSerializationError`` for inputs that
        don't fit the declared ``OkT`` (e.g. a function value carried
        in an ``int`` slot). ``PydanticSerializationError`` is a
        ``ValueError`` subclass — not a ``ValidationError`` — so the
        existing catch ``except (StrictJsonError, ValidationError)``
        did not cover it. The exception escaped and was wrapped as a
        generic ``TASK_EXCEPTION`` by an outer handler, masking the
        real cause (encoder mismatch). The fix adds
        ``PydanticSerializationError`` to both encode-time catch tuples.
        """
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_ok_type = int

        def _identity(x: Any) -> Any:
            return x

        # Task wraps the callable as ok inside a TaskResult — same
        # shape as ``unserializable_result_task`` in tests/e2e/tasks/basic.py.
        mock_task.return_value = TaskResult(ok=_identity)
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason
        parsed = _parse_task_result(payload)
        err = parsed.get('err') or parsed.get('error')
        assert err is not None
        assert err['error_code'] == {
            '__builtin_task_code__': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value,
        }

    def test_task_returns_task_result_ok(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Task returning TaskResult(ok=...) → serialized correctly."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.return_value = TaskResult(ok='success')
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is None

    def test_task_returns_task_result_err(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Task returning TaskResult(err=...) → serialized correctly, ok=True."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.return_value = TaskResult(
            err=TaskError(error_code='CUSTOM_ERR', message='failed'),
        )
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is None

    def test_dumps_json_failure_on_task_result(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """When dumps_json fails on TaskResult output → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_ok_type = str
        mock_task.return_value = TaskResult(ok='data')
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches), patch(
            'horsies.core.worker.child_runner.dumps_json',
            return_value=Err(SerializationError('cannot serialize')),
        ):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason

    def test_dumps_json_failure_on_plain_value(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """When dumps_json fails wrapping a plain value → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_ok_type = int
        mock_task.return_value = 42
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches), patch(
            'horsies.core.worker.child_runner.dumps_json',
            return_value=Err(SerializationError('cannot serialize')),
        ):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason


# ===================================================================
# J. Workflow injection deserialization errors
# ===================================================================


@pytest.mark.unit
class TestRunTaskEntryWorkflowInjectionErrors:
    """Workflow injection serde errors in _run_task_entry."""

    def test_horsies_taskresult_bad_data_json(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """__h_taskresult_envelope__ with invalid 'data' string → serde error."""
        patches = _make_run_task_patches()
        _run_entry_defaults['kwargs_json'] = json.dumps({
            'upstream': {
                '__h_taskresult_envelope__': True,
                'data': '{{{invalid',
            },
        })

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason

    def test_horsies_taskresult_not_a_task_result_json(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """__h_taskresult_envelope__ with valid JSON but not TaskResult shape → serde error."""
        patches = _make_run_task_patches()
        _run_entry_defaults['kwargs_json'] = json.dumps({
            'upstream': {
                '__h_taskresult_envelope__': True,
                'data': '"just a string"',
            },
        })

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason

    # (test_workflow_ctx_results_by_id_bad_json removed: the strict-serde
    # phase 5/6 rewrite of workflow_ctx kwarg decode no longer raises
    # SerializationError on per-node decode failure. Per-node failures
    # now fold into a sentinel TaskResult(err=RESULT_DESERIALIZATION_ERROR)
    # so the workflow_ctx stays typed. New contract is covered by
    # test_workflow_handle_decode.py and the integration tests.)

    def test_workflow_ctx_summaries_by_id_bad_json(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """workflow_ctx with invalid summaries_by_id JSON → serde error."""

        def _task_with_ctx(workflow_ctx: Any = None) -> TaskResult[str, TaskError]:
            return TaskResult(ok='done')

        patches = _make_run_task_patches(task_fn=_task_with_ctx)
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.return_value = TaskResult(ok='done')
        mock_task._original_fn = _task_with_ctx
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        _run_entry_defaults['kwargs_json'] = json.dumps({
            '__h_workflow_ctx__': {
                'workflow_id': 'wf-1',
                'task_index': 0,
                'task_name': 'my_task',
                'results_by_id': {},
                'summaries_by_id': {
                    'sub_wf': '{{{invalid',
                },
            },
        })

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason

    def test_workflow_ctx_summaries_by_id_bad_status(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """summaries_by_id with valid JSON but unrecognized status → serde error.

        Regression: a corrupt persisted status used to be silently coerced to
        WorkflowStatus.FAILED and injected; it must now surface as a worker
        serialization error instead.
        """

        def _task_with_ctx(workflow_ctx: Any = None) -> TaskResult[str, TaskError]:
            return TaskResult(ok='done')

        patches = _make_run_task_patches(task_fn=_task_with_ctx)
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.return_value = TaskResult(ok='done')
        mock_task._original_fn = _task_with_ctx
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        _run_entry_defaults['kwargs_json'] = json.dumps({
            '__h_workflow_ctx__': {
                'workflow_id': 'wf-1',
                'task_index': 0,
                'task_name': 'my_task',
                'results_by_id': {},
                'summaries_by_id': {
                    'sub_wf': json.dumps({'status': 'NOT_A_STATUS', 'total_tasks': 1}),
                },
            },
        })

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason

    def test_workflow_meta_injection(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """workflow_meta injected correctly when task declares the parameter."""

        def _task_with_meta(
            workflow_meta: Any = None,
        ) -> TaskResult[str, TaskError]:
            return TaskResult(ok=f'meta:{workflow_meta}')

        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.return_value = TaskResult(ok='meta_ok')
        mock_task._original_fn = _task_with_meta
        mock_task._fn = _task_with_meta
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        _run_entry_defaults['kwargs_json'] = json.dumps({
            '__h_workflow_meta__': {
                'workflow_id': 'wf-1',
                'task_index': 3,
                'task_name': 'my_task',
            },
        })

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is None


# ===================================================================
# K. _locate_app
# ===================================================================


@pytest.mark.unit
class TestLocateApp:
    """_locate_app import and validation paths."""

    def test_invalid_locator_no_colon(self) -> None:
        from horsies.core.errors import ConfigurationError
        from horsies.core.worker.child_runner import _locate_app

        with pytest.raises(ConfigurationError, match='invalid app locator'):
            _locate_app('no_colon_here')

    def test_invalid_locator_empty(self) -> None:
        from horsies.core.errors import ConfigurationError
        from horsies.core.worker.child_runner import _locate_app

        with pytest.raises(ConfigurationError, match='invalid app locator'):
            _locate_app('')

    def test_resolved_not_horsies_instance(self) -> None:
        from horsies.core.errors import ConfigurationError
        from horsies.core.worker.child_runner import _locate_app

        with patch(
            'horsies.core.worker.child_runner.import_module',
        ) as mock_import:
            fake_mod = MagicMock()
            fake_mod.app = 'not a Horsies instance'
            mock_import.return_value = fake_mod

            with pytest.raises(
                ConfigurationError,
                match='did not resolve to Horsies instance',
            ):
                _locate_app('some.module:app')

    def test_file_path_locator_uses_import_by_path(self) -> None:
        from horsies.core.app import Horsies
        from horsies.core.worker.child_runner import _locate_app

        mock_app = MagicMock(spec=Horsies)
        fake_mod = MagicMock()
        fake_mod.app = mock_app

        with patch(
            'horsies.core.worker.child_runner.import_by_path',
            return_value=fake_mod,
        ):
            result = _locate_app('/some/path.py:app')

        assert result is mock_app


# ===================================================================
# L. _dedupe_paths and _build_sys_path_roots
# ===================================================================


@pytest.mark.unit
class TestDedupeAndBuildSysPath:
    """Utility functions for sys.path management."""

    def test_dedupe_paths_removes_duplicates(self) -> None:
        from horsies.core.worker.child_runner import _dedupe_paths

        result = _dedupe_paths(['/a', '/b', '/a', '/c', '/b'])
        assert result == ['/a', '/b', '/c']

    def test_dedupe_paths_skips_empty(self) -> None:
        from horsies.core.worker.child_runner import _dedupe_paths

        result = _dedupe_paths(['', '/a', '', '/b'])
        assert result == ['/a', '/b']

    def test_build_sys_path_roots_with_extra_roots(self) -> None:
        from horsies.core.worker.child_runner import _build_sys_path_roots

        result = _build_sys_path_roots('mod:app', [], ['/extra'])
        assert any('/extra' in r for r in result)

    def test_build_sys_path_roots_with_file_locator(self) -> None:
        from horsies.core.worker.child_runner import _build_sys_path_roots

        result = _build_sys_path_roots('/tmp/myapp.py:app', [], [])
        assert any('/tmp' in r for r in result)

    def test_build_sys_path_roots_with_file_imports(self) -> None:
        from horsies.core.worker.child_runner import _build_sys_path_roots

        result = _build_sys_path_roots('mod:app', ['/opt/tasks.py'], [])
        assert any('/opt' in r for r in result)


# ===================================================================
# L. Pre-start expiry operation boundary
# ===================================================================


@pytest.mark.unit
class TestExpireClaimedBeforeStart:
    """The child maps typed expiry outcomes to its existing return contract."""

    @pytest.mark.parametrize('outcome_type', ['Applied', 'AlreadyApplied'])
    def test_applied_or_replayed_expiry_commits_and_returns_expired(
        self,
        outcome_type: str,
    ) -> None:
        from horsies.core.lifecycle.commands import ExpireOwnedClaim
        from horsies.core.lifecycle.outcomes import AlreadyApplied, Applied
        from horsies.core.worker.child_runner import (
            _expire_claimed_task_before_start,
        )

        outcomes = {'Applied': Applied, 'AlreadyApplied': AlreadyApplied}
        cursor = _FakeCursor()
        conn = _FakeConn(cursor)
        with patch(
            'horsies.core.worker.child_runner.apply_sync',
            return_value=MagicMock(spec=outcomes[outcome_type]),
        ) as apply:
            result = _expire_claimed_task_before_start(
                cursor,
                conn,
                'task-1',
                'worker-A',
            )

        assert result == (False, '', OutcomeCode.TASK_EXPIRED.value)
        assert conn.commits == 1
        command = apply.call_args.args[1]
        assert isinstance(command, ExpireOwnedClaim)
        assert command.task_id == 'task-1'
        assert command.fence.worker_id == 'worker-A'
        assert command.error_code == OutcomeCode.TASK_EXPIRED.value
        assert OutcomeCode.TASK_EXPIRED.value in command.result_json

    @pytest.mark.parametrize(
        'outcome_type',
        ['LostClaim', 'SourceStateConflict', 'TaskAbsent'],
    )
    def test_refusal_or_absence_falls_through_to_existing_classification(
        self,
        outcome_type: str,
    ) -> None:
        from horsies.core.lifecycle.outcomes import (
            LostClaim,
            SourceStateConflict,
            TaskAbsent,
        )
        from horsies.core.worker.child_runner import (
            _expire_claimed_task_before_start,
        )

        outcomes = {
            'LostClaim': LostClaim,
            'SourceStateConflict': SourceStateConflict,
            'TaskAbsent': TaskAbsent,
        }
        cursor = _FakeCursor()
        conn = _FakeConn(cursor)
        with patch(
            'horsies.core.worker.child_runner.apply_sync',
            return_value=MagicMock(spec=outcomes[outcome_type]),
        ):
            result = _expire_claimed_task_before_start(
                cursor,
                conn,
                'task-1',
                'worker-A',
            )

        assert result is None
        assert conn.commits == 0


# ===================================================================
# M. Ownership lost → workflow PAUSED/CANCELLED sub-branch
# ===================================================================


@pytest.mark.unit
class TestOwnershipLostWorkflowBranch:
    """When ownership is lost AND the workflow is PAUSED/CANCELLED."""

    def test_ownership_lost_workflow_paused_dispatches(self) -> None:
        """UPDATE returns None, workflow is PAUSED → handler called."""
        from horsies.core.lifecycle.commands import AbandonOwnedNode
        from horsies.core.lifecycle.outcomes import Applied
        from horsies.core.worker.child_runner import (
            _confirm_ownership_and_set_running,
        )

        call_count = 0

        class _MultiReturnCursor(_FakeCursor):
            def fetchone(self) -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return None  # UPDATE RETURNING → None (ownership lost)
                # Workflow status check → PAUSED
                return _FakeRow(status='PAUSED')

        cursor = _MultiReturnCursor()
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ), patch(
            'horsies.core.worker.child_runner._expire_claimed_task_before_start',
            return_value=None,
        ), patch(
            'horsies.core.worker.child_runner._update_workflow_task_running_with_retry',
        ), patch(
            'horsies.core.worker.child_runner.apply_sync',
            return_value=MagicMock(spec=Applied),
        ) as apply:
            result = _confirm_ownership_and_set_running('task-1', 'worker-A')

        assert result is not None
        ok, _, reason = result
        assert ok is False
        assert reason == 'WORKFLOW_STOPPED'
        command = apply.call_args.args[1]
        assert isinstance(command, AbandonOwnedNode)
        assert command.task_id == 'task-1'

    def test_ownership_lost_due_to_good_until_marks_expired(self) -> None:
        """UPDATE blocked by good_until at actual execution start marks EXPIRED."""
        from horsies.core.worker.child_runner import (
            _confirm_ownership_and_set_running,
        )

        cursor = _FakeCursor(fetchone_return=None)
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)
        expired = (False, '', OutcomeCode.TASK_EXPIRED.value)

        def expire_and_commit(*args: Any) -> tuple[bool, str, str]:  # noqa: ARG001
            conn.commit()
            return expired

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ), patch(
            'horsies.core.worker.child_runner._expire_claimed_task_before_start',
            side_effect=expire_and_commit,
        ), patch(
            'horsies.core.worker.child_runner._update_workflow_task_running_with_retry',
        ) as update_workflow_task:
            result = _confirm_ownership_and_set_running('task-1', 'worker-A')

        assert result == (False, '', OutcomeCode.TASK_EXPIRED.value)
        assert conn.commits == 1
        assert conn.rollbacks == 0
        update_workflow_task.assert_not_called()
        executed_sql = '\n'.join(sql for sql, _ in cursor.queries)
        assert 'good_until IS NULL OR good_until > now()' in executed_sql

    def test_expire_race_falls_back_to_claim_lost(self) -> None:
        """If the expiry UPDATE matches no row, ownership loss is still reported."""
        from horsies.core.worker.child_runner import (
            _confirm_ownership_and_set_running,
        )

        class _MultiReturnCursor(_FakeCursor):
            def __init__(self) -> None:
                super().__init__()
                self._returns = [
                    None,  # RUNNING UPDATE rejected
                    None,  # workflow status check: not PAUSED/CANCELLED
                ]

            def fetchone(self) -> Any:
                assert self._returns, 'unexpected extra fetchone() call'
                return self._returns.pop(0)

        cursor = _MultiReturnCursor()
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ), patch(
            'horsies.core.worker.child_runner._expire_claimed_task_before_start',
            return_value=None,
        ), patch(
            'horsies.core.worker.child_runner._update_workflow_task_running_with_retry',
        ) as update_workflow_task:
            result = _confirm_ownership_and_set_running('task-1', 'worker-A')

        assert result == (False, '', 'CLAIM_LOST')
        assert conn.commits == 0
        assert conn.rollbacks == 1
        update_workflow_task.assert_not_called()


# ===================================================================
# M2. Regression: workflow_task RUNNING sync failure aborts execution
# ===================================================================


@pytest.mark.unit
class TestWorkflowTaskRunningSyncFailureAborts:
    """Workflow RUNNING sync failures abort before user code starts."""

    def test_running_sync_failure_returns_preexec_abort(self) -> None:
        """Task rolls back when workflow_task RUNNING update fails."""
        from horsies.core.worker.child_runner import (
            _confirm_ownership_and_set_running,
        )

        class _SequenceCursor(_FakeCursor):
            def __init__(self) -> None:
                super().__init__()
                self._rows = [_FakeRow(id='task-1'), None]

            def fetchone(self) -> Any:
                return self._rows.pop(0)

        cursor = _SequenceCursor()
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ):
            result = _confirm_ownership_and_set_running('task-1', 'worker-A')

        assert result == (False, '', 'WORKFLOW_CHECK_FAILED')
        assert conn.commits == 0
        assert conn.rollbacks == 1

    def test_running_sync_success_returns_none(self) -> None:
        """When workflow_task RUNNING update succeeds, returns None (proceed)."""
        from horsies.core.worker.child_runner import (
            _confirm_ownership_and_set_running,
        )

        cursor = _FakeCursor(fetchone_return=_FakeRow(id='task-1'))
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ):
            result = _confirm_ownership_and_set_running('task-1', 'worker-A')

        assert result is None  # None = proceed to user code
        sql_blob = '\n'.join(q[0] for q in cursor.queries)
        assert "status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')" in sql_blob

    def test_first_heartbeat_rides_running_transaction(self) -> None:
        """The initial runner heartbeat is inserted before the RUNNING commit.

        Regression pin for the WAN statement-budget work: a row must never
        be observable as RUNNING without heartbeat coverage, and the
        heartbeat must not cost a separate transaction.
        """
        from horsies.core.worker.child_runner import (
            _confirm_ownership_and_set_running,
        )

        cursor = _FakeCursor(fetchone_return=_FakeRow(id='task-1'))
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ):
            result = _confirm_ownership_and_set_running(
                'task-1', 'worker-A', is_workflow_task=False,
            )

        assert result is None
        heartbeat_queries = [
            q for q in cursor.queries if 'horsies_heartbeats' in q[0]
        ]
        assert len(heartbeat_queries) == 1
        _, params = heartbeat_queries[0]
        assert params[0] == 'task-1'
        assert params[1].startswith('worker-A:')
        assert conn.commits == 1


# ===================================================================
# N. Workflow ctx task_result_from_json error path
# ===================================================================


# (TestWorkflowCtxTaskResultFromJsonError removed: the legacy
# JSON-string-shape protection has been replaced by the strict-serde
# per-node decode fold — see the comment above
# test_workflow_ctx_summaries_by_id_bad_json. Per-node decode failures
# are exercised at the model layer in test_workflow_handle_decode.py.)


# ===================================================================
# O. Happy-path lines for workflow injection + taskresult serde
# ===================================================================


@pytest.mark.unit
class TestRunTaskEntryWorkflowHappyPaths:
    """Exercise successful deserialization paths that were uncovered."""

    def test_horsies_taskresult_successful_deserialization(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """``__h_taskresult_envelope__`` (args_from) decodes via the
        registered source task's ``task_ok_type``.

        Strict-serde phase 5/6 wire format:

            {
                '__h_taskresult_envelope__': True,
                'source_task_name': '<upstream>',
                'inner': <encode_task_result(tr, source_ok_type)>,
            }
        """
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value

        # Source task (provides ok_type for the upstream envelope's inner).
        source_task = MagicMock()
        source_task.task_ok_type = str

        # Consumer task whose kwarg gets injected. Real type for its
        # own return-encoding too.
        mock_task = MagicMock()
        mock_task.task_ok_type = str
        mock_task.return_value = TaskResult(ok='processed')

        def _task_lookup(name: str) -> Any:
            if name == 'upstream_task':
                return source_task
            return mock_task

        mock_app.tasks.__getitem__ = MagicMock(side_effect=_task_lookup)
        mock_app.tasks.get = MagicMock(side_effect=_task_lookup)

        # New envelope: source_task_name + inner = full task-result envelope.
        _run_entry_defaults['kwargs_json'] = json.dumps({
            'upstream': {
                '__h_taskresult_envelope__': True,
                'source_task_name': 'upstream_task',
                'inner': {
                    '__h_task_result__': True,
                    'ok': 'upstream_value',
                    'err': None,
                },
            },
        })

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is None

    def test_workflow_ctx_successful_results_and_summaries(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """workflow_ctx with valid results_by_id + summaries_by_id → injected."""

        def _task_with_ctx(workflow_ctx: Any = None) -> TaskResult[str, TaskError]:
            return TaskResult(ok='done')

        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.return_value = TaskResult(ok='ctx_ok')
        mock_task._original_fn = _task_with_ctx
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        valid_tr = json.dumps({
            '__task_result__': True,
            'ok': 'node_a_result',
        })
        valid_summary = json.dumps({
            'status': 'COMPLETED',
            'output': None,
            'total_tasks': 3,
            'completed_tasks': 3,
            'failed_tasks': 0,
            'skipped_tasks': 0,
        })
        _run_entry_defaults['kwargs_json'] = json.dumps({
            '__h_workflow_ctx__': {
                'workflow_id': 'wf-1',
                'task_index': 0,
                'task_name': 'my_task',
                'results_by_id': {
                    'node_a': valid_tr,
                },
                'summaries_by_id': {
                    'sub_wf': valid_summary,
                },
            },
        })

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is None

    # (test_workflow_ctx_results_by_id_task_result_from_json_error removed:
    # the patched-out ``task_result_from_json`` helper no longer exists
    # in the strict-serde rebuild. Per-node decode failure now folds
    # into a RESULT_DESERIALIZATION_ERROR sentinel — covered at the
    # model layer in test_workflow_handle_decode.py.)

# ===================================================================
# P. _start_heartbeat_thread
# ===================================================================


@pytest.mark.unit
class TestStartHeartbeatThread:
    """_start_heartbeat_thread creates and starts a daemon thread."""

    def test_creates_daemon_thread(self) -> None:
        from horsies.core.worker.child_runner import _start_heartbeat_thread

        stop = threading.Event()
        stop.set()  # Stop immediately

        cursor = _FakeCursor()
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ):
            thread = _start_heartbeat_thread(
                task_id='t-1',
                database_url='unused',
                heartbeat_stop_event=stop,
                worker_id='w-1',
                runner_heartbeat_interval_ms=100,
            )

        assert thread.daemon is True
        assert 'heartbeat' in thread.name
        thread.join(timeout=2)


# ===================================================================
# Helpers
# ===================================================================


@contextmanager
def _apply_patches(patches: dict[str, Any]) -> Generator[None, None, None]:
    """Apply multiple unittest.mock.patch targets from a dict."""
    stack: list[Any] = []
    try:
        for target, value in patches.items():
            if isinstance(value, MagicMock):
                p = patch(target, value)
            else:
                p = patch(target, return_value=value)
            p.start()
            stack.append(p)
        yield
    finally:
        for p in reversed(stack):
            p.stop()
