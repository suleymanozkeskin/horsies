"""Unit tests for child_runner.py — full coverage.

Sections:
A. _serialization_error_response
B. _debug_imports_log
C. _child_initializer
D. _heartbeat_worker
E. _get_workflow_status_for_task
F. _handle_workflow_stop_before_start (wildcard branch)
G. _update_workflow_task_running_with_retry
H. _preflight_workflow_check
I. _run_task_entry error paths
J. Workflow injection deserialization errors in _run_task_entry
"""

from __future__ import annotations

import json
import threading
import time
from contextlib import contextmanager
from typing import Any, Generator
from unittest.mock import MagicMock, patch, call

import pytest

from horsies.core.codec.serde import SerializationError
from horsies.core.models.tasks import LibraryErrorCode, TaskError, TaskResult
from horsies.core.types.result import Err, Ok
from horsies.core.worker.child_runner import (
    _debug_imports_log,
    _get_workflow_status_for_task,
    _handle_workflow_stop_before_start,
    _heartbeat_worker,
    _is_retryable_db_error,
    _preflight_workflow_check,
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
        assert err_data['error_code'] == LibraryErrorCode.WORKER_SERIALIZATION_ERROR.value

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
    @patch('horsies.core.logging.set_default_level')
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
        mock_init_pool.assert_called_once_with('postgresql://localhost/test')

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.signal.signal')
    @patch('horsies.core.logging.set_default_level')
    def test_suppress_sends_exception_swallowed(
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

        # Should not raise despite suppress_sends failing
        _child_initializer(
            app_locator='mymod:app',
            imports=[],
            sys_path_roots=[],
            loglevel=20,
            database_url='postgresql://localhost/test',
        )
        mock_init_pool.assert_called_once()

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.signal.signal')
    @patch('horsies.core.logging.set_default_level')
    def test_discovered_task_modules_exception_swallowed(
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

        _child_initializer(
            app_locator='mymod:app',
            imports=[],
            sys_path_roots=[],
            loglevel=20,
            database_url='postgresql://localhost/test',
        )
        mock_init_pool.assert_called_once()

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.signal.signal')
    @patch('horsies.core.logging.set_default_level')
    def test_keys_list_fallback_to_keys(
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

        _child_initializer(
            app_locator='mymod:app',
            imports=[],
            sys_path_roots=[],
            loglevel=20,
            database_url='postgresql://localhost/test',
        )
        # keys() fallback should have been called
        mocks['app'].tasks.keys.assert_called()

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.import_module')
    @patch('horsies.core.worker.child_runner.signal.signal')
    @patch('horsies.core.logging.set_default_level')
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
    @patch('horsies.core.logging.set_default_level')
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
# D. _heartbeat_worker
# ===================================================================


@pytest.mark.unit
class TestHeartbeatWorker:
    """_heartbeat_worker loop and failure paths."""

    def test_stops_on_event_set(self) -> None:
        """Heartbeat thread exits when stop_event is set."""
        cursor = _FakeCursor()
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)
        stop = threading.Event()

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ):
            # Set stop event immediately so the loop exits after first heartbeat
            stop.set()
            _heartbeat_worker(
                task_id='t-1',
                database_url='unused',
                stop_event=stop,
                sender_worker_id='w-1',
                heartbeat_interval_ms=100,
            )

        # At least the initial heartbeat was sent
        assert len(cursor.queries) >= 1
        assert 'horsies_heartbeats' in cursor.queries[0][0]

    def test_stops_on_send_failure(self) -> None:
        """Heartbeat thread exits when send_heartbeat raises."""
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
            # Subsequent calls fail
            raise RuntimeError('db down')

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

        # Thread should have exited after the second (failed) heartbeat
        assert call_count >= 2


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
        cursor = _FakeCursor()
        conn = _FakeConn(cursor)
        result = _handle_workflow_stop_before_start(
            cursor, conn, 'task-1', 'CANCELLED',  # type: ignore[arg-type]
        )
        assert result == (False, '', 'WORKFLOW_STOPPED')
        assert conn.commits == 1
        sql_blob = '\n'.join(q[0] for q in cursor.queries)
        assert "SET status = 'SKIPPED'" in sql_blob
        assert "SET status = 'CANCELLED'" in sql_blob

    def test_paused_requeues(self) -> None:
        cursor = _FakeCursor()
        conn = _FakeConn(cursor)
        result = _handle_workflow_stop_before_start(
            cursor, conn, 'task-2', 'PAUSED',  # type: ignore[arg-type]
        )
        assert result == (False, '', 'WORKFLOW_STOPPED')
        assert conn.commits == 1
        sql_blob = '\n'.join(q[0] for q in cursor.queries)
        assert "SET status = 'PENDING'" in sql_blob
        assert "SET status = 'READY'" in sql_blob

    def test_unknown_status_returns_workflow_check_failed(self) -> None:
        cursor = _FakeCursor()
        conn = _FakeConn(cursor)
        result = _handle_workflow_stop_before_start(
            cursor, conn, 'task-3', 'UNKNOWN_STATUS',  # type: ignore[arg-type]
        )
        assert result == (False, '', 'WORKFLOW_CHECK_FAILED')
        # No SQL should have been executed for the unknown branch
        assert len(cursor.queries) == 0
        assert conn.commits == 0


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
            _update_workflow_task_running_with_retry('task-1')

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
            _update_workflow_task_running_with_retry('task-1')

        assert call_count == 2
        assert conn.commits == 1

    def test_retryable_error_exhausts_all_attempts(self) -> None:
        """OperationalError on all attempts → logs error, returns."""
        from psycopg import OperationalError

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            side_effect=OperationalError('permanent'),
        ), patch('horsies.core.worker.child_runner.time.sleep'):
            # Should not raise
            _update_workflow_task_running_with_retry('task-1')

    def test_non_retryable_error_fails_immediately(self) -> None:
        """Non-retryable error (e.g. ValueError) does not retry."""
        call_count = 0

        def _pool_factory() -> Any:
            nonlocal call_count
            call_count += 1
            raise ValueError('schema error')

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            side_effect=_pool_factory,
        ):
            _update_workflow_task_running_with_retry('task-1')

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
# H. _preflight_workflow_check
# ===================================================================


@pytest.mark.unit
class TestPreflightWorkflowCheck:
    """_preflight_workflow_check dispatches or returns None."""

    def test_running_workflow_returns_none(self) -> None:
        """Non-stopped workflow → None (proceed with execution)."""
        row = _FakeRow(status='RUNNING')
        cursor = _FakeCursor(fetchone_return=row)
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ):
            result = _preflight_workflow_check('task-1')

        assert result is None

    def test_no_workflow_returns_none(self) -> None:
        """Task not in any workflow → None."""
        cursor = _FakeCursor(fetchone_return=None)
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ):
            result = _preflight_workflow_check('task-1')

        assert result is None

    def test_paused_workflow_dispatches_to_handler(self) -> None:
        """PAUSED workflow → dispatches to _handle_workflow_stop_before_start."""
        row = _FakeRow(status='PAUSED')
        cursor = _FakeCursor(fetchone_return=row)
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ):
            result = _preflight_workflow_check('task-1')

        assert result is not None
        ok, _, reason = result
        assert ok is False
        assert reason == 'WORKFLOW_STOPPED'

    def test_cancelled_workflow_dispatches_to_handler(self) -> None:
        """CANCELLED workflow → dispatches to _handle_workflow_stop_before_start."""
        row = _FakeRow(status='CANCELLED')
        cursor = _FakeCursor(fetchone_return=row)
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ):
            result = _preflight_workflow_check('task-1')

        assert result is not None
        ok, _, reason = result
        assert ok is False
        assert reason == 'WORKFLOW_STOPPED'

    def test_exception_returns_workflow_check_failed(self) -> None:
        """DB exception → WORKFLOW_CHECK_FAILED."""
        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            side_effect=RuntimeError('pool exploded'),
        ):
            result = _preflight_workflow_check('task-1')

        assert result is not None
        ok, _, reason = result
        assert ok is False
        assert reason == 'WORKFLOW_CHECK_FAILED'


# ===================================================================
# I. _run_task_entry error paths
# ===================================================================


def _make_run_task_patches(
    *,
    preflight: Any = None,
    ownership: Any = None,
    task_fn: Any = None,
    task_missing: bool = False,
) -> dict[str, Any]:
    """Build common patches for _run_task_entry tests.

    Returns a dict of patch targets → values suitable for use with `patch`.
    """
    patches: dict[str, Any] = {
        'horsies.core.worker.child_runner._preflight_workflow_check': MagicMock(
            return_value=preflight,
        ),
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

    def test_preflight_blocks_execution(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """When preflight returns a tuple, _run_task_entry returns it immediately."""
        sentinel = (False, '', 'WORKFLOW_STOPPED')
        patches = _make_run_task_patches(preflight=sentinel)

        with _apply_patches(patches):
            result = _run_task_entry(**_run_entry_defaults)

        assert result == sentinel

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
        assert err['error_code'] == LibraryErrorCode.WORKER_RESOLUTION_ERROR.value

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
        assert err['error_code'] == LibraryErrorCode.WORKER_SERIALIZATION_ERROR.value

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
        assert err['error_code'] == LibraryErrorCode.TASK_EXCEPTION.value

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
        assert err['error_code'] == LibraryErrorCode.TASK_EXCEPTION.value
        assert 'ValueError' in err['message']
        assert 'boom' in err['message']

    def test_task_returns_plain_value_wrapped(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Task returning a plain value → wrapped into TaskResult(ok=value)."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.return_value = 42
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is None
        parsed = _parse_task_result(payload)
        assert parsed.get('ok') == 42 or parsed.get('__task_result__') is not None

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
        """__horsies_taskresult__ with invalid 'data' string → serde error."""
        patches = _make_run_task_patches()
        _run_entry_defaults['kwargs_json'] = json.dumps({
            'upstream': {
                '__horsies_taskresult__': True,
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
        """__horsies_taskresult__ with valid JSON but not TaskResult shape → serde error."""
        patches = _make_run_task_patches()
        _run_entry_defaults['kwargs_json'] = json.dumps({
            'upstream': {
                '__horsies_taskresult__': True,
                'data': '"just a string"',
            },
        })

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason

    def test_workflow_ctx_results_by_id_bad_json(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """workflow_ctx with invalid results_by_id JSON → serde error."""

        def _task_with_ctx(workflow_ctx: Any = None) -> TaskResult[str, TaskError]:
            return TaskResult(ok='done')

        patches = _make_run_task_patches(task_fn=_task_with_ctx)
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.return_value = TaskResult(ok='done')
        mock_task._original_fn = _task_with_ctx
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        _run_entry_defaults['kwargs_json'] = json.dumps({
            '__horsies_workflow_ctx__': {
                'workflow_id': 'wf-1',
                'task_index': 0,
                'task_name': 'my_task',
                'results_by_id': {
                    'node_a': '{{{invalid',
                },
                'summaries_by_id': {},
            },
        })

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason

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
            '__horsies_workflow_ctx__': {
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
            '__horsies_workflow_meta__': {
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
# M. Ownership lost → workflow PAUSED/CANCELLED sub-branch
# ===================================================================


@pytest.mark.unit
class TestOwnershipLostWorkflowBranch:
    """When ownership is lost AND the workflow is PAUSED/CANCELLED."""

    def test_ownership_lost_workflow_paused_dispatches(self) -> None:
        """UPDATE returns None, workflow is PAUSED → handler called."""
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
            'horsies.core.worker.child_runner._update_workflow_task_running_with_retry',
        ):
            result = _confirm_ownership_and_set_running('task-1', 'worker-A')

        assert result is not None
        ok, _, reason = result
        assert ok is False
        assert reason == 'WORKFLOW_STOPPED'


# ===================================================================
# N. Workflow ctx task_result_from_json error path
# ===================================================================


@pytest.mark.unit
class TestWorkflowCtxTaskResultFromJsonError:
    """workflow_ctx results_by_id with valid JSON but invalid TaskResult shape."""

    def test_workflow_ctx_results_by_id_not_task_result(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """results_by_id value is valid JSON but not a TaskResult → serde error."""

        def _task_with_ctx(workflow_ctx: Any = None) -> TaskResult[str, TaskError]:
            return TaskResult(ok='done')

        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.return_value = TaskResult(ok='done')
        mock_task._original_fn = _task_with_ctx
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        # Valid JSON string, but not a TaskResult shape (just a number)
        _run_entry_defaults['kwargs_json'] = json.dumps({
            '__horsies_workflow_ctx__': {
                'workflow_id': 'wf-1',
                'task_index': 0,
                'task_name': 'my_task',
                'results_by_id': {
                    'node_a': '42',
                },
                'summaries_by_id': {},
            },
        })

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason


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
        """__horsies_taskresult__ with valid TaskResult JSON → kwarg replaced."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.return_value = TaskResult(ok='processed')
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        # Valid serialized TaskResult
        valid_tr_json = json.dumps({
            '__task_result__': True,
            'ok': 'upstream_value',
        })
        _run_entry_defaults['kwargs_json'] = json.dumps({
            'upstream': {
                '__horsies_taskresult__': True,
                'data': valid_tr_json,
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
            'success_case': None,
            'output': None,
            'total_tasks': 3,
            'completed_tasks': 3,
            'failed_tasks': 0,
            'skipped_tasks': 0,
        })
        _run_entry_defaults['kwargs_json'] = json.dumps({
            '__horsies_workflow_ctx__': {
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

    def test_workflow_ctx_results_by_id_task_result_from_json_error(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """results_by_id value deserializes as JSON but task_result_from_json fails."""

        def _task_with_ctx(workflow_ctx: Any = None) -> TaskResult[str, TaskError]:
            return TaskResult(ok='done')

        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.return_value = TaskResult(ok='done')
        mock_task._original_fn = _task_with_ctx
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        # Valid JSON dict but missing __task_result__ key (and no ok/err)
        bad_tr = json.dumps({'random': 'data'})
        _run_entry_defaults['kwargs_json'] = json.dumps({
            '__horsies_workflow_ctx__': {
                'workflow_id': 'wf-1',
                'task_index': 0,
                'task_name': 'my_task',
                'results_by_id': {
                    'node_a': bad_tr,
                },
                'summaries_by_id': {},
            },
        })

        with _apply_patches(patches):
            ok, payload, reason = _run_task_entry(**_run_entry_defaults)

        assert ok is True
        assert reason is not None and 'SerializationError' in reason


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
