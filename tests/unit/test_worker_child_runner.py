"""Unit tests for child_runner.py — full coverage.

Sections:
A. _serialization_error_response
B. _debug_imports_log
C. _child_initializer
E. _get_workflow_status_for_task
F. _handle_workflow_stop_before_start (wildcard branch)
I. _run_task_entry error paths
J. Workflow injection deserialization errors in _run_task_entry
K. Async task calls in _run_task_entry
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel, SecretStr, field_validator
from pydantic_core import PydanticCustomError

from horsies.core.app import Horsies
from horsies.core.codec.json_io import SerializationError, loads_json
from horsies.core.codec.typed import decode_task_result
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.worker_profile import ResolvedPool
from horsies.core.models.tasks import (
    MAX_CANCEL_GRACE_MS,
    MAX_TASK_TIMEOUT_MS,
    MIN_CANCEL_GRACE_MS,
    MIN_TASK_TIMEOUT_MS,
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import Err
from horsies.core.worker.stored_timeout import (
    CancelGraceInvalid,
    OptionsUnreadable,
    TimeoutRefusal,
    TimeoutValueInvalid,
)
from horsies.core.task_decorator import TaskBodyKind
from horsies.core.worker.task_loop import LoopOpen, TaskLoop
from horsies.core.worker.child_runner import (
    _debug_imports_log,
    _get_workflow_status_for_task,
    _handle_workflow_stop_before_start,
    _run_task_entry,
    _serialization_error_response,
)
from horsies.core.worker.run_return import SetRunning, StoppedBeforeRunning
from tests.fixtures.run_return import wire_of


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
    def connection(
        self,
        timeout: float | None = None,
    ) -> Generator[_FakeConn, None, None]:
        """``timeout`` is the wait for a connection, as in psycopg_pool."""
        _ = timeout
        yield self._conn


class _FakeRow:
    """Minimal named-tuple-like row returned by fetchone."""

    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


class _SequenceCursor(_FakeCursor):
    """Cursor whose fetchone returns the given rows in order."""

    def __init__(self, rows: list[Any]) -> None:
        super().__init__()
        self._rows = rows

    def fetchone(self) -> Any:
        return self._rows.pop(0)


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
    @patch('horsies.core.logging.configure_logging')
    def test_happy_path(
        self,
        mock_set_level: MagicMock,
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
            pool=ResolvedPool(min_size=0, max_size=2, check=True),
            task_send_pool=2,
        )

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
    @patch('horsies.core.logging.configure_logging')
    def test_child_pool_bounds_are_forwarded(
        self,
        mock_set_level: MagicMock,
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
            pool=ResolvedPool(min_size=0, max_size=1, check=False),
            task_send_pool=2,
            connect_kwargs={'keepalives': 1, 'prepare_threshold': None},
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
    @patch('horsies.core.logging.configure_logging')
    def test_child_initializer_discards_inherited_broker(
        self,
        mock_set_level: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_pool: MagicMock,
    ) -> None:
        from horsies.core.worker.child_runner import _child_initializer

        from horsies.core.brokers.postgres import PostgresBroker

        mocks = self._make_patches(app_tasks={'add': True})
        inherited_broker = MagicMock(spec=PostgresBroker)
        mocks['app']._broker = inherited_broker
        mock_locate_app.return_value = mocks['app']

        _child_initializer(
            app_locator='mymod:app',
            imports=[],
            sys_path_roots=[],
            loglevel=20,
            database_url='postgresql://localhost/test',
            pool=ResolvedPool(min_size=0, max_size=2, check=True),
            task_send_pool=2,
        )

        inherited_broker.discard_inherited_resources.assert_called_once_with()
        assert mocks['app']._broker is None
        mocks['app'].set_role.assert_called_once_with('worker')

    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_module')
    @patch('horsies.core.logging.configure_logging')
    def test_an_import_time_broker_opens_the_task_send_pool(
        self,
        _mock_logging: MagicMock,
        mock_import_module: MagicMock,
        mock_locate_app: MagicMock,
        _mock_set_current: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The child sets the task-send pool of its kind before it imports
        task modules, so a broker that an import builds through
        ``app.get_broker()`` opens that pool, not the producer pool."""
        from horsies.core.worker.child_runner import import_app_for_child

        monkeypatch.delenv('HORSIES_CHILD_PROCESS', raising=False)
        app = Horsies(
            AppConfig(
                broker=PostgresConfig(
                    database_url=SecretStr('postgresql+psycopg://u:p@localhost/db'),
                    pool_size=30,
                    max_overflow=30,
                ),
            ),
        )
        mock_locate_app.return_value = app
        built: list[tuple[int, int]] = []

        def import_that_builds_a_broker(_name: str) -> None:
            config = app.get_broker().config
            built.append((config.pool_size, config.max_overflow))

        mock_import_module.side_effect = import_that_builds_a_broker

        returned = import_app_for_child('mymod:app', ['tasks_mod'], [], 20, 4)

        try:
            assert returned is app
            assert built == [(4, 0)]
        finally:
            _ = app.get_broker().close()

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.logging.configure_logging')
    def test_suppress_sends_failure_propagates(
        self,
        mock_set_level: MagicMock,
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
                pool=ResolvedPool(min_size=0, max_size=2, check=True),
                task_send_pool=2,
            )
        mock_init_pool.assert_not_called()

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.logging.configure_logging')
    def test_discovered_task_modules_failure_propagates(
        self,
        mock_set_level: MagicMock,
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
                pool=ResolvedPool(min_size=0, max_size=2, check=True),
                task_send_pool=2,
            )
        mock_init_pool.assert_not_called()

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.logging.configure_logging')
    def test_keys_list_failure_propagates(
        self,
        mock_set_level: MagicMock,
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
                pool=ResolvedPool(min_size=0, max_size=2, check=True),
                task_send_pool=2,
            )

    @patch('horsies.core.worker.child_runner._initialize_worker_pool')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.worker.child_runner.import_module')
    @patch('horsies.core.logging.configure_logging')
    @patch('horsies.core.worker.child_runner.os.path.samefile', return_value=False)
    def test_file_path_import_uses_import_by_path(
        self,
        mock_samefile: MagicMock,
        mock_set_level: MagicMock,
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
            pool=ResolvedPool(min_size=0, max_size=2, check=True),
            task_send_pool=2,
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
    @patch('horsies.core.logging.configure_logging')
    @patch('horsies.core.worker.child_runner.os.path.samefile', return_value=True)
    def test_file_path_import_skips_app_module(
        self,
        mock_samefile: MagicMock,
        mock_set_level: MagicMock,
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
            pool=ResolvedPool(min_size=0, max_size=2, check=True),
            task_send_pool=2,
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

        assert mock_pool.call_args.kwargs['kwargs'] == {
            **connect_kwargs,
            'autocommit': True,
        }

    def test_no_connect_kwargs_yields_an_autocommit_pool(self) -> None:
        """Every connection of a child pool is in autocommit, and the
        checkout check is off unless asked for."""
        from horsies.core.worker.child_pool import _initialize_worker_pool

        with self._reset_pool(), patch(
            'horsies.core.worker.child_pool.ConnectionPool'
        ) as mock_pool, patch(
            'horsies.core.worker.child_pool.atexit.register'
        ):
            _initialize_worker_pool('postgresql://localhost/test')

        assert mock_pool.call_args.kwargs['kwargs'] == {'autocommit': True}
        assert mock_pool.call_args.kwargs['check'] is None

    @pytest.mark.asyncio
    async def test_the_shared_async_pool_is_an_autocommit_pool(self) -> None:
        from horsies.core.worker.child_pool import open_shared_async_pool

        with patch('horsies.core.worker.child_pool.AsyncConnectionPool') as mock_pool:
            mock_pool.return_value.open = AsyncMock()
            _ = await open_shared_async_pool(
                'postgresql://localhost/test',
                connect_kwargs={'keepalives': 1},
                min_size=0,
                max_size=2,
                check_on_checkout=False,
            )

        assert mock_pool.call_args.kwargs['kwargs'] == {
            'keepalives': 1,
            'autocommit': True,
        }
        assert mock_pool.call_args.kwargs['check'] is None

    def test_a_refused_pool_thread_is_a_refused_thread_start(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The open starts the threads of the real pool. A refused start is
        named as such, and no pool is kept."""
        import horsies.core.worker.child_pool as child_pool
        from horsies.core.models.boot_failure import ThreadStartRefusedError

        def refuse(_thread: threading.Thread) -> None:
            raise RuntimeError("can't start new thread")

        with (
            self._reset_pool(),
            patch('horsies.core.worker.child_pool.atexit.register') as register,
        ):
            monkeypatch.setattr(threading.Thread, 'start', refuse)
            with pytest.raises(ThreadStartRefusedError) as raised:
                child_pool._initialize_worker_pool(  # pyright: ignore[reportPrivateUsage]
                    'postgresql://localhost/test',
                )
            monkeypatch.undo()

            assert raised.value.thread == 'a thread of the connection pool'
            assert str(raised.value.refusal) == "can't start new thread"
            assert child_pool._worker_pool is None  # pyright: ignore[reportPrivateUsage]
            register.assert_not_called()


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

        def _before_node_update(*_args: Any) -> MagicMock:
            assert cursor.queries == []
            return MagicMock(spec=Applied)

        with patch(
            'horsies.core.worker.child_runner.apply_sync',
            side_effect=_before_node_update,
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
    }

    mock_app = MagicMock()
    if task_missing:
        mock_app.tasks.__getitem__ = MagicMock(
            side_effect=KeyError('no_such_task'),
        )
    elif task_fn is not None:
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.__call__ = MagicMock(return_value=task_fn())
        mock_task._original_fn = task_fn
        mock_task._fn = task_fn
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)
    else:
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
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
        'master_worker_id': 'w-1',
        'task_loop': TaskLoop(),
    }


def _takes_upstream(*, upstream: TaskResult[str, TaskError]) -> TaskResult[str, TaskError]:
    return TaskResult(ok=upstream.unwrap())


class _EchoingModel(BaseModel):
    """A validator that writes the refused value in its error text."""

    name: str

    @field_validator('name')
    @classmethod
    def refuse(cls, value: str) -> str:
        raise ValueError(f'bad name {value}')


class _ConstantSurrogateModel(BaseModel):
    """A validator whose own text holds a lone surrogate.

    Stored JSON cannot hold one, so a decode reaches the case only this way.
    """

    name: str

    @field_validator('name')
    @classmethod
    def refuse(cls, value: str) -> str:
        raise ValueError(f'bad name {value}{chr(0xd800)}')


class _CustomErrorModel(BaseModel):
    """A validator that writes the refused value in the error context."""

    name: str

    @field_validator('name')
    @classmethod
    def refuse(cls, value: str) -> str:
        raise PydanticCustomError('bad_name', 'bad name {name}', {'name': value})


def _constant_surrogate_task(*, x: _ConstantSurrogateModel) -> None:
    pass


_LONE_SURROGATE_NAME: Any = {'name': f'text{chr(0xd800)}'}


class _Linked(BaseModel):
    """A self-referencing model: registration accepts it in a task signature."""

    next: _Linked | None = None


def _registered_tasks() -> dict[str, Any]:
    """Registered tasks: the worker calls their wrapper, not a mock."""
    app = Horsies(AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql+psycopg://u:p@localhost:5432/db'),
        ),
    ))

    @app.task('surrogate.echoing')
    def returns_echoing() -> TaskResult[_EchoingModel, TaskError]:
        return TaskResult(ok=_LONE_SURROGATE_NAME)

    @app.task('surrogate.custom_error')
    def returns_custom_error() -> TaskResult[_CustomErrorModel, TaskError]:
        return TaskResult(ok=_LONE_SURROGATE_NAME)

    @app.task('circular.returned')
    def returns_itself() -> TaskResult[_Linked, TaskError]:
        node = _Linked()
        node.next = node
        return TaskResult(ok=node)

    @app.task('circular.error_data')
    def error_data_holds_itself() -> TaskResult[int, TaskError]:
        value: list[Any] = []
        value.append(value)
        return TaskResult(
            err=TaskError(error_code='MY_ERROR', message='m', data=value),
        )

    @app.task('deep.returned')
    def returns_a_long_chain() -> TaskResult[_Linked, TaskError]:
        node = _Linked()
        for _ in range(299):
            node = _Linked(next=node)
        return TaskResult(ok=node)

    @app.task('deep.error_data')
    def error_data_is_too_deep() -> TaskResult[int, TaskError]:
        value: list[Any] = []
        for _ in range(299):
            value = [value]
        return TaskResult(
            err=TaskError(error_code='MY_ERROR', message='m', data=value),
        )

    @app.task('bytes.error_data')
    def error_data_is_not_utf8() -> TaskResult[int, TaskError]:
        return TaskResult(
            err=TaskError(error_code='MY_ERROR', message='m', data=b'\xff'),
        )

    return {
        'error-data-not-utf8': error_data_is_not_utf8,
        'value-error': returns_echoing,
        'custom-error-context': returns_custom_error,
        'returned-model': returns_itself,
        'error-data': error_data_holds_itself,
        'deep-returned-model': returns_a_long_chain,
        'deep-error-data': error_data_is_too_deep,
    }


_LONE_SURROGATE_RETURNS: list[tuple[Any, Any]] = [
    (str, TaskResult(ok=f'text{chr(0xd800)}')),
    (dict[str, int], TaskResult(ok={f'key{chr(0xd800)}': 1})),
]


@pytest.mark.unit
class TestRunTaskEntryErrorPaths:
    """Error paths in _run_task_entry."""

    def test_ownership_blocks_execution(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """When ownership returns a tuple, _run_task_entry returns it as a
        run that stopped before its RUNNING update."""
        sentinel = (False, '', 'CLAIM_LOST')
        patches = _make_run_task_patches(ownership=sentinel)

        with _apply_patches(patches):
            result = _run_task_entry(**_run_entry_defaults)

        assert result == StoppedBeforeRunning(sentinel)

    def test_a_run_after_the_running_update_set_its_row_running(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """A run past the ownership confirm returns SetRunning, also when the
        task cannot be resolved."""
        patches = _make_run_task_patches(task_missing=True)

        with _apply_patches(patches):
            result = _run_task_entry(**_run_entry_defaults)

        assert isinstance(result, SetRunning)

    def test_task_resolution_failure(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Unknown task name → WORKER_RESOLUTION_ERROR."""
        patches = _make_run_task_patches(task_missing=True)

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        assert reason is not None and 'SerializationError' in reason

    def test_kwargs_json_lone_surrogate(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """An escape such as \\ud800 → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['kwargs_json'] = r'{"x":"\ud800"}'

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        assert reason is not None and 'lone surrogate' in reason
        assert 'WORKER_SERIALIZATION_ERROR' in payload

    def test_kwargs_json_past_the_depth_bound(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """A text past the bound → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['kwargs_json'] = (
            '{"x":' + '[' * 1_100 + ']' * 1_100 + '}'
        )

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        assert reason is not None and 'nested deeper than' in reason
        assert 'WORKER_SERIALIZATION_ERROR' in payload

    def test_kwargs_json_number_outside_finite_binary64(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """A literal such as 1e9999 → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['kwargs_json'] = '{"x":1e9999}'

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        assert reason is not None and 'finite binary64' in reason
        assert 'WORKER_SERIALIZATION_ERROR' in payload

    def test_kwargs_json_not_a_dict(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """kwargs_json is valid JSON but not a dict → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['kwargs_json'] = '[1, 2]'

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.task_ok_type = int
        mock_task.return_value = TaskResult(ok=42)
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)
        _run_entry_defaults['kwargs_json'] = None

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        parsed = _parse_task_result(payload)
        err = parsed.get('err') or parsed.get('error')
        assert err is None, (
            f'expected no error for NULL kwargs_json, got: {err}'
        )

    def test_a_wrapper_that_returns_no_task_result_is_a_defect(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """The wrapper turns each return into a `TaskResult`, so another
        value here is a defect in the library. It ends as TASK_EXCEPTION."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.task_ok_type = int
        mock_task.return_value = 42
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, _ = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        err = _parse_task_result(payload)['err']
        assert err['error_code'] == {
            '__builtin_task_code__': OperationalErrorCode.TASK_EXCEPTION.value,
        }
        assert 'task wrapper' in err['message']

    def test_task_raises_exception(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Task raising an exception → TASK_EXCEPTION with ok=True."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.side_effect = ValueError('boom')
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        assert reason is None  # exception path sets reason=None
        parsed = _parse_task_result(payload)
        err = parsed.get('err') or parsed.get('error')
        assert err['error_code'] == {'__builtin_task_code__': OperationalErrorCode.TASK_EXCEPTION.value}
        assert 'ValueError' in err['message']
        assert 'boom' in err['message']

    @pytest.mark.parametrize(
        ('ok_type', 'returned'),
        _LONE_SURROGATE_RETURNS,
        ids=[
            'ok-string', 'ok-key',
        ],
    )
    def test_a_lone_surrogate_in_a_return_value_ends_the_task(
        self,
        _run_entry_defaults: dict[str, Any],
        ok_type: Any,
        returned: Any,
    ) -> None:
        """The encode refuses the value, and the task ends with a stored
        WORKER_SERIALIZATION_ERROR: the worker writes it and does not retry
        the encode. The payload itself must encode. The text is the text of
        the encode refusal, not of the `dumps_json` guard after it."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.task_ok_type = ok_type
        mock_task.return_value = returned
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        assert reason is not None and 'holds a lone surrogate' in reason
        _ = payload.encode('utf-8')
        parsed = _parse_task_result(payload)
        assert parsed['err']['error_code'] == {
            '__builtin_task_code__': 'WORKER_SERIALIZATION_ERROR',
        }
        assert 'holds a lone surrogate' in parsed['err']['message']

    def test_a_validator_that_echoes_a_lone_surrogate_in_a_result_ends_the_task(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Pydantic cannot build the error text. The encode refuses the value
        through its catalog, so the code is WORKER_SERIALIZATION_ERROR and
        not TASK_EXCEPTION for a raw exception. The mock task skips the
        wrapper: a registered task stops earlier, in the return check."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.task_ok_type = _EchoingModel
        mock_task.return_value = TaskResult(ok={'name': f'text{chr(0xd800)}'})
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, _ = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        parsed = _parse_task_result(payload)
        assert parsed['err']['error_code'] == {
            '__builtin_task_code__': 'WORKER_SERIALIZATION_ERROR',
        }
        assert 'a validation error holds a lone surrogate' in parsed['err']['message']

    @pytest.mark.parametrize('form', ['value-error', 'custom-error-context'])
    def test_a_registered_task_stops_in_the_return_check(
        self,
        _run_entry_defaults: dict[str, Any],
        form: str,
    ) -> None:
        """The wrapper refuses the value before the worker encodes it."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_app.tasks.__getitem__ = MagicMock(
            return_value=_registered_tasks()[form],
        )

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        assert reason is None
        parsed = _parse_task_result(payload)
        assert parsed['err']['error_code'] == {
            '__builtin_task_code__': 'RETURN_TYPE_MISMATCH',
        }
        assert 'a validation error holds a lone surrogate' in parsed['err']['message']
        assert parsed['err']['exception'] is None

    def test_error_data_that_holds_itself_keeps_the_code(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_app.tasks.__getitem__ = MagicMock(
            return_value=_registered_tasks()['error-data'],
        )

        with _apply_patches(patches):
            _, payload, _ = wire_of(_run_task_entry(**_run_entry_defaults))

        err = _parse_task_result(payload)['err']
        assert err['error_code'] == 'MY_ERROR'
        assert 'data_refused' in err['data']

    @pytest.mark.parametrize('form', ['returned-model'])
    def test_a_value_that_holds_itself_ends_the_task(
        self,
        _run_entry_defaults: dict[str, Any],
        form: str,
    ) -> None:
        """The encode refuses the value through its catalog, so the code is
        WORKER_SERIALIZATION_ERROR and not TASK_EXCEPTION for a raw exception."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_app.tasks.__getitem__ = MagicMock(
            return_value=_registered_tasks()[form],
        )

        with _apply_patches(patches):
            ok, payload, _ = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        parsed = _parse_task_result(payload)
        assert parsed['err']['error_code'] == {
            '__builtin_task_code__': 'WORKER_SERIALIZATION_ERROR',
        }
        assert 'Circular reference detected (id repeated)' in parsed['err']['message']

    def test_error_data_that_is_too_deep_keeps_the_code(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_app.tasks.__getitem__ = MagicMock(
            return_value=_registered_tasks()['deep-error-data'],
        )

        with _apply_patches(patches):
            _, payload, _ = wire_of(_run_task_entry(**_run_entry_defaults))

        err = _parse_task_result(payload)['err']
        assert err['error_code'] == 'MY_ERROR'
        assert 'data_refused' in err['data']

    @pytest.mark.parametrize('form', ['deep-returned-model'])
    def test_a_value_that_is_too_deep_ends_the_task(
        self,
        _run_entry_defaults: dict[str, Any],
        form: str,
    ) -> None:
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_app.tasks.__getitem__ = MagicMock(
            return_value=_registered_tasks()[form],
        )

        with _apply_patches(patches):
            ok, payload, _ = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        parsed = _parse_task_result(payload)
        assert parsed['err']['error_code'] == {
            '__builtin_task_code__': 'WORKER_SERIALIZATION_ERROR',
        }
        assert (
            'Circular reference detected (depth exceeded)'
            in parsed['err']['message']
        )

    def test_bytes_that_are_not_utf8_in_error_data_keep_the_code(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """`TaskError.data` is the one task value that registration does not
        type. The refusal holds no byte value."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_app.tasks.__getitem__ = MagicMock(
            return_value=_registered_tasks()['error-data-not-utf8'],
        )

        with _apply_patches(patches):
            ok, payload, _ = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        parsed = _parse_task_result(payload)
        assert parsed['err']['error_code'] == 'MY_ERROR'
        assert 'data_refused' in parsed['err']['data']
        assert '0xff' not in payload

    def test_a_validator_text_with_a_lone_surrogate_fails_the_kwargs_decode(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task._original_fn = _constant_surrogate_task
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)
        _run_entry_defaults['kwargs_json'] = json.dumps({'x': {'name': 'plain'}})

        with _apply_patches(patches):
            ok, payload, _ = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        parsed = _parse_task_result(payload)
        assert parsed['err']['error_code'] == {
            '__builtin_task_code__': 'WORKER_SERIALIZATION_ERROR',
        }
        assert parsed['err']['message'] == (
            'Failed to decode kwargs for my_task: x: a validation error holds '
            'a lone surrogate, which UTF-8 cannot encode'
        )

    def test_a_dict_key_without_a_json_form_in_error_data_ends_the_task(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """`TaskError.data` is the one task value that registration does not
        type. The encode refuses the key through its catalog, so the code is
        WORKER_SERIALIZATION_ERROR and not TASK_EXCEPTION for a raw exception."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.task_ok_type = int
        mock_task.return_value = TaskResult(
            err=TaskError(
                error_code='MY_ERROR', message='m', data={frozenset({1}): 1},
            ),
        )
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, _ = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        parsed = _parse_task_result(payload)
        assert parsed['err']['error_code'] == 'MY_ERROR'
        assert 'data_refused' in parsed['err']['data']

    def test_a_lone_surrogate_in_a_raised_exception_keeps_the_code(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """An error reports a failure, so its text is escaped and not refused."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.task_ok_type = str
        mock_task.side_effect = ValueError(f'text{chr(0xd800)}')
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, _ = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        _ = payload.encode('utf-8')
        err = _parse_task_result(payload)['err']
        assert err['error_code'] == {
            '__builtin_task_code__': OperationalErrorCode.TASK_EXCEPTION.value,
        }

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
        real cause (encoder mismatch). The codec now raises
        ``ValueValidationError`` for it, and both encode-time catches
        name that type.
        """
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.task_ok_type = int

        def _identity(x: Any) -> Any:
            return x

        # Task wraps the callable as ok inside a TaskResult — same
        # shape as ``unserializable_result_task`` in tests/e2e/tasks/basic.py.
        mock_task.return_value = TaskResult(ok=_identity)
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.return_value = TaskResult(ok='success')
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.return_value = TaskResult(
            err=TaskError(error_code='CUSTOM_ERR', message='failed'),
        )
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.task_ok_type = str
        mock_task.return_value = TaskResult(ok='data')
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches), patch(
            'horsies.core.worker.child_runner.dumps_json',
            return_value=Err(SerializationError('cannot serialize')),
        ):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
        mock_task.task_body_kind = TaskBodyKind.DEF
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
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
        mock_task.task_body_kind = TaskBodyKind.DEF
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
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
        mock_task.task_body_kind = TaskBodyKind.DEF
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
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
        ):
            result = _confirm_ownership_and_set_running('task-1', 'worker-A')

        assert result == (False, '', OutcomeCode.TASK_EXPIRED.value)
        assert conn.commits == 1
        assert conn.rollbacks == 0
        assert not any('UPDATE horsies_workflow_tasks' in sql for sql, _ in cursor.queries)
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
        ):
            result = _confirm_ownership_and_set_running('task-1', 'worker-A')

        assert result == (False, '', 'CLAIM_LOST')
        assert conn.commits == 0
        assert conn.rollbacks == 1
        assert not any('UPDATE horsies_workflow_tasks' in sql for sql, _ in cursor.queries)


# ===================================================================
# M2. Regression: workflow_task RUNNING sync failure aborts execution
# ===================================================================


@pytest.mark.unit
class TestWorkflowTaskRunningSyncFailureAborts:
    """Workflow RUNNING sync failures abort before user code starts."""

    def test_running_sync_failure_returns_preexec_abort(self) -> None:
        """Task rolls back when no waiting or RUNNING node is linked."""
        from horsies.core.worker.child_runner import (
            _confirm_ownership_and_set_running,
        )

        cursor = _SequenceCursor([_FakeRow(id='task-1'), (False,)])
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
        """When a node is linked, returns None (proceed)."""
        from horsies.core.worker.child_runner import (
            _confirm_ownership_and_set_running,
        )

        cursor = _SequenceCursor([_FakeRow(id='task-1'), (True,)])
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
        """The first runner heartbeat is part of the RUNNING statement.

        A row must never be observable as RUNNING without heartbeat coverage,
        and the heartbeat must not cost a round trip of its own.
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
        sql, params = heartbeat_queries[0]
        assert "SET status = 'RUNNING'" in sql
        assert params[-3].startswith('worker-A:')
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
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task._original_fn = _takes_upstream
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
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

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
        mock_task.task_body_kind = TaskBodyKind.DEF
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
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        assert reason is None

    # (test_workflow_ctx_results_by_id_task_result_from_json_error removed:
    # the patched-out ``task_result_from_json`` helper no longer exists
    # in the strict-serde rebuild. Per-node decode failure now folds
    # into a RESULT_DESERIALIZATION_ERROR sentinel — covered at the
    # model layer in test_workflow_handle_decode.py.)

# ===================================================================
# P. The child writes no runner heartbeat
# ===================================================================


@pytest.mark.unit
class TestChildWritesNoRunnerHeartbeat:
    """The worker process writes the runner heartbeats of a held task.

    The child writes only the first one, inside the RUNNING transition,
    which is patched out here.
    """

    def test_no_heartbeat_thread_and_no_heartbeat_statement(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        names_during_run: list[str] = []

        def body() -> TaskResult[str, TaskError]:
            # Longer than two of the beat intervals the child used to have.
            time.sleep(0.3)
            names_during_run.extend(thread.name for thread in threading.enumerate())
            return TaskResult(ok='done')

        def call(*args: Any, **kwargs: Any) -> TaskResult[str, TaskError]:
            return body()

        task = MagicMock(side_effect=call)
        task.task_body_kind = TaskBodyKind.DEF
        task.task_ok_type = str
        task._original_fn = body
        app = MagicMock()
        app.tasks.__getitem__ = MagicMock(return_value=task)
        cursor = _FakeCursor()
        patches: dict[str, Any] = {
            'horsies.core.worker.child_runner._confirm_ownership_and_set_running': MagicMock(
                return_value=None,
            ),
            'horsies.core.worker.child_runner.get_current_app': MagicMock(
                return_value=app,
            ),
            'horsies.core.worker.child_runner._get_worker_pool': MagicMock(
                return_value=_FakePool(_FakeConn(cursor)),
            ),
        }

        with _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert (ok, reason) == (True, None)
        assert _parse_task_result(payload)['ok'] == 'done'
        assert names_during_run
        assert not any(name.startswith('heartbeat') for name in names_during_run)
        statements = [' '.join(sql.split()) for sql, _ in cursor.queries]
        assert not any('horsies_heartbeats' in sql for sql in statements)
        # The child writes the marker after the run has ended, with the work
        # that the task left on its loop (child_main._report_run).
        assert not any('SET finalizing_at = NOW()' in sql for sql in statements)


# ===================================================================
# Q. Refusal text in the child holds no input values
# ===================================================================


_INPUT_MARKER = 'child-marker-8b2f'


def _typed_count_task(*, count: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=count)


@pytest.mark.unit
class TestRunTaskEntryRefusalTextHoldsNoInput:
    """A decode or encode refusal stores the field, the message and the type,
    never the refused value."""

    def _assert_no_marker(
        self,
        payload: str,
        reason: str | None,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        assert reason is not None and 'SerializationError' in reason
        assert _INPUT_MARKER not in payload
        assert _INPUT_MARKER not in reason
        assert _INPUT_MARKER not in caplog.text

    def test_kwargs_decode_refusal(
        self,
        _run_entry_defaults: dict[str, Any],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task._original_fn = _typed_count_task
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)
        _run_entry_defaults['kwargs_json'] = json.dumps({'count': _INPUT_MARKER})

        with caplog.at_level(logging.DEBUG), _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        self._assert_no_marker(payload, reason, caplog)
        assert 'count: Input should be a valid integer' in payload

    def test_args_from_decode_refusal(
        self,
        _run_entry_defaults: dict[str, Any],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        source_task = MagicMock()
        source_task.task_ok_type = int
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.task_ok_type = str

        def _task_lookup(name: str) -> Any:
            return source_task if name == 'upstream_task' else mock_task

        mock_app.tasks.__getitem__ = MagicMock(side_effect=_task_lookup)
        mock_app.tasks.get = MagicMock(side_effect=_task_lookup)
        _run_entry_defaults['kwargs_json'] = json.dumps({
            'upstream': {
                '__h_taskresult_envelope__': True,
                'source_task_name': 'upstream_task',
                'inner': {'__h_task_result__': True, 'ok': _INPUT_MARKER, 'err': None},
            },
        })

        with caplog.at_level(logging.DEBUG), _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        self._assert_no_marker(payload, reason, caplog)

    def test_task_call_raises_validation_error(
        self,
        _run_entry_defaults: dict[str, Any],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from pydantic import TypeAdapter, ValidationError

        with pytest.raises(ValidationError) as refused:
            TypeAdapter(int).validate_python(_INPUT_MARKER)
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock(side_effect=refused.value)
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with caplog.at_level(logging.DEBUG), _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        assert reason is None
        assert _INPUT_MARKER not in payload
        assert _INPUT_MARKER not in caplog.text
        assert 'ValidationError: Input should be a valid integer' in payload

    def test_result_encode_refusal(
        self,
        _run_entry_defaults: dict[str, Any],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.task_ok_type = int
        mock_task.return_value = TaskResult(ok=_INPUT_MARKER)
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with caplog.at_level(logging.DEBUG), _apply_patches(patches):
            ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        self._assert_no_marker(payload, reason, caplog)


@pytest.mark.unit
class TestRunTaskEntryStoredErrorRefusal:
    """An injected err slot that does not parse gives its cause, not a raise."""

    def test_args_from_err_slot_gives_a_serialization_error(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        source_task = MagicMock()
        source_task.task_ok_type = str
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task._original_fn = _takes_upstream
        mock_task.task_ok_type = str

        def _task_lookup(name: str) -> Any:
            return source_task if name == 'upstream_task' else mock_task

        mock_app.tasks.__getitem__ = MagicMock(side_effect=_task_lookup)
        mock_app.tasks.get = MagicMock(side_effect=_task_lookup)
        _run_entry_defaults['kwargs_json'] = json.dumps({
            'upstream': {
                '__h_taskresult_envelope__': True,
                'source_task_name': 'upstream_task',
                'inner': {
                    '__h_task_result__': True,
                    'ok': None,
                    'err': {'nope': 1},
                },
            },
        })

        with _apply_patches(patches):
            ok, payload, _ = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        err = _parse_task_result(payload)['err']
        assert err['error_code'] == {
            '__builtin_task_code__': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value,
        }
        assert 'nope' in err['message']
        mock_task.assert_not_called()

    def test_workflow_ctx_err_slot_gives_a_stored_data_entry(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        def _task_with_ctx(workflow_ctx: Any = None) -> TaskResult[str, TaskError]:
            return TaskResult(ok='done')

        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.task_ok_type = str
        mock_task.return_value = TaskResult(ok='done')
        mock_task._original_fn = _task_with_ctx
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)
        mock_app.tasks.get = MagicMock(return_value=mock_task)
        _run_entry_defaults['kwargs_json'] = json.dumps({
            '__h_workflow_ctx__': {
                'workflow_id': 'wf-1',
                'task_index': 0,
                'task_name': 'my_task',
                'results_by_id': {
                    'node_a': {
                        '__h_task_result__': True,
                        'ok': None,
                        'err': {'nope': 1},
                    },
                },
                'task_name_by_id': {'node_a': 'upstream_task'},
                'summaries_by_id': {},
            },
        })

        with _apply_patches(patches):
            ok, _, reason = wire_of(_run_task_entry(**_run_entry_defaults))

        assert ok is True
        assert reason is None
        injected = mock_task.call_args.kwargs['workflow_ctx']
        entry = injected._results_by_id['node_a'].unwrap_err()
        assert entry.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert 'nope' in entry.message


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


_REFUSALS: list[tuple[TimeoutRefusal, str]] = [
    (OptionsUnreadable(reason='stored task_options do not parse'), 'task_options'),
    (
        TimeoutValueInvalid(reason='stored timeout_ms is above the limit'),
        'timeout_ms',
    ),
    (
        CancelGraceInvalid(reason='stored cancel_grace_ms is below 100 ms'),
        'cancel_grace_ms',
    ),
]


@pytest.mark.unit
class TestRunTaskEntryStoredTimeout:
    """A stored timeout reaches the child as `StoredTimeout`."""

    @pytest.mark.parametrize(('refusal', 'field'), _REFUSALS)
    def test_a_refusal_fails_the_task_after_running_and_before_resolve(
        self,
        _run_entry_defaults: dict[str, Any],
        refusal: TimeoutRefusal,
        field: str,
    ) -> None:
        patches = _make_run_task_patches()
        confirm = patches[
            'horsies.core.worker.child_runner._confirm_ownership_and_set_running'
        ]
        app_lookup = patches['horsies.core.worker.child_runner.get_current_app']

        with _apply_patches(patches):
            ok, payload, reason = wire_of(
                _run_task_entry(
                    **_run_entry_defaults,
                    timeout=refusal,
                )
            )

        assert ok is True
        assert reason == f'StoredTimeoutRefused: {refusal.reason}'
        confirm.assert_called_once()
        app_lookup.assert_not_called()
        err = _parse_task_result(payload)['err']
        assert err['error_code'] == {
            '__builtin_task_code__': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value,
        }
        assert err['message'] == refusal.reason
        assert err['data']['field'] == field

    def test_a_bad_value_names_the_bounds(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        patches = _make_run_task_patches()

        with _apply_patches(patches):
            _, payload, _ = wire_of(
                _run_task_entry(
                    **_run_entry_defaults,
                    timeout=TimeoutValueInvalid(reason='stored timeout_ms is a bool'),
                )
            )

        data = _parse_task_result(payload)['err']['data']
        assert data == {
            'task_name': 'my_task',
            'field': 'timeout_ms',
            'min_ms': MIN_TASK_TIMEOUT_MS,
            'max_ms': MAX_TASK_TIMEOUT_MS,
        }

    def test_a_bad_grace_names_its_bounds(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        patches = _make_run_task_patches()

        with _apply_patches(patches):
            _, payload, _ = wire_of(
                _run_task_entry(
                    **_run_entry_defaults,
                    timeout=CancelGraceInvalid(
                        reason='stored cancel_grace_ms is absent'
                    ),
                )
            )

        data = _parse_task_result(payload)['err']['data']
        assert data == {
            'task_name': 'my_task',
            'field': 'cancel_grace_ms',
            'min_ms': MIN_CANCEL_GRACE_MS,
            'max_ms': MAX_CANCEL_GRACE_MS,
        }


# ===================================================================
# K. Async task calls in _run_task_entry
# ===================================================================


def _ok_value(payload: str, ok_type: Any) -> Any:
    return decode_task_result(loads_json(payload).unwrap(), ok_type).unwrap()


@pytest.mark.unit
class TestRunTaskEntryCoroutineCall:
    """The call of an `async def` task returns a coroutine."""

    def test_the_coroutine_runs_on_the_given_task_loop(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        task_loop = TaskLoop()
        task_loop.open().unwrap()
        _run_entry_defaults['task_loop'] = task_loop
        seen: list[asyncio.AbstractEventLoop] = []

        async def body() -> TaskResult[int, TaskError]:
            seen.append(asyncio.get_running_loop())
            return TaskResult(ok=5)

        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock(side_effect=lambda: body())
        mock_task.task_body_kind = TaskBodyKind.ASYNC_DEF
        mock_task.task_ok_type = int
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        try:
            with _apply_patches(patches):
                ok, payload, reason = wire_of(_run_task_entry(**_run_entry_defaults))
            loop_state = task_loop.state
        finally:
            task_loop.close()

        assert (ok, reason) == (True, None)
        assert _ok_value(payload, int) == 5
        assert isinstance(loop_state, LoopOpen)
        assert seen == [loop_state.loop]

