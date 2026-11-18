"""Unit tests for child_runner.py — full coverage, on the version-2 child.

The child takes its work from version-2 frames (``tests.fixtures.core_frames``)
and ends a run with a ``RunReturn``: the result envelope after the RUNNING
update, or the reason of a stop before it.

Sections:
A. _serialization_error_response
B. _debug_imports_log
C. The boot of a child (child_main.boot_app)
E. (removed: the start reads the workflow in its own statement)
F. The stop before start of a workflow task
I. _run_task_entry error paths
J. Workflow injection deserialization errors in _run_task_entry
K. Async task calls in _run_task_entry
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Final, Generator
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, SecretStr, field_validator
from pydantic_core import PydanticCustomError

import horsies._core as core
from horsies.core.app import Horsies
from horsies.core.codec.json_io import SerializationError, loads_json
from horsies.core.codec.typed import decode_task_result
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    MAX_CANCEL_GRACE_MS,
    MAX_TASK_TIMEOUT_MS,
    MIN_CANCEL_GRACE_MS,
    MIN_TASK_TIMEOUT_MS,
    OperationalErrorCode,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import Err, Ok
from horsies.core.worker.stored_timeout import (
    CancelGraceInvalid,
    OptionsUnreadable,
    TimeoutRefusal,
    TimeoutValueInvalid,
)
from horsies.core.task_decorator import TaskBodyKind
from horsies.core.models.boot_failure import AppLoadFailed
from horsies.core.worker.task_loop import AsyncTaskPresence, LoopOpen, TaskLoop
from horsies.core.worker.child_frames import BootFailedFrame
from horsies.core.worker.child_main import boot_app
from horsies.core.worker.child_runner import (
    RunTheTask,
    _debug_imports_log,
    _discard_inherited_broker,  # pyright: ignore[reportPrivateUsage]
    _run_task_entry,
    _serialization_error_response,
    attempt_elapsed_ms,
)
from horsies.core.worker.run_return import SetRunning, StoppedBeforeRunning
from tests.fixtures.core_frames import init_frame, isolated_kind, pool
from tests.fixtures.run_return import envelope_of

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)
TASK_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e02'


# ---------------------------------------------------------------------------
# Shared fakes
# ---------------------------------------------------------------------------


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``, which has no Python
    constructor: a class pattern matches it, and ``fields`` are set as its
    attributes."""
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


# The most time that a miss path of these tests may report as spent in its
# start attempt: the fakes answer at once.
_MOST_ELAPSED_MS = 5_000


def _attempt_elapsed_ms(arguments: dict[str, object]) -> dict[str, object]:
    """The arguments of a miss path call without its ``attempt_elapsed_ms``,
    after a check that it is the whole milliseconds of a start attempt that
    began right before the call."""
    rest = dict(arguments)
    elapsed = rest.pop('attempt_elapsed_ms')
    assert isinstance(elapsed, int)
    assert 0 <= elapsed <= _MOST_ELAPSED_MS
    return rest


def _ended_row(kind: str) -> MagicMock:
    """A ``TaskRowEnded`` of the task of these tests, with the row CLAIMED by
    ``worker-A``."""
    return _record(
        core.TaskRowEnded,
        task_id=uuid.UUID(TASK_ID),
        terminal_at=CLAIMED_AT,
        kind=kind,
        observed_status='CLAIMED',
        observed_worker_id='worker-A',
        observed_claimed_at=CLAIMED_AT,
    )


def _refusal(record_class: type) -> MagicMock:
    """A refusal record of the task of these tests, with the observed row."""
    return _record(
        record_class,
        task_id=uuid.UUID(TASK_ID),
        terminal_at=CLAIMED_AT,
        kind='EXPIRE_CLAIMED',
        guard='DEADLINE',
        observed_status='CLAIMED',
        observed_worker_id='worker-B',
        observed_claimed_at=CLAIMED_AT,
    )


class _FakeChildHandle:
    """A stand-in for ``CoreChildHandle``: each method answers the next
    record of its queue, and each call is kept with its arguments, in
    order."""

    def __init__(
        self,
        *,
        starts: list[Any],
        stops: list[Any] | None = None,
        expiries: list[Any] | None = None,
    ) -> None:
        self._starts = starts
        self._stops = stops or []
        self._expiries = expiries or []
        self.calls: list[tuple[str, uuid.UUID, dict[str, object]]] = []

    def start_attempt(self, task_id: uuid.UUID, **kwargs: object) -> Any:
        self.calls.append(('start_attempt', task_id, kwargs))
        return self._starts.pop(0)

    def stop_workflow_task_before_start(self, task_id: uuid.UUID, **kwargs: object) -> Any:
        self.calls.append(('stop_workflow_task_before_start', task_id, kwargs))
        return self._stops.pop(0)

    def expire_owned_claim(self, task_id: uuid.UUID, **kwargs: object) -> Any:
        self.calls.append(('expire_owned_claim', task_id, kwargs))
        return self._expiries.pop(0)

    def names(self) -> list[str]:
        return [name for name, _, _ in self.calls]


# ---------------------------------------------------------------------------
# Helper to parse a TaskResult from serialized JSON produced by child_runner
# ---------------------------------------------------------------------------


def _parse_task_result(json_str: str) -> dict[str, Any]:
    """Parse a serialized TaskResult JSON into a dict for assertions."""
    return json.loads(json_str)  # type: ignore[no-any-return]


_SERIALIZATION_ERROR: Final = {
    '__builtin_task_code__': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value,
}
"""The stored error code of ``WORKER_SERIALIZATION_ERROR``."""


def _error_of(payload: str) -> dict[str, Any]:
    """The task error of a result envelope; a success fails the test."""
    err: object = _parse_task_result(payload).get('err')
    assert isinstance(err, dict), payload
    return err  # pyright: ignore[reportUnknownVariableType]


def _error_code(payload: str) -> object:
    """The stored error code of the task error of a result envelope."""
    return _error_of(payload)['error_code']


def _error_message(payload: str) -> str:
    """The message of the task error of a result envelope."""
    message = _error_of(payload)['message']
    assert isinstance(message, str), payload
    return message


# ===================================================================
# A. _serialization_error_response
# ===================================================================


@pytest.mark.unit
class TestSerializationErrorResponse:
    """_serialization_error_response builds the error envelope."""

    def test_returns_the_error_envelope_with_the_message(self) -> None:
        error = SerializationError('bad json input')
        payload = _serialization_error_response('my_task', error).text

        assert _error_code(payload) == _SERIALIZATION_ERROR
        assert _error_message(payload) == 'bad json input'

    def test_includes_task_name_in_data(self) -> None:
        error = SerializationError('corrupt')
        payload = _serialization_error_response('process_order', error).text

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
# C. The boot of a child (child_main.boot_app)
# ===================================================================


_INIT_POOL: Final = pool(min_size=0, max_size=1, check=False)
"""The pool of the child kind of the init frames of these tests."""


def _boot_init_frame(
    app_locator: str,
    imports: tuple[str, ...],
) -> core.InitFrame:
    """The init frame of an isolated child of ``app_locator`` with
    ``imports``, a task-send pool of 2 and the pool ``_INIT_POOL``: the
    argument of ``boot_app``."""
    return init_frame(
        app_locator=app_locator,
        imports=imports,
        log_level='info',
        child_kind=isolated_kind(task_send_pool=2, child_pool=_INIT_POOL),
    )


def _app_load_failed(detail: str) -> Err[BootFailedFrame]:
    """The boot of a child whose app import raised ``RuntimeError(detail)``."""
    return Err(
        BootFailedFrame(cause=AppLoadFailed(), error_name='RuntimeError', detail=detail),
    )


@pytest.mark.unit
class TestChildBoot:
    """``boot_app`` wires up the child process environment from its init
    frame: it imports the app and its task modules
    (``import_app_for_child``), then runs the per-child start steps
    (``finish_child_start``). A failure of the import ends the boot with a
    named ``BootFailedFrame``, before the core handle of the child is built.

    ``restore_parent_state`` is replaced in each test: it sets
    ``sys.path``, ``sys.argv`` and the process name of the process."""

    def _make_patches(
        self,
        *,
        app_tasks: dict[str, Any] | None = None,
        discovered_modules: list[str] | None = None,
        suppress_raises: bool = False,
        discovered_raises: bool = False,
        keys_list_raises: bool = False,
    ) -> dict[str, Any]:
        """Build a dict of mock objects for all dependencies of the boot."""
        mock_app = MagicMock()
        if app_tasks is not None:
            mock_app.tasks.keys_list.return_value = list(app_tasks.keys())
            mock_app.tasks.keys.return_value = app_tasks.keys()
            mock_app.tasks.values.return_value = list(app_tasks.values())
        else:
            mock_app.tasks.keys_list.return_value = []
            mock_app.tasks.keys.return_value = {}.keys()
            mock_app.tasks.values.return_value = []

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

    @patch('horsies.core.worker.child_main.restore_parent_state')
    @patch('horsies.core.worker.child_runner._initialize_child_core_handle')
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
        mock_init_core_handle: MagicMock,
        mock_restore: MagicMock,
    ) -> None:
        """The boot hands the located app on as the current app of the
        child, and builds the core handle from the init frame."""
        mocks = self._make_patches(app_tasks={'add': True})
        mock_locate_app.return_value = mocks['app']

        init = _boot_init_frame('mymod:app', ('extra_mod',))

        booted = boot_app(init)

        assert booted == Ok(AsyncTaskPresence.NO_ASYNC_TASK)
        mock_restore.assert_called_once_with(init)
        mock_set_level.assert_called_once_with(20)
        mock_locate_app.assert_called_once_with('mymod:app')
        mock_set_current.assert_called_once_with(mocks['app'])
        mock_import_module.assert_any_call('extra_mod')
        mocks['app'].set_task_send_pool.assert_called_once_with(2)
        mock_init_core_handle.assert_called_once_with(init)

    @patch('horsies.core.worker.child_main.restore_parent_state')
    @patch('horsies.core.worker.child_runner._initialize_child_core_handle')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.logging.configure_logging')
    def test_the_child_pool_goes_to_the_core_handle_with_the_init_frame(
        self,
        mock_set_level: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_core_handle: MagicMock,
        mock_restore: MagicMock,
    ) -> None:
        """The child opens no pool of its own: the bounds of the pool of its
        kind and the connection settings reach the core handle in the init
        frame (``CoreChildHandle.from_init``)."""
        mocks = self._make_patches(app_tasks={'add': True})
        mock_locate_app.return_value = mocks['app']
        init = _boot_init_frame('mymod:app', ())

        _ = boot_app(init)

        mock_init_core_handle.assert_called_once_with(init)
        handed: core.InitFrame = mock_init_core_handle.call_args.args[0]
        child_pool = handed.child_kind.pool
        assert (child_pool.min_size, child_pool.max_size, child_pool.check) == (0, 1, False)

    @patch('horsies.core.worker.child_main.restore_parent_state')
    @patch('horsies.core.worker.child_runner._initialize_child_core_handle')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.logging.configure_logging')
    def test_the_boot_discards_the_inherited_broker(
        self,
        mock_set_level: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_core_handle: MagicMock,
        mock_restore: MagicMock,
    ) -> None:
        from horsies.core.brokers.postgres import PostgresBroker

        mocks = self._make_patches(app_tasks={'add': True})
        inherited_broker = MagicMock(spec=PostgresBroker)
        mocks['app']._broker = inherited_broker
        mock_locate_app.return_value = mocks['app']

        _ = boot_app(_boot_init_frame('mymod:app', ()))

        # Dropped, not closed: the boot calls no method of the import-time broker.
        assert inherited_broker.method_calls == []
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
                    database_url=SecretStr('postgresql://u:p@localhost/db'),
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

    @patch('horsies.core.worker.child_main.restore_parent_state')
    @patch('horsies.core.worker.child_runner._initialize_child_core_handle')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.logging.configure_logging')
    def test_a_failed_send_suppression_fails_the_boot(
        self,
        mock_set_level: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_core_handle: MagicMock,
        mock_restore: MagicMock,
    ) -> None:
        mocks = self._make_patches(suppress_raises=True)
        mock_locate_app.return_value = mocks['app']

        booted = boot_app(_boot_init_frame('mymod:app', ()))

        assert booted == _app_load_failed('suppress failed')
        mock_init_core_handle.assert_not_called()

    @patch('horsies.core.worker.child_main.restore_parent_state')
    @patch('horsies.core.worker.child_runner._initialize_child_core_handle')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.logging.configure_logging')
    def test_a_failed_module_discovery_fails_the_boot(
        self,
        mock_set_level: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_core_handle: MagicMock,
        mock_restore: MagicMock,
    ) -> None:
        mocks = self._make_patches(discovered_raises=True)
        mock_locate_app.return_value = mocks['app']

        booted = boot_app(_boot_init_frame('mymod:app', ()))

        assert booted == _app_load_failed('discovery failed')
        mock_init_core_handle.assert_not_called()

    @patch('horsies.core.worker.child_main.restore_parent_state')
    @patch('horsies.core.worker.child_runner._initialize_child_core_handle')
    @patch('horsies.core.worker.child_runner.set_current_app')
    @patch('horsies.core.worker.child_runner._locate_app')
    @patch('horsies.core.worker.child_runner.import_by_path')
    @patch('horsies.core.logging.configure_logging')
    def test_a_failed_task_listing_fails_the_boot(
        self,
        mock_set_level: MagicMock,
        mock_import_path: MagicMock,
        mock_locate_app: MagicMock,
        mock_set_current: MagicMock,
        mock_init_core_handle: MagicMock,
        mock_restore: MagicMock,
    ) -> None:
        mocks = self._make_patches(keys_list_raises=True, app_tasks={'t1': 1})
        mock_locate_app.return_value = mocks['app']

        booted = boot_app(_boot_init_frame('mymod:app', ()))

        assert booted == _app_load_failed('keys_list failed')

    @patch('horsies.core.worker.child_main.restore_parent_state')
    @patch('horsies.core.worker.child_runner._initialize_child_core_handle')
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
        mock_init_core_handle: MagicMock,
        mock_restore: MagicMock,
    ) -> None:
        mocks = self._make_patches()
        mock_locate_app.return_value = mocks['app']

        _ = boot_app(_boot_init_frame('/tmp/myapp.py:app', ('/tmp/tasks.py',)))
        # /tmp/tasks.py should be imported via import_by_path, not import_module
        assert mock_import_path.called
        # samefile was checked to avoid re-importing the app module
        mock_samefile.assert_called()

    @patch('horsies.core.worker.child_main.restore_parent_state')
    @patch('horsies.core.worker.child_runner._initialize_child_core_handle')
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
        mock_init_core_handle: MagicMock,
        mock_restore: MagicMock,
    ) -> None:
        """When import file matches app file, import_by_path is NOT called."""
        mocks = self._make_patches()
        mock_locate_app.return_value = mocks['app']

        _ = boot_app(_boot_init_frame('/tmp/myapp.py:app', ('/tmp/myapp.py',)))
        # import_by_path should NOT be called because samefile returns True
        mock_import_path.assert_not_called()


# ===================================================================
# F. The stop before start of a workflow task: one core call
# ===================================================================


def _missed(reason: str) -> MagicMock:
    return _record(core.AttemptMissed, task_id=uuid.UUID(TASK_ID), reason=reason)


def _stopped(outcome: str, move_outcome: str | None) -> MagicMock:
    return _record(
        core.StoppedBeforeStart,
        task_id=uuid.UUID(TASK_ID),
        outcome=outcome,
        move_outcome=move_outcome,
    )


def _start_with(
    handle: _FakeChildHandle,
    *,
    orphan_self_heal: bool = True,
    start_check: Any = None,
) -> Any:
    """Run the sync start of the task of these tests on ``handle``."""
    from horsies.core.worker.child_runner import (
        _confirm_ownership_and_set_running,
        start_always_allowed,
    )

    with patch(
        'horsies.core.worker.child_runner._get_child_core_handle',
        return_value=handle,
    ):
        return _confirm_ownership_and_set_running(
            TASK_ID,
            'worker-A',
            claimed_at=CLAIMED_AT,
            orphan_self_heal=orphan_self_heal,
            start_check=start_check or start_always_allowed,
        )


@pytest.mark.unit
class TestDiscardInheritedBroker:
    """``_discard_inherited_broker`` drops the broker that importing the app
    built in the child."""

    def test_without_a_broker_does_nothing(self) -> None:
        app = Horsies(AppConfig(broker=PostgresConfig(
            database_url=SecretStr('postgresql://u:p@localhost:5432/db'),
        )))
        app._broker = None  # pyright: ignore[reportPrivateUsage]

        _discard_inherited_broker(app)

        assert app._broker is None  # pyright: ignore[reportPrivateUsage]


@pytest.mark.unit
class TestStopBeforeStart:
    """A miss of a workflow task sends one
    ``horsies_workflow_task_stop_before_start``; its step gives the stop of
    the run."""

    def test_a_cancelled_workflow_stops_the_task_in_one_call(self) -> None:
        handle = _FakeChildHandle(
            starts=[_missed('WORKFLOW_CANCELLED')],
            stops=[_stopped('CANCELLED', 'APPLIED')],
        )

        result = _start_with(handle)

        assert result == StoppedBeforeRunning('WORKFLOW_STOPPED')
        assert handle.names() == ['start_attempt', 'stop_workflow_task_before_start']

    def test_a_paused_workflow_stops_the_task_in_one_call(self) -> None:
        handle = _FakeChildHandle(
            starts=[_missed('WORKFLOW_PAUSED')],
            stops=[_stopped('ABANDONED', 'APPLIED')],
        )

        result = _start_with(handle)

        assert result == StoppedBeforeRunning('WORKFLOW_STOPPED')
        assert handle.names() == ['start_attempt', 'stop_workflow_task_before_start']

    def test_a_node_that_is_not_linked_with_the_self_heal_off_fails_the_workflow_check(
        self,
    ) -> None:
        handle = _FakeChildHandle(
            starts=[_missed('NODE_NOT_LINKED')],
            stops=[_stopped('NODE_NOT_LINKED', None)],
        )

        result = _start_with(handle, orphan_self_heal=False)

        assert result == StoppedBeforeRunning('NODE_NOT_LINKED')
        assert handle.calls[1][2]['orphan_self_heal'] is False

    def test_a_node_that_is_not_linked_with_the_self_heal_on_is_cancelled(self) -> None:
        handle = _FakeChildHandle(
            starts=[_missed('NODE_NOT_LINKED')],
            stops=[_stopped('ORPHAN_CANCELLED', 'APPLIED')],
        )

        result = _start_with(handle, orphan_self_heal=True)

        assert result == StoppedBeforeRunning('ORPHAN_CANCELLED')
        assert handle.calls[1][2]['orphan_self_heal'] is True

    @pytest.mark.parametrize('reason', ['WORKFLOW_PAUSED', 'WORKFLOW_CANCELLED', 'NODE_NOT_LINKED'])
    def test_each_stop_carries_the_claim_generation_fence(self, reason: str) -> None:
        """The stop fences on (worker_id, claimed_at). Without the pair, a row
        requeued and re-claimed since this child was handed its dispatch is
        still CLAIMED and would be ended under the new claim generation."""
        generation = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
        handle = _FakeChildHandle(
            starts=[_missed(reason)],
            stops=[_stopped('CANCELLED', 'LOST_CLAIM')],
        )

        with patch(
            'horsies.core.worker.child_runner._get_child_core_handle',
            return_value=handle,
        ):
            from horsies.core.worker.child_runner import _confirm_ownership_and_set_running

            _ = _confirm_ownership_and_set_running(
                TASK_ID,
                'worker-7',
                claimed_at=generation,
                orphan_self_heal=True,
            )

        name, task_id, arguments = handle.calls[1]
        assert name == 'stop_workflow_task_before_start'
        assert task_id == uuid.UUID(TASK_ID)
        assert _attempt_elapsed_ms(arguments) == {
            'worker_id': 'worker-7',
            'claimed_at': generation,
            'orphan_self_heal': True,
        }

    @pytest.mark.parametrize(
        'move_outcome',
        ['APPLIED', 'ALREADY_APPLIED', 'LOST_CLAIM', 'SOURCE_STATE_CONFLICT', 'TASK_ABSENT'],
    )
    def test_a_stopped_workflow_stops_the_task_whatever_its_move_found(
        self,
        move_outcome: str,
    ) -> None:
        """A row that the reaper requeued to PENDING is cancelled by the
        function (the carve-out of a CANCELLED workflow); a row that another
        claim holds is left to it. Either way the task of this child stops."""
        for outcome in ('ABANDONED', 'CANCELLED'):
            handle = _FakeChildHandle(
                starts=[_missed('WORKFLOW_CANCELLED')],
                stops=[_stopped(outcome, move_outcome)],
            )

            assert _start_with(handle) == StoppedBeforeRunning('WORKFLOW_STOPPED'), outcome

    def test_an_expiry_found_by_the_stop_expires_the_task(self) -> None:
        handle = _FakeChildHandle(
            starts=[_missed('WORKFLOW_PAUSED')],
            stops=[_stopped('EXPIRED', 'APPLIED')],
        )

        assert _start_with(handle) == StoppedBeforeRunning('TASK_EXPIRED')

    def test_a_claim_that_the_stop_finds_not_held_is_lost(self) -> None:
        handle = _FakeChildHandle(
            starts=[_missed('WORKFLOW_CANCELLED')],
            stops=[_stopped('NOT_HELD', 'LOST_CLAIM')],
        )

        assert _start_with(handle) == StoppedBeforeRunning('CLAIM_LOST')

    def test_a_resume_before_the_stop_starts_again(self) -> None:
        from horsies.core.worker.child_runner import _WorkflowResumedBeforeStart

        handle = _FakeChildHandle(
            starts=[_missed('WORKFLOW_PAUSED')],
            stops=[_record(core.ResumedBeforeStart, task_id=uuid.UUID(TASK_ID))],
        )

        assert _start_with(handle) == _WorkflowResumedBeforeStart()

    def test_a_failed_stop_is_unconfirmed(self) -> None:
        handle = _FakeChildHandle(
            starts=[_missed('WORKFLOW_PAUSED')],
            stops=[
                _record(
                    core.StopBeforeStartFailed,
                    task_id=uuid.UUID(TASK_ID),
                    message='connection lost',
                    retryable=True,
                ),
            ],
        )

        assert _start_with(handle) == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')


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
        'input_json': '{}',
        'task_id': TASK_ID,
        'master_worker_id': 'w-1',
        'work': RunTheTask(),
        'claimed_at': CLAIMED_AT,
        'task_loop': TaskLoop(),
        'orphan_self_heal': True,
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
            database_url=SecretStr('postgresql://u:p@localhost:5432/db'),
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
        """When the ownership confirm returns a stop, _run_task_entry returns
        it as a run that stopped before its RUNNING update."""
        stopped = StoppedBeforeRunning('CLAIM_LOST')
        patches = _make_run_task_patches(ownership=stopped)

        with _apply_patches(patches):
            result = _run_task_entry(**_run_entry_defaults)

        assert result == stopped

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert 'KeyError' in _error_message(payload)
        parsed = _parse_task_result(payload)
        err = parsed.get('err') or parsed.get('error')
        assert err['error_code'] == {'__builtin_task_code__': OperationalErrorCode.WORKER_RESOLUTION_ERROR.value}

    def test_input_json_invalid(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """Malformed input_json → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['input_json'] = '{{{bad'

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _error_code(payload) == _SERIALIZATION_ERROR

    def test_input_json_lone_surrogate(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """An escape such as \\ud800 → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['input_json'] = r'{"x":"\ud800"}'

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert 'lone surrogate' in _error_message(payload)
        assert 'WORKER_SERIALIZATION_ERROR' in payload

    def test_input_json_past_the_depth_bound(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """A text past the bound → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['input_json'] = (
            '{"x":' + '[' * 1_100 + ']' * 1_100 + '}'
        )

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert 'nested deeper than' in _error_message(payload)
        assert 'WORKER_SERIALIZATION_ERROR' in payload

    def test_input_json_number_outside_finite_binary64(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """A literal such as 1e9999 → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['input_json'] = '{"x":1e9999}'

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert 'finite binary64' in _error_message(payload)
        assert 'WORKER_SERIALIZATION_ERROR' in payload

    def test_input_json_not_a_dict(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """input_json is valid JSON but not a dict → WORKER_SERIALIZATION_ERROR."""
        patches = _make_run_task_patches()
        _run_entry_defaults['input_json'] = '[1, 2]'

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _error_code(payload) == _SERIALIZATION_ERROR

    def test_an_empty_input_object_runs_a_task_with_no_arguments(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """The stored input of a task sent with no keyword arguments is the
        empty object, and the task runs with none. Without this every task
        with no arguments (healthcheck, return_none_task, ...) fails at the
        worker."""
        patches = _make_run_task_patches()
        mock_app = patches['horsies.core.worker.child_runner.get_current_app'].return_value
        mock_task = MagicMock()
        mock_task.task_body_kind = TaskBodyKind.DEF
        mock_task.task_ok_type = int
        mock_task.return_value = TaskResult(ok=42)
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)
        _run_entry_defaults['input_json'] = '{}'

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        parsed = _parse_task_result(payload)
        assert parsed.get('err') is None, (
            f'expected no error for an empty input, got: {parsed}'
        )
        assert parsed['ok'] == 42

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert 'holds a lone surrogate' in _error_message(payload)
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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
        _run_entry_defaults['input_json'] = json.dumps({'x': {'name': 'plain'}})

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _error_code(payload) == _SERIALIZATION_ERROR
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
        mock_task.task_ok_type = str
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _parse_task_result(payload)['err'] is None, payload

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _error_code(payload) != _SERIALIZATION_ERROR
        assert _error_message(payload) == 'failed'

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _error_code(payload) == _SERIALIZATION_ERROR


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
        _run_entry_defaults['input_json'] = json.dumps({
            'upstream': {
                '__h_taskresult_envelope__': True,
                'data': '{{{invalid',
            },
        })

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _error_code(payload) == _SERIALIZATION_ERROR

    def test_horsies_taskresult_not_a_task_result_json(
        self,
        _run_entry_defaults: dict[str, Any],
    ) -> None:
        """__h_taskresult_envelope__ with valid JSON but not TaskResult shape → serde error."""
        patches = _make_run_task_patches()
        _run_entry_defaults['input_json'] = json.dumps({
            'upstream': {
                '__h_taskresult_envelope__': True,
                'data': '"just a string"',
            },
        })

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _error_code(payload) == _SERIALIZATION_ERROR

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

        _run_entry_defaults['input_json'] = json.dumps({
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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _error_code(payload) == _SERIALIZATION_ERROR

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

        _run_entry_defaults['input_json'] = json.dumps({
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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _error_code(payload) == _SERIALIZATION_ERROR

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
        mock_task.task_ok_type = str
        mock_task._original_fn = _task_with_meta
        mock_task._fn = _task_with_meta
        mock_app.tasks.__getitem__ = MagicMock(return_value=mock_task)

        _run_entry_defaults['input_json'] = json.dumps({
            '__h_workflow_meta__': {
                'workflow_id': 'wf-1',
                'task_index': 3,
                'task_name': 'my_task',
            },
        })

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _parse_task_result(payload)['err'] is None, payload


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
    """The child maps the records of the expiry to its return contract."""

    @pytest.mark.parametrize('ended', ['TaskRowEnded', 'AttemptAlreadyApplied'])
    def test_applied_or_replayed_expiry_returns_expired(self, ended: str) -> None:
        records = {
            'TaskRowEnded': _ended_row('EXPIRE_CLAIMED'),
            'AttemptAlreadyApplied': _refusal(core.AttemptAlreadyApplied),
        }
        handle = _FakeChildHandle(
            starts=[_missed('DEADLINE_PASSED')],
            expiries=[records[ended]],
        )

        result = _start_with(handle)

        assert result == StoppedBeforeRunning('TASK_EXPIRED')
        name, task_id, arguments = handle.calls[1]
        assert name == 'expire_owned_claim'
        assert task_id == uuid.UUID(TASK_ID)
        assert _attempt_elapsed_ms(arguments) == {'worker_id': 'worker-A'}

    @pytest.mark.parametrize(
        'refusal',
        [core.AttemptLostClaim, core.AttemptSourceStateConflict, core.AttemptTaskAbsent],
    )
    def test_refusal_or_absence_is_a_lost_claim(self, refusal: type) -> None:
        handle = _FakeChildHandle(
            starts=[_missed('DEADLINE_PASSED')],
            expiries=[_refusal(refusal)],
        )

        assert _start_with(handle) == StoppedBeforeRunning('CLAIM_LOST')

    def test_a_failed_expiry_is_unconfirmed(self) -> None:
        handle = _FakeChildHandle(
            starts=[_missed('DEADLINE_PASSED')],
            expiries=[
                _record(
                    core.AttemptWriteFailed,
                    task_id=uuid.UUID(TASK_ID),
                    message='connection lost',
                    retryable=True,
                    exception=core.CoreConnectionError(
                        'expire_owned_claim', 'connection lost', True,
                    ),
                ),
            ],
        )

        assert _start_with(handle) == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')


# ===================================================================
# M. The start: one horsies_start_attempt call, then the miss path by reason
# ===================================================================


@pytest.mark.unit
class TestAttemptElapsedMs:
    """``attempt_elapsed_ms``: the time since a start attempt began, which
    the miss path sends to the core."""

    def test_the_time_since_the_attempt_began_is_rounded_up_to_a_millisecond(
        self,
    ) -> None:
        with patch(
            'horsies.core.worker.child_runner.time.monotonic',
            return_value=10.0004,
        ):
            assert attempt_elapsed_ms(10.0) == 1

    def test_a_clock_reading_before_the_attempt_began_is_no_time(self) -> None:
        with patch(
            'horsies.core.worker.child_runner.time.monotonic',
            return_value=9.0,
        ):
            assert attempt_elapsed_ms(10.0) == 0


class TestStartAttempt:
    def test_a_started_row_runs_the_task_after_one_call(self) -> None:
        handle = _FakeChildHandle(
            starts=[_record(core.AttemptStarted, task_id=uuid.UUID(TASK_ID))],
        )

        result = _start_with(handle)

        assert result is None
        assert handle.calls == [
            (
                'start_attempt',
                uuid.UUID(TASK_ID),
                {
                    'worker_id': 'worker-A',
                    'claimed_at': CLAIMED_AT,
                    'worker_pid': os.getpid(),
                    'worker_hostname': socket.gethostname(),
                    'deadline_in_ms': None,
                },
            ),
        ]

    def test_a_passed_deadline_expires_the_claim(self) -> None:
        handle = _FakeChildHandle(
            starts=[_missed('DEADLINE_PASSED')],
            expiries=[_ended_row('EXPIRE_CLAIMED')],
        )

        result = _start_with(handle)

        assert result == StoppedBeforeRunning('TASK_EXPIRED')
        assert handle.names() == ['start_attempt', 'expire_owned_claim']

    @pytest.mark.parametrize('reason', ['NOT_HELD', 'LEASE_EXPIRED'])
    def test_a_claim_that_is_not_held_or_expired_is_lost_with_no_write(
        self, reason: str,
    ) -> None:
        handle = _FakeChildHandle(starts=[_missed(reason)])

        result = _start_with(handle)

        assert result == StoppedBeforeRunning('CLAIM_LOST')
        assert handle.names() == ['start_attempt']

    def test_the_time_left_before_the_deadline_goes_with_the_start(self) -> None:
        from horsies.core.worker.child_runner import StartWithin

        handle = _FakeChildHandle(
            starts=[_record(core.AttemptStarted, task_id=uuid.UUID(TASK_ID))],
        )

        assert _start_with(handle, start_check=lambda: StartWithin(milliseconds=250)) is None
        assert handle.calls[0][2]['deadline_in_ms'] == 250

    def test_a_deadline_that_passed_before_the_check_sends_nothing(self) -> None:
        from horsies.core.worker.child_runner import DeadlineBeforeStart

        handle = _FakeChildHandle(starts=[])

        result = _start_with(handle, start_check=DeadlineBeforeStart)

        assert result == DeadlineBeforeStart()
        assert handle.calls == []

    def test_a_deadline_that_passed_while_the_pool_waited_does_not_start(self) -> None:
        from horsies.core.worker.child_runner import DeadlineBeforeStart, StartWithin

        handle = _FakeChildHandle(
            starts=[_record(core.DeadlineBeforeStart, task_id=uuid.UUID(TASK_ID))],
        )

        result = _start_with(handle, start_check=lambda: StartWithin(milliseconds=1))

        assert result == DeadlineBeforeStart()

    def test_a_failed_start_is_unconfirmed(self) -> None:
        handle = _FakeChildHandle(
            starts=[
                _record(
                    core.StartFailed,
                    task_id=uuid.UUID(TASK_ID),
                    message='horsies_start_attempt returned 0 rows',
                    retryable=False,
                ),
            ],
        )

        assert _start_with(handle) == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')

    def test_a_core_that_cannot_run_the_call_is_unconfirmed(self) -> None:
        handle = MagicMock()
        handle.start_attempt.side_effect = core.TooManyPendingCalls('the queue is full')

        assert _start_with(handle) == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')

    def test_a_record_outside_the_catalog_is_a_defect(self) -> None:
        handle = _FakeChildHandle(starts=[object()])

        with pytest.raises(AssertionError):
            _start_with(handle)

    def test_a_workflow_resumed_on_each_attempt_is_unconfirmed(self) -> None:
        from horsies.core.worker.child_runner import _confirm_until_not_resumed

        resumed = _record(core.ResumedBeforeStart, task_id=uuid.UUID(TASK_ID))
        handle = _FakeChildHandle(
            starts=[_missed('WORKFLOW_PAUSED'), _missed('WORKFLOW_PAUSED')],
            stops=[resumed, resumed],
        )

        with patch(
            'horsies.core.worker.child_runner._get_child_core_handle',
            return_value=handle,
        ):
            result = _confirm_until_not_resumed(
                TASK_ID,
                'worker-A',
                CLAIMED_AT,
                orphan_self_heal=True,
            )

        assert result == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')
        assert handle.names() == [
            'start_attempt',
            'stop_workflow_task_before_start',
            'start_attempt',
            'stop_workflow_task_before_start',
        ]


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
        _run_entry_defaults['input_json'] = json.dumps({
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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _parse_task_result(payload)['err'] is None, payload

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
        mock_task.task_ok_type = str
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
        _run_entry_defaults['input_json'] = json.dumps({
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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _parse_task_result(payload)['err'] is None, payload

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
        handle = MagicMock()
        patches: dict[str, Any] = {
            'horsies.core.worker.child_runner._confirm_ownership_and_set_running': MagicMock(
                return_value=None,
            ),
            'horsies.core.worker.child_runner.get_current_app': MagicMock(
                return_value=app,
            ),
            'horsies.core.worker.child_runner._get_child_core_handle': MagicMock(
                return_value=handle,
            ),
        }

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        assert _parse_task_result(payload)['ok'] == 'done'
        assert names_during_run
        assert not any(name.startswith('heartbeat') for name in names_during_run)
        # Every write of the child goes through its core handle: the run
        # makes none. The child writes the marker after the run has ended,
        # with the work that the task left on its loop (child_main._report_run).
        assert handle.mock_calls == []


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
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        assert _error_code(payload) == _SERIALIZATION_ERROR
        assert _INPUT_MARKER not in payload
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
        _run_entry_defaults['input_json'] = json.dumps({'count': _INPUT_MARKER})

        with caplog.at_level(logging.DEBUG), _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        self._assert_no_marker(payload, caplog)
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
        _run_entry_defaults['input_json'] = json.dumps({
            'upstream': {
                '__h_taskresult_envelope__': True,
                'source_task_name': 'upstream_task',
                'inner': {'__h_task_result__': True, 'ok': _INPUT_MARKER, 'err': None},
            },
        })

        with caplog.at_level(logging.DEBUG), _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        self._assert_no_marker(payload, caplog)

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

        self._assert_no_marker(payload, caplog)


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
        _run_entry_defaults['input_json'] = json.dumps({
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
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
        _run_entry_defaults['input_json'] = json.dumps({
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
            _ = envelope_of(_run_task_entry(**_run_entry_defaults))

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
    """Stored options that the worker refused reach the child as the work of
    its run frame (``isolated_work_of``)."""

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

        _run_entry_defaults['work'] = refusal

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
        _run_entry_defaults['work'] = TimeoutValueInvalid(reason='stored timeout_ms is a bool')

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
        _run_entry_defaults['work'] = CancelGraceInvalid(
            reason='stored cancel_grace_ms is absent',
        )

        with _apply_patches(patches):
            payload = envelope_of(_run_task_entry(**_run_entry_defaults))

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
                payload = envelope_of(_run_task_entry(**_run_entry_defaults))
            loop_state = task_loop.state
        finally:
            task_loop.close()

        assert _ok_value(payload, int) == 5
        assert isinstance(loop_state, LoopOpen)
        assert seen == [loop_state.loop]
