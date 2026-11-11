# pyright: reportPrivateUsage=false
"""Unit tests for @app.on_child_process_start registration and execution.

Execution tests patch os._exit (the fail-closed terminal path) with a
recorder, since the real call would kill the pytest process.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Coroutine
from typing import Any

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.worker import child_runner, shared_runner
from horsies.core.worker.child_runner import (
    CHILD_HOOK_FAILURE_EXIT_CODE,
    _run_child_start_hooks,
)
from horsies.core.worker.shared_runner import run_shared_async_start_hooks

pytestmark = [pytest.mark.unit]


def _make_app() -> Horsies:
    return Horsies(
        config=AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql+psycopg://user:pass@localhost/db'),
            ),
        )
    )


class TestRegistration:
    def test_decorator_returns_function_and_registers(self) -> None:
        app = _make_app()

        @app.on_child_process_start
        def reset_engines() -> None:
            pass

        assert app.get_child_process_start_hooks() == [reset_engines]

    def test_duplicate_registration_deduped_by_identity(self) -> None:
        app = _make_app()

        def reset_engines() -> None:
            pass

        app.on_child_process_start(reset_engines)
        app.on_child_process_start(reset_engines)

        assert app.get_child_process_start_hooks() == [reset_engines]

    def test_registration_order_preserved(self) -> None:
        app = _make_app()

        def first() -> None:
            pass

        def second() -> None:
            pass

        app.on_child_process_start(first)
        app.on_child_process_start(second)

        assert app.get_child_process_start_hooks() == [first, second]

    def test_async_function_rejected(self) -> None:
        app = _make_app()

        async def async_hook() -> None:
            pass

        with pytest.raises(ConfigurationError) as exc_info:
            app.on_child_process_start(async_hook)  # type: ignore[arg-type]
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_CHILD_HOOK
        assert app.get_child_process_start_hooks() == []

    def test_non_callable_rejected(self) -> None:
        app = _make_app()

        with pytest.raises(ConfigurationError) as exc_info:
            app.on_child_process_start('not a function')  # type: ignore[arg-type]
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_CHILD_HOOK

    def test_snapshot_is_a_copy(self) -> None:
        app = _make_app()

        def hook() -> None:
            pass

        app.on_child_process_start(hook)
        snapshot = app.get_child_process_start_hooks()
        snapshot.clear()

        assert app.get_child_process_start_hooks() == [hook]


class _ExitCalled(Exception):
    """Raised by the patched os._exit to halt execution like the real call."""

    def __init__(self, code: int) -> None:
        super().__init__(f'os._exit({code})')
        self.code = code


class TestRunChildStartHooks:
    def test_hooks_run_in_registration_order(self) -> None:
        app = _make_app()
        calls: list[str] = []

        app.on_child_process_start(lambda: calls.append('first'))
        app.on_child_process_start(lambda: calls.append('second'))

        _run_child_start_hooks(app)

        assert calls == ['first', 'second']

    def test_no_hooks_is_a_no_op(self) -> None:
        _run_child_start_hooks(_make_app())

    def test_failing_hook_exits_173_and_skips_rest(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        app = _make_app()
        calls: list[str] = []

        def fake_exit(code: int) -> Any:
            raise _ExitCalled(code)

        monkeypatch.setattr(child_runner.os, '_exit', fake_exit)

        def boom() -> None:
            raise RuntimeError('pool rebind failed')

        app.on_child_process_start(boom)
        app.on_child_process_start(lambda: calls.append('after'))

        with pytest.raises(_ExitCalled) as exc_info:
            _run_child_start_hooks(app)

        assert exc_info.value.code == CHILD_HOOK_FAILURE_EXIT_CODE
        assert calls == []

    def test_hung_hook_triggers_watchdog_exit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        app = _make_app()
        exit_codes: list[int] = []
        exit_seen = threading.Event()

        def recording_exit(code: int) -> None:
            exit_codes.append(code)
            exit_seen.set()

        monkeypatch.setattr(child_runner.os, '_exit', recording_exit)
        monkeypatch.setattr(child_runner, 'CHILD_HOOK_TIMEOUT_SECONDS', 0.1)

        def hung_hook() -> None:
            # Wait for the watchdog instead of sleeping a fixed time, so the
            # test is not timing-sensitive; cap it to keep failures bounded.
            exit_seen.wait(timeout=5.0)

        app.on_child_process_start(hung_hook)
        _run_child_start_hooks(app)

        assert exit_codes == [CHILD_HOOK_FAILURE_EXIT_CODE]

    def test_mid_run_hook_failure_stops_worker_instead_of_restart_looping(
        self,
    ) -> None:
        """A hook that fails only in a replacement child stops the worker.

        Pins the mid-run off-ramp: the failure re-runs in every replacement
        child, so the worker stops instead of starting more children.
        """
        import asyncio
        from unittest.mock import AsyncMock, MagicMock

        from horsies.core.worker.config import WorkerConfig
        from horsies.core.worker.slots import ChildHookFailed
        from horsies.core.worker.worker import Worker

        cfg = WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
        )
        worker = Worker(
            session_factory=MagicMock(), listener=MagicMock(), cfg=cfg,
        )
        pool = MagicMock()
        pool.wait_failed = AsyncMock(return_value=ChildHookFailed())

        asyncio.run(worker._slot_failure_loop(pool))

        assert worker._stop.is_set()
        pool.wait_failed.assert_awaited_once()

    def test_fast_hook_does_not_trip_watchdog(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        app = _make_app()
        exit_codes: list[int] = []

        def recording_exit(code: int) -> None:
            exit_codes.append(code)

        monkeypatch.setattr(child_runner.os, '_exit', recording_exit)
        monkeypatch.setattr(child_runner, 'CHILD_HOOK_TIMEOUT_SECONDS', 0.2)

        app.on_child_process_start(lambda: None)
        _run_child_start_hooks(app)

        # Give a cancelled-timer misfire a moment to surface before asserting.
        time.sleep(0.3)
        assert exit_codes == []


class _AsyncCallHook:
    """A hook object whose ``__call__`` is ``async def``."""

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self) -> None:
        await asyncio.sleep(0)
        self.calls += 1


class _SyncCallHook:
    def __call__(self) -> None:
        pass


def _sync_function() -> None:
    pass


class TestSharedAsyncHookRegistration:
    def test_an_async_def_hook_is_registered(self) -> None:
        app = _make_app()

        @app.on_shared_async_child_start
        async def open_client() -> None:
            pass

        assert app.get_shared_async_child_start_hooks() == [open_client]

    def test_a_sync_hook_is_refused(self) -> None:
        app = _make_app()

        def sync_hook() -> None:
            pass

        with pytest.raises(ConfigurationError) as raised:
            app.on_shared_async_child_start(sync_hook)  # type: ignore[arg-type]
        assert raised.value.code == ErrorCode.CONFIG_INVALID_CHILD_HOOK
        assert 'must be async def' in raised.value.message
        assert app.get_shared_async_child_start_hooks() == []

    def test_a_non_callable_is_refused(self) -> None:
        app = _make_app()
        with pytest.raises(ConfigurationError) as raised:
            app.on_shared_async_child_start('nope')  # type: ignore[arg-type]
        assert raised.value.code == ErrorCode.CONFIG_INVALID_CHILD_HOOK

    def test_an_object_with_an_async_call_is_registered(self) -> None:
        app = _make_app()
        hook = _AsyncCallHook()

        app.on_shared_async_child_start(hook)

        assert app.get_shared_async_child_start_hooks() == [hook]

    def test_a_lambda_that_returns_a_coroutine_is_registered(self) -> None:
        """A lambda can be neither async def nor annotated: the child awaits
        its value."""
        app = _make_app()

        async def open_client(size: int) -> None:
            del size

        def hook() -> Coroutine[Any, Any, None]:
            return open_client(10)

        app.on_shared_async_child_start(hook)
        as_lambda = lambda: open_client(10)  # noqa: E731
        app.on_shared_async_child_start(as_lambda)

        assert app.get_shared_async_child_start_hooks() == [hook, as_lambda]

    @pytest.mark.parametrize(
        'hook',
        [_SyncCallHook(), _sync_function],
        ids=['object with a sync call', 'def that returns None'],
    )
    def test_a_sync_callable_is_still_refused(self, hook: Any) -> None:
        app = _make_app()

        with pytest.raises(ConfigurationError) as raised:
            app.on_shared_async_child_start(hook)

        assert raised.value.code == ErrorCode.CONFIG_INVALID_CHILD_HOOK
        assert app.get_shared_async_child_start_hooks() == []

    def test_a_second_registration_of_one_hook_is_ignored(self) -> None:
        app = _make_app()

        async def open_client() -> None:
            pass

        app.on_shared_async_child_start(open_client)
        app.on_shared_async_child_start(open_client)
        assert app.get_shared_async_child_start_hooks() == [open_client]

    def test_the_two_kinds_of_hooks_are_kept_apart(self) -> None:
        app = _make_app()

        async def open_client() -> None:
            pass

        app.on_shared_async_child_start(open_client)
        assert app.get_child_process_start_hooks() == []


class TestRunSharedAsyncHooks:
    def _run_on_app(self, app: Horsies, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(shared_runner, 'get_current_app', lambda: app)
        asyncio.run(run_shared_async_start_hooks())

    def test_hooks_run_in_order_on_the_running_loop(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        app = _make_app()
        seen: list[tuple[str, bool]] = []

        async def first() -> None:
            seen.append(('first', asyncio.get_running_loop().is_running()))

        async def second() -> None:
            await asyncio.sleep(0)
            seen.append(('second', asyncio.get_running_loop().is_running()))

        app.on_shared_async_child_start(first)
        app.on_shared_async_child_start(second)
        self._run_on_app(app, monkeypatch)

        assert seen == [('first', True), ('second', True)]

    def test_an_async_call_object_and_a_lambda_run_on_the_loop(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        app = _make_app()
        hook = _AsyncCallHook()
        seen: list[bool] = []

        async def open_client() -> None:
            seen.append(asyncio.get_running_loop().is_running())

        app.on_shared_async_child_start(hook)
        app.on_shared_async_child_start(lambda: open_client())
        self._run_on_app(app, monkeypatch)

        assert hook.calls == 1
        assert seen == [True]

    def test_a_lambda_whose_value_is_not_awaitable_fails_the_hook(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        app = _make_app()

        def fake_exit(code: int) -> Any:
            raise _ExitCalled(code)

        monkeypatch.setattr(shared_runner.os, '_exit', fake_exit)
        app.on_shared_async_child_start(lambda: None)  # type: ignore[arg-type, return-value]

        with pytest.raises(_ExitCalled) as raised:
            self._run_on_app(app, monkeypatch)

        assert raised.value.code == CHILD_HOOK_FAILURE_EXIT_CODE

    def test_a_failing_hook_exits_173_and_skips_the_rest(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        app = _make_app()
        calls: list[str] = []

        def fake_exit(code: int) -> Any:
            raise _ExitCalled(code)

        monkeypatch.setattr(shared_runner.os, '_exit', fake_exit)

        async def boom() -> None:
            raise RuntimeError('client setup failed')

        async def after() -> None:
            calls.append('after')

        app.on_shared_async_child_start(boom)
        app.on_shared_async_child_start(after)
        with pytest.raises(_ExitCalled) as raised:
            self._run_on_app(app, monkeypatch)

        assert raised.value.code == CHILD_HOOK_FAILURE_EXIT_CODE
        assert calls == []

    def test_a_hook_that_blocks_the_loop_trips_the_watchdog(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The bound holds also when the hook never yields to the loop."""
        app = _make_app()
        exit_codes: list[int] = []
        exit_seen = threading.Event()

        def recording_exit(code: int) -> None:
            exit_codes.append(code)
            exit_seen.set()

        monkeypatch.setattr(child_runner.os, '_exit', recording_exit)
        monkeypatch.setattr(child_runner, 'CHILD_HOOK_TIMEOUT_SECONDS', 0.1)

        async def blocking_hook() -> None:
            exit_seen.wait(timeout=5.0)

        app.on_shared_async_child_start(blocking_hook)
        self._run_on_app(app, monkeypatch)

        assert exit_codes == [CHILD_HOOK_FAILURE_EXIT_CODE]
