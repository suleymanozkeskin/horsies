# pyright: reportPrivateUsage=false
"""Unit tests for @app.on_child_process_start registration and execution.

Execution tests patch os._exit (the fail-closed terminal path) with a
recorder, since the real call would kill the pytest process.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.worker import child_runner
from horsies.core.worker.child_runner import (
    CHILD_HOOK_FAILURE_EXIT_CODE,
    _run_child_start_hooks,
)

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
        """ChildHookFailedError from a warmed-executor rebuild sets the stop flag.

        Pins the mid-run off-ramp: the boot path is covered e2e, but a hook
        that fails only on a respawn reaches _restart_executor, which must
        stop the worker rather than retry a failure that re-runs in every
        replacement child.
        """
        import asyncio
        from unittest.mock import AsyncMock, MagicMock

        from horsies.core.worker.config import WorkerConfig
        from horsies.core.worker.worker import ChildHookFailedError, Worker

        cfg = WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
        )
        worker = Worker(
            session_factory=MagicMock(), listener=MagicMock(), cfg=cfg,
        )
        broken_executor = MagicMock()
        worker._executor = broken_executor
        worker._create_warmed_executor = AsyncMock(  # type: ignore[method-assign]
            side_effect=ChildHookFailedError('hook failed in replacement child'),
        )

        async def _run() -> None:
            await worker._restart_executor(
                'broken pool during test', failed_executor=broken_executor,
            )

        asyncio.run(_run())

        assert worker._stop.is_set()
        worker._create_warmed_executor.assert_awaited_once()

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
