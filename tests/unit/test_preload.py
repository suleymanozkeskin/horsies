"""Tests for the preload parent. Tests that fork need Linux."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import ctypes
import os
import subprocess
import sys
import textwrap
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker import preload, slots
from horsies.core.worker.child_protocol import (
    ChildForkedFrame,
    ForkFailedFrame,
    InitFrame,
    PreloadReadyFrame,
    RunFrame,
)
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.preload import (
    PreloadDown,
    PreloadParent,
    PreloadRefused,
    PreloadStartFailed,
    PreloadSupported,
    PreloadUnsupported,
    preload_support,
    reap_landed_orphans,
)
from horsies.core.worker.preload_main import parse_arguments
from horsies.core.worker.slots import (
    PRELOAD_ENTRY,
    SlotPool,
    SlotPoolConfig,
    SpawnFailed,
    TaskFinished,
)

pytestmark = [pytest.mark.unit]

FIXTURES = Path(__file__).parent.parent / 'fixtures' / 'slots'
SCRIPTED_CHILD = str(FIXTURES / 'scripted_child.py')
SCRIPTED_PRELOAD = str(FIXTURES / 'scripted_preload.py')
OUTCOME_TIMEOUT_S = 30.0
PR_GET_CHILD_SUBREAPER = 37

linux_only = pytest.mark.skipif(
    isinstance(preload_support(), PreloadUnsupported),
    reason='this host cannot run the preload parent',
)


def _init(app_locator: str = 'ok') -> InitFrame:
    return InitFrame(
        app_locator=app_locator,
        imports=(),
        sys_path_roots=(),
        parent_sys_path=tuple(path or os.getcwd() for path in sys.path),
        parent_argv=('horsies', 'worker'),
        loglevel=30,
        database_url='postgresql://user:secret@localhost/none',
        connect_kwargs={},
        child_pool_min_size=0,
        child_pool_max_size=1,
        child_pool_check=False,
        process_name='horsies-child',
    )


def _config(
    slot_count: int = 1,
    *,
    mode: ChildPreload = ChildPreload.LIBRARY,
    max_tasks_per_child: int = 0,
    preload_entry: tuple[str, ...] = (SCRIPTED_PRELOAD,),
) -> SlotPoolConfig:
    return SlotPoolConfig(
        slots=slot_count,
        init=_init(),
        max_tasks_per_child=max_tasks_per_child,
        max_memory_per_child_mb=None,
        child_entry=(SCRIPTED_CHILD,),
        child_preload=mode,
        preload_entry=preload_entry,
    )


_task_number = 0


def _frame(task_name: str, argument: str | None = None) -> RunFrame:
    global _task_number
    _task_number += 1
    return RunFrame(
        task_id=f'task-{_task_number}',
        task_name=task_name,
        args_json=None,
        kwargs_json=argument,
        worker_id='worker-1',
        runner_heartbeat_interval_ms=30_000,
        is_workflow_task=False,
        timeout_ms=None,
        claimed_at=None,
    )


async def _result(pool: SlotPool, task_name: str, argument: str | None = None) -> str:
    handle = pool.submit(_frame(task_name, argument))
    outcome = await asyncio.wait_for(asyncio.shield(handle.outcome), OUTCOME_TIMEOUT_S)
    assert isinstance(outcome, TaskFinished), outcome
    return outcome.result_json


@pytest_asyncio.fixture
async def started_pools() -> AsyncIterator[list[SlotPool]]:
    pools: list[SlotPool] = []
    yield pools
    for pool in pools:
        await pool.stop(0)


async def _started(pools: list[SlotPool], config: SlotPoolConfig) -> SlotPool:
    pool = SlotPool(config, pending_limit=1000)
    pools.append(pool)
    match await pool.start():
        case Ok(_):
            return pool
        case Err(failure):
            raise AssertionError(failure)


async def _running_preload(pool: SlotPool) -> PreloadParent:
    for _ in range(500):
        parent = pool._preload
        if parent is not None and parent.is_running:
            return parent
        await asyncio.sleep(0.01)
    raise AssertionError('no preload parent came up')


class TestArguments:
    def test_valid(self) -> None:
        assert parse_arguments(['5', '42', 'app']) == (5, 42, ChildPreload.APP)

    @pytest.mark.parametrize(
        'argv',
        [[], ['5', '42'], ['x', '42', 'app'], ['5', 'x', 'app'], ['5', '42', 'other']],
    )
    def test_invalid(self, argv: list[str]) -> None:
        assert parse_arguments(argv) is None


class TestSupport:
    def test_other_platforms_use_exec(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(preload.sys, 'platform', 'darwin')
        assert isinstance(preload_support(), PreloadUnsupported)

    @linux_only
    def test_a_kernel_with_no_pidfd_uses_exec(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def refuse(_pid: int) -> int:
            raise OSError(38, 'no pidfd')

        monkeypatch.setattr(preload, '_open_pidfd', refuse)
        assert isinstance(preload_support(), PreloadUnsupported)

    @linux_only
    def test_a_blocked_prctl_uses_exec(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def refuse(_enabled: bool) -> bool:
            return False

        monkeypatch.setattr(preload, '_set_subreaper', refuse)
        assert isinstance(preload_support(), PreloadUnsupported)

    @linux_only
    def test_this_host_is_supported(self) -> None:
        assert preload_support() == PreloadSupported()


class TestOrphanSweep:
    @pytest.fixture
    def reaped(self, monkeypatch: pytest.MonkeyPatch) -> Iterator[list[int]]:
        calls: list[int] = []

        def waitpid(pid: int, _options: int) -> tuple[int, int]:
            calls.append(pid)
            return pid, 0

        monkeypatch.setattr(preload.os, 'waitpid', waitpid)

        def getsid(pid: int) -> int:
            return 1 if pid in (0, 11) else 2

        monkeypatch.setattr(preload.os, 'getsid', getsid)
        yield calls

    @staticmethod
    def _exited(monkeypatch: pytest.MonkeyPatch, *pids: int) -> None:
        remaining = list(pids)
        monkeypatch.setattr(
            preload,
            '_next_exited_child',
            lambda: remaining.pop(0) if remaining else None,
        )

    def test_an_orphan_from_another_session_is_reaped(
        self,
        reaped: list[int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._exited(monkeypatch, 20, 21)
        reap_landed_orphans(())
        assert reaped == [20, 21]

    def test_a_known_child_keeps_its_exit_status(
        self,
        reaped: list[int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._exited(monkeypatch, 20, 21)
        reap_landed_orphans({20})
        assert reaped == []

    def test_a_child_in_this_session_keeps_its_exit_status(
        self,
        reaped: list[int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._exited(monkeypatch, 11, 21)
        reap_landed_orphans(())
        assert reaped == []

    def test_the_sweep_is_bounded(
        self,
        reaped: list[int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(preload, '_next_exited_child', lambda: 20)
        reap_landed_orphans(())
        assert len(reaped) == preload.ORPHAN_SWEEP_LIMIT


class TestForkReplies:
    def _parent(self) -> PreloadParent:
        parent = PreloadParent.__new__(PreloadParent)
        parent.process = MagicMock(returncode=None)
        parent._loop = MagicMock()
        return parent

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_new_child_is_registered_before_the_sweep(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The sweep must never see a pid that only a later await would register."""
        parent = self._parent()
        parent._lock = asyncio.Lock()
        parent._sock = MagicMock()

        def allow(_enabled: bool) -> bool:
            return True

        def record(pids: set[int]) -> None:
            swept.append(pids)

        swept: list[set[int]] = []
        monkeypatch.setattr(preload, '_set_subreaper', allow)
        monkeypatch.setattr(preload, 'reap_landed_orphans', record)
        monkeypatch.setattr(preload.socket, 'send_fds', MagicMock())
        monkeypatch.setattr(
            parent,
            '_read_frame',
            AsyncMock(return_value=ChildForkedFrame(pid=4321)),
        )
        monkeypatch.setattr(parent, '_child_from', MagicMock())
        live_pids: set[int] = set()

        await parent.fork_child(MagicMock(), live_pids)

        assert live_pids == {4321}
        assert 4321 in swept[0]

    def test_a_failed_fork_leaves_the_parent_up(self) -> None:
        parent = self._parent()
        assert parent._child_from(ForkFailedFrame('EAGAIN')) == Err(PreloadDown('EAGAIN'))
        cast(MagicMock, parent.process).kill.assert_not_called()

    def test_an_unexpected_frame_ends_the_parent(self) -> None:
        parent = self._parent()
        assert isinstance(parent._child_from(PreloadReadyFrame(pid=1)), Err)
        cast(MagicMock, parent.process).kill.assert_called_once_with()

    def test_a_pid_that_cannot_be_watched_is_no_child(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def refuse(_pid: int) -> int:
            raise OSError(24, 'too many files')

        monkeypatch.setattr(preload, '_open_pidfd', refuse)
        match self._parent()._child_from(ChildForkedFrame(pid=1)):
            case Err(PreloadDown(detail=detail)):
                assert 'too many files' in detail
            case other:
                raise AssertionError(other)


@linux_only
@pytest.mark.asyncio(loop_scope='function')
class TestForkedChild:
    async def test_the_worker_is_the_real_parent(
        self,
        started_pools: list[SlotPool],
    ) -> None:
        pool = await _started(started_pools, _config())
        child = pool._slots[0].child
        assert child is not None
        assert isinstance(child.process, preload.ReparentedProcess)
        assert int(await _result(pool, 'pid')) == child.pid
        stat = Path(f'/proc/{child.pid}/stat').read_text().rsplit(')', 1)[1].split()
        assert (int(stat[1]), int(stat[3])) == (os.getpid(), child.pid)

    async def test_the_subreaper_flag_is_off_between_spawns(
        self,
        started_pools: list[SlotPool],
    ) -> None:
        await _started(started_pools, _config(2))
        value = ctypes.c_int(-1)
        libc = ctypes.CDLL(None, use_errno=True)
        assert libc.prctl(PR_GET_CHILD_SUBREAPER, ctypes.byref(value), 0, 0, 0) == 0
        assert value.value == 0

    async def test_exit_status_comes_from_the_real_exit(
        self,
        started_pools: list[SlotPool],
    ) -> None:
        pool = await _started(started_pools, _config())
        handle = pool.submit(_frame('kill_self'))
        outcome = await asyncio.wait_for(handle.outcome, OUTCOME_TIMEOUT_S)
        assert 'SIGKILL' in outcome.describe()  # type: ignore[union-attr]

    async def test_a_new_child_replaces_a_recycled_one_by_fork(
        self,
        started_pools: list[SlotPool],
    ) -> None:
        pool = await _started(started_pools, _config(max_tasks_per_child=1))
        first = await _result(pool, 'pid')
        second = await _result(pool, 'pid')
        assert first != second
        child = pool._slots[0].child
        assert child is not None
        assert isinstance(child.process, preload.ReparentedProcess)


@linux_only
@pytest.mark.asyncio(loop_scope='function')
class TestPreloadParentDeath:
    async def test_running_tasks_finish_and_new_children_use_exec(
        self,
        started_pools: list[SlotPool],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(slots, 'PRELOAD_RESTART_DELAY_S', 60.0)
        pool = await _started(started_pools, _config(2, max_tasks_per_child=1))
        running = pool.submit(_frame('sleep', '1.0'))
        await asyncio.wait_for(running.assigned, OUTCOME_TIMEOUT_S)
        parent = pool._preload
        assert parent is not None
        parent.process.kill()
        await parent.process.wait()
        await asyncio.sleep(0)

        outcome = await asyncio.wait_for(running.outcome, OUTCOME_TIMEOUT_S)

        assert isinstance(outcome, TaskFinished)
        assert await _result(pool, 'echo', 'after') == 'after'
        await _result(pool, 'echo', 'again')
        children = [slot.child for slot in pool._slots if slot.child is not None]
        assert any(
            isinstance(child.process, asyncio.subprocess.Process) for child in children
        )

    async def test_a_replacement_serves_new_children(
        self,
        started_pools: list[SlotPool],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(slots, 'PRELOAD_RESTART_DELAY_S', 0.01)
        pool = await _started(started_pools, _config(max_tasks_per_child=1))
        first = pool._preload
        assert first is not None
        first.process.kill()
        await first.process.wait()
        for _ in range(500):
            if pool._preload is not None and pool._preload is not first:
                break
            await asyncio.sleep(0.01)
        assert pool._preload is not None and pool._preload is not first
        await _result(pool, 'echo', 'one')
        await _result(pool, 'echo', 'two')
        child = pool._slots[0].child
        assert child is not None
        assert isinstance(child.process, preload.ReparentedProcess)

    async def test_no_preload_parent_and_no_exec_is_a_pool_failure(
        self,
        started_pools: list[SlotPool],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(slots, 'PRELOAD_RESTART_DELAY_S', 60.0)
        pool = await _started(started_pools, _config(max_tasks_per_child=1))
        parent = await _running_preload(pool)
        parent.process.kill()
        await parent.process.wait()
        monkeypatch.setattr(sys, 'executable', '/nonexistent/python')

        handle = pool.submit(_frame('echo', 'x'))
        await asyncio.wait_for(handle.outcome, OUTCOME_TIMEOUT_S)
        failure = await asyncio.wait_for(pool.wait_failed(), OUTCOME_TIMEOUT_S)

        assert isinstance(failure, SpawnFailed)

    async def test_restarts_are_bounded(
        self,
        started_pools: list[SlotPool],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(slots, 'PRELOAD_RESTART_DELAY_S', 0.0)
        monkeypatch.setattr(slots, 'PRELOAD_RESTART_LIMIT', 1)
        pool = await _started(started_pools, _config())
        for _ in range(2):
            parent = await _running_preload(pool)
            parent.process.kill()
            await parent.process.wait()
        watch = pool._preload_watch
        assert watch is not None
        await asyncio.wait_for(watch, OUTCOME_TIMEOUT_S)
        assert pool._preload is None


@linux_only
@pytest.mark.asyncio(loop_scope='function')
class TestStartFailure:
    async def test_library_mode_falls_back_to_exec(
        self,
        started_pools: list[SlotPool],
    ) -> None:
        config = _config(preload_entry=('-c', 'raise SystemExit(3)'))
        pool = await _started(started_pools, config)
        child = pool._slots[0].child
        assert child is not None
        assert isinstance(child.process, asyncio.subprocess.Process)
        assert await _result(pool, 'echo', 'x') == 'x'

    async def test_app_mode_reports_the_failure(self) -> None:
        config = _config(
            mode=ChildPreload.APP,
            preload_entry=('-c', 'raise SystemExit(3)'),
        )
        pool = SlotPool(config, pending_limit=10)
        match await pool.start():
            case Err(PreloadRefused()):
                pass
            case other:
                raise AssertionError(other)
        await pool.stop(0)


APP_HEAD = """
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode

app = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(database_url='postgresql+psycopg://u:p@localhost/none'),
    ),
)
"""


@linux_only
@pytest.mark.asyncio(loop_scope='function')
class TestAppGuard:
    """The real preload program on a real app module."""

    @staticmethod
    async def _start(
        tmp_path: Path,
        body: str,
    ) -> Result[PreloadParent, PreloadRefused | PreloadStartFailed]:
        path = tmp_path / 'guarded_app.py'
        path.write_text(APP_HEAD + textwrap.dedent(body))
        return await PreloadParent.start(
            PRELOAD_ENTRY,
            ChildPreload.APP,
            _init(f'{path}:app'),
        )

    async def test_a_clean_app_is_accepted(self, tmp_path: Path) -> None:
        match await self._start(tmp_path, ''):
            case Ok(parent):
                await parent.stop()
            case other:
                raise AssertionError(other)

    async def test_a_thread_is_refused(self, tmp_path: Path) -> None:
        body = """
            import threading, time
            threading.Thread(target=time.sleep, args=(60,), daemon=True).start()
        """
        match await self._start(tmp_path, body):
            case Err(PreloadRefused(reason=reason)):
                assert 'threads' in reason
            case other:
                raise AssertionError(other)

    async def test_a_socket_is_refused(self, tmp_path: Path) -> None:
        body = """
            import socket
            kept = socket.socket()
        """
        match await self._start(tmp_path, body):
            case Err(PreloadRefused(reason=reason)):
                assert 'sockets' in reason
            case other:
                raise AssertionError(other)

    async def test_a_failed_import_names_its_cause(self, tmp_path: Path) -> None:
        match await self._start(tmp_path, 'raise RuntimeError("bad app")'):
            case Err(PreloadRefused(reason=reason)):
                assert 'bad app' in reason
            case other:
                raise AssertionError(other)


@linux_only
class TestProcessState:
    """``fork_refusal`` reads this process, so each case runs in its own."""

    @staticmethod
    def _refusal(setup: str) -> str:
        code = f"""
import socket, threading, time
from horsies.core.worker.preload_main import fork_refusal
{setup}
print(fork_refusal(-1))
"""
        result = subprocess.run(
            [sys.executable, '-c', code],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def test_a_clean_process_is_safe(self) -> None:
        assert self._refusal('') == 'None'

    def test_a_python_thread_is_refused(self) -> None:
        setup = 'threading.Thread(target=time.sleep, args=(5,), daemon=True).start()'
        assert 'threads' in self._refusal(setup)

    def test_a_socket_is_refused(self) -> None:
        assert 'sockets' in self._refusal('kept = socket.socket()')
