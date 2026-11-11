"""A lost child whose exit was not seen leaves its row to stale-task recovery.

Three slot paths report a loss before the child's exit is seen: a lost
channel, a child that sent a bad frame, and a slot that is cleared at a stop
or after a defect. Each test builds that loss with the real slot code, then
hands it to the real finalizer, and asserts that crash recovery wrote
nothing.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import logging
import socket
import sys
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from horsies.core.models.recovery import RecoveryConfig
from horsies.core.types.result import Ok
from horsies.core.worker import slots
from horsies.core.worker.child_protocol import InitFrame, IsolatedChild, RunFrame
from horsies.core.worker.runtime import _RequeueOutcome
from horsies.core.worker.slots import (
    Child,
    ChildLost,
    ExitUnknown,
    KilledBySignal,
    LossCause,
    SlotHandle,
    SlotPool,
    SlotPoolConfig,
)
from horsies.core.worker.stored_timeout import NoTimeout
from horsies.core.worker.worker import Worker, WorkerConfig

# A pid that cannot exist, so the kill of the double reaches no process.
NO_SUCH_PID = 2**30
TASK_ID = 'task-1'
# The real bound is 30 s; the double never ends, so a short bound is enough.
SHORT_KILL_WAIT_S = 0.05
TEST_WAIT_S = 5.0


class _StuckProcess:
    """A process that ignores SIGKILL, as one stuck in the kernel does."""

    pid = NO_SUCH_PID
    returncode = None

    async def wait(self) -> int:
        await asyncio.Event().wait()
        raise AssertionError('a stuck process does not end')


@dataclass(slots=True)
class _StuckChild:
    child: Child
    peer: socket.socket
    handle: SlotHandle = field(init=False)

    def close(self) -> None:
        self.child._exit.cancel()
        self.child._transport.abort()
        self.peer.close()


def _init() -> InitFrame:
    return InitFrame(
        app_locator='ok',
        imports=(),
        sys_path_roots=(),
        parent_sys_path=tuple(sys.path),
        parent_argv=('pytest',),
        loglevel=30,
        database_url='postgresql://unused',
        connect_kwargs={},
        child_pool_min_size=0,
        child_pool_max_size=1,
        child_pool_check=False,
        process_name='test-child',
        child_kind=IsolatedChild(),
    )


def _frame() -> RunFrame:
    return RunFrame(
        task_id=TASK_ID,
        task_name='hang',
        args_json=None,
        kwargs_json=None,
        worker_id='worker-1',
        is_workflow_task=False,
        timeout=NoTimeout(),
        claimed_at=None,
    )


@pytest_asyncio.fixture
async def stuck() -> AsyncIterator[_StuckChild]:
    parent_sock, peer = socket.socketpair()
    child = await Child._attach(
        cast('asyncio.subprocess.Process', _StuckProcess()),
        parent_sock,
        _init(),
    )
    double = _StuckChild(child=child, peer=peer)
    double.handle = SlotHandle(_frame(), asyncio.get_running_loop())
    double.handle.bind_child(child)
    yield double
    double.close()


@pytest.fixture(autouse=True)
def short_kill_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(slots, 'CHILD_KILL_WAIT_S', SHORT_KILL_WAIT_S)


def _worker(
    recovery: RecoveryConfig | None = RecoveryConfig(),
) -> tuple[Worker, AsyncMock]:
    """Return a worker and its crash recovery, which records each call.
    The default config turns stale RUNNING recovery on."""
    worker = Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
            recovery_config=recovery,
        ),
    )
    recovery = AsyncMock(return_value=_RequeueOutcome.REQUEUED)
    worker._recover_worker_future_failure = recovery  # type: ignore[method-assign]
    return worker, recovery


async def _finalize(worker: Worker, handle: SlotHandle) -> object:
    return await asyncio.wait_for(
        worker._finalize_after(handle, TASK_ID, task_name='hang'),
        TEST_WAIT_S,
    )


def _loss(handle: SlotHandle) -> ChildLost:
    match handle.outcome.result():
        case ChildLost() as lost:
            return lost
        case other:
            raise AssertionError(f'expected a lost child, got {other!r}')


async def _assert_left_to_stale_recovery(
    handle: SlotHandle,
    caplog: pytest.LogCaptureFixture,
) -> None:
    lost = _loss(handle)
    assert lost.child_exit == ExitUnknown()
    assert lost.pid == NO_SUCH_PID
    worker, recovery = _worker()
    caplog.clear()

    with caplog.at_level(logging.CRITICAL, logger='horsies.worker'):
        result = await _finalize(worker, handle)

    assert result == Ok(None)
    recovery.assert_not_awaited()
    critical = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert len(critical) == 1
    assert TASK_ID in critical[0].message
    assert f'pid={NO_SUCH_PID}' in critical[0].message


@pytest.mark.unit
@pytest.mark.asyncio
class TestLossWithNoExitSeen:
    async def test_a_lost_channel(
        self,
        stuck: _StuckChild,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        stuck.peer.close()
        match await asyncio.wait_for(stuck.child.await_frame(None), TEST_WAIT_S):
            case ChildLost() as lost:
                stuck.handle.finish(lost)
            case other:
                raise AssertionError(f'expected a lost child, got {other!r}')

        assert _loss(stuck.handle).cause is LossCause.CHANNEL_LOST
        await _assert_left_to_stale_recovery(stuck.handle, caplog)

    async def test_a_child_that_sent_a_bad_frame(
        self,
        stuck: _StuckChild,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        stuck.handle.finish(await stuck.child.discard_violating_child())

        assert _loss(stuck.handle).cause is LossCause.PROTOCOL_VIOLATION
        await _assert_left_to_stale_recovery(stuck.handle, caplog)

    async def test_a_cleared_slot(
        self,
        stuck: _StuckChild,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        pool = SlotPool(
            SlotPoolConfig(
                slots=1,
                init=_init(),
                max_tasks_per_child=0,
                max_memory_per_child_mb=None,
            ),
            pending_limit=1,
        )
        slot = pool._slots[0]
        slot.child = stuck.child
        slot.handle = stuck.handle

        await pool._clear_slot(slot)

        assert _loss(stuck.handle).cause is LossCause.SLOT_DEFECT
        await _assert_left_to_stale_recovery(stuck.handle, caplog)


class _LateProcess:
    """A process that ends only when ``ended`` is set, long after its kill."""

    pid = NO_SUCH_PID

    def __init__(self) -> None:
        self.ended = asyncio.Event()
        self.returncode: int | None = None

    async def wait(self) -> int:
        await self.ended.wait()
        self.returncode = -9
        return self.returncode


@pytest.mark.unit
@pytest.mark.asyncio
class TestLossWithStaleRecoveryOff:
    """Nothing else moves the row, so the worker waits for the exit that the
    slot did not see, and recovers the row once it sees it."""

    async def _late_loss(self) -> tuple[_LateProcess, Child, socket.socket, SlotHandle]:
        process = _LateProcess()
        parent_sock, peer = socket.socketpair()
        child = await Child._attach(
            cast('asyncio.subprocess.Process', process),
            parent_sock,
            _init(),
        )
        handle = SlotHandle(_frame(), asyncio.get_running_loop())
        handle.bind_child(child)
        handle.finish(await child.discard_violating_child())
        assert _loss(handle).child_exit == ExitUnknown()
        return process, child, peer, handle

    async def test_the_row_is_recovered_once_the_exit_is_seen(self) -> None:
        process, child, peer, handle = await self._late_loss()
        worker, recovery = _worker(recovery=None)
        try:
            waiting = asyncio.ensure_future(
                worker._finalize_after(handle, TASK_ID, task_name='hang'),
            )
            await asyncio.sleep(0.05)
            assert not waiting.done()
            recovery.assert_not_awaited()

            process.ended.set()
            await asyncio.wait_for(waiting, TEST_WAIT_S)

            recovery.assert_awaited_once()
            assert recovery.await_args is not None
            assert 'signal 9 (SIGKILL)' in recovery.await_args.args[1]
        finally:
            child._transport.abort()
            peer.close()

    async def test_a_stop_before_the_exit_writes_nothing(self) -> None:
        process, child, peer, handle = await self._late_loss()
        worker, recovery = _worker(recovery=None)
        try:
            waiting = asyncio.ensure_future(
                worker._finalize_after(handle, TASK_ID, task_name='hang'),
            )
            await asyncio.sleep(0.05)

            worker._stop.set()
            result = await asyncio.wait_for(waiting, TEST_WAIT_S)

            assert result == Ok(None)
            recovery.assert_not_awaited()
        finally:
            process.ended.set()
            await asyncio.sleep(0)
            child._transport.abort()
            peer.close()


@pytest.mark.unit
@pytest.mark.asyncio
class TestLossWithTheExitSeen:
    async def test_crash_recovery_writes_the_row(self) -> None:
        handle = SlotHandle(_frame(), asyncio.get_running_loop())
        handle.finish(
            ChildLost(
                cause=LossCause.CHILD_DIED,
                child_exit=KilledBySignal(9),
                partial_frame=False,
                pid=4242,
            ),
        )
        worker, recovery = _worker()

        await _finalize(worker, handle)

        recovery.assert_awaited_once()
