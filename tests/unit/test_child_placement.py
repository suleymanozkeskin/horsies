"""Where the worker places its children and itself (``ParentCpu.OWN_CORE``).

A child gets its CPUs before its init frame; a child whose CPUs the OS
refuses is killed and its start fails. The worker's start fails when the
placement is refused, and places its own threads after the children start.
The OS calls are replaced: these tests run on any platform.
"""

from __future__ import annotations

import asyncio
import signal
import socket
import sys
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest

from horsies.core.cli import execution_settings
from horsies.core.models.boot_failure import SpawnFailed
from horsies.core.models.worker_profile import (
    IsolatedChildren,
    ResolvedPool,
    WorkerProfile,
    describe_worker_profile,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker import slots as slots_module
from horsies.core.worker import worker as worker_module
from horsies.core.worker.child_protocol import InitFrame, IsolatedChild
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.cpu_placement import (
    NotLinux,
    OwnCore,
    ParentCpu,
    PlaceRefused,
    SharedCpus,
    parse_cpu_list,
    plan_cpu_placement,
)
from horsies.core.worker.runtime import ChildStartFailedError
from horsies.core.worker.slots import BootFailure, Child, SlotPool, SlotPoolConfig
from horsies.core.worker.worker import Worker

pytestmark = [pytest.mark.unit]

CHILD_PID = 4242
SCRIPTED_CHILD = str(Path(__file__).parent.parent / 'fixtures' / 'slots' / 'scripted_child.py')
OWN_CORE = OwnCore(parent=frozenset({6, 7}), children=frozenset({8, 9}))


class _Process:
    """A started child process that has not read its init frame."""

    pid = CHILD_PID
    returncode: int | None = None

    def __init__(self) -> None:
        self.waited = False

    async def wait(self) -> int | None:
        self.waited = True
        return -signal.SIGKILL


def _init() -> InitFrame:
    """The init frame of the slot pool. ``app_locator`` is the boot mode
    of the scripted child: ``ok`` boots it with no other step."""
    return InitFrame(
        app_locator='ok',
        imports=(),
        sys_path_roots=(),
        parent_sys_path=(),
        parent_argv=(),
        loglevel=20,
        database_url='postgresql://u:p@localhost/db',
        connect_kwargs={},
        process_name='horsies-child',
        child_kind=IsolatedChild(
            pool=ResolvedPool(min_size=1, max_size=1, check=False),
            task_send_pool=1,
        ),
    )


def _worker(parent_cpu: ParentCpu = ParentCpu.SHARED) -> Worker:
    return Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
            parent_cpu=parent_cpu,
        ),
    )


@pytest.mark.asyncio
async def test_a_refused_child_placement_kills_the_child_and_fails_its_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    killed: list[tuple[int, int]] = []

    def _refuse(pid: int, cpus: frozenset[int]) -> Result[None, PlaceRefused]:
        del cpus
        return Err(PlaceRefused(target=pid, cause='OSError: [Errno 22]'))

    def _kill(pid: int, sig: int) -> None:
        killed.append((pid, sig))

    monkeypatch.setattr(slots_module, 'place_process', _refuse)
    monkeypatch.setattr(slots_module.os, 'kill', _kill)
    attached = MagicMock()
    monkeypatch.setattr(Child, '_attach', attached)
    process = _Process()
    parent_sock, child_sock = socket.socketpair()
    registry: set[int] = set()

    started = await Child._attach_registered(  # pyright: ignore[reportPrivateUsage]
        cast('asyncio.subprocess.Process', process),
        parent_sock,
        _init(),
        registry,
        OWN_CORE,
    )

    match started:
        case BootFailure(cause=SpawnFailed(), detail=detail):
            assert str(CHILD_PID) in detail
        case other:
            raise AssertionError(f'expected a SpawnFailed boot failure, got {other!r}')
    assert killed == [(CHILD_PID, signal.SIGKILL)]
    assert process.waited
    assert registry == set()
    assert parent_sock.fileno() == -1
    attached.assert_not_called()
    child_sock.close()


@pytest.mark.asyncio
async def test_a_child_is_placed_before_its_init_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    steps: list[str] = []

    def _place(pid: int, cpus: frozenset[int]) -> Result[None, PlaceRefused]:
        steps.append(f'placed {pid} on {sorted(cpus)}')
        return Ok(None)

    async def _attach(
        process: object,
        parent_sock: socket.socket,
        init: InitFrame,
    ) -> MagicMock:
        del process, parent_sock, init
        steps.append('init frame sent')
        return MagicMock()

    monkeypatch.setattr(slots_module, 'place_process', _place)
    monkeypatch.setattr(Child, '_attach', _attach)
    parent_sock, child_sock = socket.socketpair()

    _ = await Child._attach_registered(  # pyright: ignore[reportPrivateUsage]
        cast('asyncio.subprocess.Process', _Process()),
        parent_sock,
        _init(),
        set(),
        OWN_CORE,
    )

    assert steps == [f'placed {CHILD_PID} on [8, 9]', 'init frame sent']
    parent_sock.close()
    child_sock.close()


@pytest.mark.asyncio
async def test_shared_cpus_place_no_child(monkeypatch: pytest.MonkeyPatch) -> None:
    placed = MagicMock()

    async def _attach(
        process: object,
        parent_sock: socket.socket,
        init: InitFrame,
    ) -> MagicMock:
        del process, parent_sock, init
        return MagicMock()

    monkeypatch.setattr(slots_module, 'place_process', placed)
    monkeypatch.setattr(Child, '_attach', _attach)
    parent_sock, child_sock = socket.socketpair()

    _ = await Child._attach_registered(  # pyright: ignore[reportPrivateUsage]
        cast('asyncio.subprocess.Process', _Process()),
        parent_sock,
        _init(),
        set(),
        SharedCpus(),
    )

    placed.assert_not_called()
    parent_sock.close()
    child_sock.close()


@pytest.mark.asyncio
async def test_a_refused_placement_stops_the_worker_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _not_linux(setting: ParentCpu) -> Result[OwnCore, NotLinux]:
        del setting
        return Err(NotLinux(platform='darwin'))

    monkeypatch.setattr(worker_module, 'plan_cpu_placement', _not_linux)
    worker = _worker(ParentCpu.OWN_CORE)

    with pytest.raises(ChildStartFailedError, match='Linux only'):
        await worker._start_slots()  # pyright: ignore[reportPrivateUsage]

    assert worker._slots is None  # pyright: ignore[reportPrivateUsage]


def test_the_worker_places_its_threads_on_its_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    placed: list[frozenset[int]] = []

    def _place(cpus: frozenset[int]) -> Result[int, PlaceRefused]:
        placed.append(cpus)
        return Ok(5)

    monkeypatch.setattr(worker_module, 'place_worker_threads', _place)
    worker = _worker(ParentCpu.OWN_CORE)
    worker._cpu_placement = OWN_CORE  # pyright: ignore[reportPrivateUsage]

    worker._place_worker_process()  # pyright: ignore[reportPrivateUsage]

    assert placed == [frozenset({6, 7})]
    assert worker._slot_pool_config().placement == OWN_CORE  # pyright: ignore[reportPrivateUsage]


def test_a_refused_worker_thread_stops_the_start(monkeypatch: pytest.MonkeyPatch) -> None:
    def _refuse(cpus: frozenset[int]) -> Result[int, PlaceRefused]:
        del cpus
        return Err(PlaceRefused(target=77, cause='OSError: [Errno 22]'))

    monkeypatch.setattr(worker_module, 'place_worker_threads', _refuse)
    worker = _worker(ParentCpu.OWN_CORE)
    worker._cpu_placement = OWN_CORE  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(ChildStartFailedError, match='worker thread 77'):
        worker._place_worker_process()  # pyright: ignore[reportPrivateUsage]


def test_shared_cpus_place_no_worker_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    placed = MagicMock()
    monkeypatch.setattr(worker_module, 'place_worker_threads', placed)

    _worker()._place_worker_process()  # pyright: ignore[reportPrivateUsage]

    placed.assert_not_called()


def test_the_profile_setting_reaches_the_worker_settings() -> None:
    profile = WorkerProfile(isolated=IsolatedChildren(count=2), parent_cpu=ParentCpu.OWN_CORE)

    assert execution_settings(profile).parent_cpu is ParentCpu.OWN_CORE
    assert 'parent_cpu=OWN_CORE' in describe_worker_profile(profile)
    assert WorkerProfile(isolated=IsolatedChildren(count=2)).parent_cpu is ParentCpu.SHARED


@pytest.mark.asyncio
async def test_the_start_places_the_worker_after_its_children_started(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Entered through ``_start_slots``: the plan goes into the pool config,
    the pool starts, and then the worker places its own threads."""
    steps: list[str] = []

    class _Pool:
        def __init__(self, config: object, pending_limit: int) -> None:
            del pending_limit
            steps.append(f'pool built with {getattr(config, "placement")!r}')

        async def start(self) -> Ok[tuple[()]]:
            steps.append('children started')
            return Ok(())

    def _own_core(setting: ParentCpu) -> Result[OwnCore, NotLinux]:
        del setting
        return Ok(OWN_CORE)

    def _no_heartbeat(pool: object) -> MagicMock:
        del pool
        return MagicMock()

    def _no_baseline_check(baselines: object) -> None:
        del baselines

    monkeypatch.setattr(worker_module, 'plan_cpu_placement', _own_core)
    monkeypatch.setattr(worker_module, 'SlotPool', _Pool)
    worker = _worker(ParentCpu.OWN_CORE)
    monkeypatch.setattr(worker, '_launch_runner_heartbeat', _no_heartbeat)
    monkeypatch.setattr(worker, '_check_memory_baseline', _no_baseline_check)
    monkeypatch.setattr(
        worker,
        '_place_worker_process',
        lambda: steps.append('worker placed'),
    )

    await worker._start_slots()  # pyright: ignore[reportPrivateUsage]

    assert steps == [f'pool built with {OWN_CORE!r}', 'children started', 'worker placed']


def _allowed_list(pid: int) -> frozenset[int]:
    """The CPUs of ``pid`` from ``/proc/<pid>/status``."""
    for line in Path(f'/proc/{pid}/status').read_text().splitlines():
        if line.startswith('Cpus_allowed_list:'):
            match parse_cpu_list(line.split(':', 1)[1]):
                case Ok(cpus):
                    return cpus
                case Err(reason):
                    raise AssertionError(reason)
    raise AssertionError(f'no Cpus_allowed_list for {pid}')


@pytest.mark.skipif(sys.platform != 'linux', reason='CPU placement is Linux only')
@pytest.mark.asyncio
async def test_real_children_start_on_the_children_cpus() -> None:
    """Two children started through a slot pool, on this host's own split."""
    match plan_cpu_placement(ParentCpu.OWN_CORE):
        case Ok(OwnCore() as placement):
            pass
        case other:
            pytest.skip(f'this host cannot split its CPUs: {other!r}')
    config = SlotPoolConfig(
        slots=2,
        init=_init(),
        max_tasks_per_child=0,
        max_memory_per_child_mb=None,
        child_entry=(SCRIPTED_CHILD,),
        placement=placement,
    )
    pool = SlotPool(config, pending_limit=100)
    match await pool.start():
        case Ok(_):
            pass
        case Err(failure):
            await pool.stop(0)
            raise AssertionError(failure)
    try:
        pids = [
            slot.child.pid
            for slot in pool._slots  # pyright: ignore[reportPrivateUsage]
            if slot.child is not None
        ]
        assert len(pids) == 2
        assert {pid: _allowed_list(pid) for pid in pids} == dict.fromkeys(
            pids,
            placement.children,
        )
    finally:
        await pool.stop(0)
