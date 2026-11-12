"""Machine and server facts of a worker profile: per-child memory limits
against the RAM, and the Postgres connections of one worker against the
server limit (``horsies check --live``)."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from collections.abc import AsyncGenerator
from typing import Any
from unittest import mock

import pytest
from pydantic import SecretStr
from sqlalchemy.exc import OperationalError

from horsies.core.app import Horsies
from horsies.core.errors import ErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.child_sizing import (
    DEFAULT_TASK_SEND_POOL_CONNECTIONS,
    PARENT_POOL_CONNECTIONS_FLOOR,
    parent_pool_connections,
    shared_async_pool_connections,
    shared_thread_pool_connections,
)
from horsies.core.models.worker_profile import (
    IsolatedChildren,
    IsolatedPool,
    IsolatedRecycle,
    MemoryLimit,
    NoIsolatedChildren,
    ParentPool,
    PoolSize,
    SharedAsyncChildren,
    SharedPool,
    SharedRecycle,
    SharedThreadChildren,
    WorkerProfile,
)
from horsies.core.worker import profile_fit
from horsies.core.worker.profile_fit import (
    CGROUP_FILE_MAX_BYTES,
    CGROUP_PATH_MAX_DEPTH,
    LISTENER_CONNECTIONS_PER_WORKER,
    RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER,
    CgroupLimit,
    CgroupRoots,
    CgroupUnreadable,
    ChildMode,
    ConnectionsExceedServer,
    ConnectionsFit,
    MachineMemory,
    MachineMemoryUnreadable,
    MemoryLimitsExceedMachine,
    MemoryLimitsFit,
    MemoryLimitsNotSet,
    MemorySource,
    ModePools,
    InCgroup,
    NoCgroupLimit,
    NoCgroupPath,
    ProcCgroup,
    ServerConnectionLimit,
    ServerConnectionLimitUnreadable,
    UsableMemory,
    UsableMemoryUnreadable,
    WorkerConnectionNeed,
    compare_connections,
    compare_memory_limits,
    parse_proc_cgroup,
    read_cgroup_memory_limit,
    read_machine_memory,
    read_server_connection_limit,
    usable_memory,
    worker_connection_need,
)

pytestmark = [pytest.mark.unit]

_URL = 'postgresql+psycopg://user:pass@localhost/db'


def _limited(count: int, mb: int) -> WorkerProfile:
    return WorkerProfile(
        isolated=IsolatedChildren(
            count=count,
            recycle=IsolatedRecycle(above=MemoryLimit(mb=mb)),
        ),
    )


def _broker(**fields: Any) -> PostgresConfig:
    return PostgresConfig(database_url=SecretStr(_URL), **fields)


def _host_ram(mb: int) -> UsableMemory:
    return UsableMemory(mb=mb, source=MemorySource.HOST_RAM)


class TestCompareMemoryLimits:
    def test_sum_above_the_usable_memory_exceeds(self) -> None:
        outcome = compare_memory_limits(_limited(4, 512), _host_ram(2047))
        assert outcome == MemoryLimitsExceedMachine(total_mb=2048, machine_mb=2047)

    def test_sum_equal_to_the_usable_memory_fits(self) -> None:
        outcome = compare_memory_limits(_limited(4, 512), _host_ram(2048))
        assert outcome == MemoryLimitsFit(total_mb=2048, machine_mb=2048)

    def test_no_memory_limit_has_no_sum(self) -> None:
        profile = WorkerProfile(isolated=IsolatedChildren(count=4))
        assert compare_memory_limits(profile, _host_ram(1)) == MemoryLimitsNotSet()

    def test_the_sum_covers_shared_async_children(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(
                count=2,
                recycle=IsolatedRecycle(above=MemoryLimit(mb=256)),
            ),
            shared_async=SharedAsyncChildren(
                count=3,
                seats=10,
                recycle=SharedRecycle(above=MemoryLimit(mb=1000)),
            ),
        )
        outcome = compare_memory_limits(profile, _host_ram(3511))
        assert outcome == MemoryLimitsExceedMachine(total_mb=3512, machine_mb=3511)

    def test_shared_children_with_no_memory_limit_have_no_sum(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(
                count=2,
                recycle=IsolatedRecycle(above=MemoryLimit(mb=256)),
            ),
            shared_async=SharedAsyncChildren(count=1, seats=10),
        )
        assert compare_memory_limits(profile, _host_ram(1)) == MemoryLimitsNotSet()

    def test_the_sum_covers_shared_thread_children(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(
                count=1,
                recycle=IsolatedRecycle(above=MemoryLimit(mb=256)),
            ),
            shared_thread=SharedThreadChildren(
                count=2,
                seats=8,
                recycle=SharedRecycle(above=MemoryLimit(mb=700)),
            ),
        )
        outcome = compare_memory_limits(profile, _host_ram(1655))
        assert outcome == MemoryLimitsExceedMachine(total_mb=1656, machine_mb=1655)

    def test_thread_children_with_no_memory_limit_have_no_sum(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(
                count=2,
                recycle=IsolatedRecycle(above=MemoryLimit(mb=256)),
            ),
            shared_thread=SharedThreadChildren(count=1, seats=4),
        )
        assert compare_memory_limits(profile, _host_ram(10**6)) == MemoryLimitsNotSet()

    def test_only_shared_children_are_summed(self) -> None:
        profile = WorkerProfile(
            shared_async=SharedAsyncChildren(
                count=4,
                seats=10,
                recycle=SharedRecycle(above=MemoryLimit(mb=500)),
            ),
        )
        outcome = compare_memory_limits(profile, _host_ram(2000))
        assert outcome == MemoryLimitsFit(total_mb=2000, machine_mb=2000)


_MIB = 1024 * 1024
# The cgroup v1 value for "no limit" on 64-bit Linux with 4 KiB pages.
_V1_NO_LIMIT = 9_223_372_036_854_771_712


class _CgroupFiles:
    """Fake ``/proc/self/cgroup`` and cgroup v2 and v1 trees under ``tmp_path``.

    ``proc`` does not exist until a test writes it: the reader then falls
    back to the limit files at the mount points, ``v2`` and ``v1``.
    """

    def __init__(self, root: Path) -> None:
        self.proc = root / 'proc' / 'cgroup'
        self.v2_root = root / 'v2'
        self.v1_root = root / 'v1'
        self.v2 = self.v2_root / 'memory.max'
        self.v1 = self.v1_root / 'memory.limit_in_bytes'

    def v2_group(self, path: str) -> Path:
        return self.v2_root / path / 'memory.max'

    def v1_group(self, path: str) -> Path:
        return self.v1_root / path / 'memory.limit_in_bytes'

    def write(self, path: Path, content: str | bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        match content:
            case str():
                path.write_text(content)
            case bytes():
                path.write_bytes(content)

    def read(self) -> CgroupLimit | NoCgroupLimit | CgroupUnreadable:
        return read_cgroup_memory_limit(
            CgroupRoots(
                proc_self_cgroup=self.proc,
                v2_root=self.v2_root,
                v1_memory_root=self.v1_root,
            ),
        )


@pytest.fixture
def cgroup(tmp_path: Path) -> _CgroupFiles:
    return _CgroupFiles(tmp_path)


class TestReadCgroupMemoryLimit:
    def test_v2_number_is_the_limit(self, cgroup: _CgroupFiles) -> None:
        cgroup.write(cgroup.v2, '536870912\n')
        assert cgroup.read() == CgroupLimit(limit_bytes=536_870_912)

    def test_v2_max_is_no_limit(self, cgroup: _CgroupFiles) -> None:
        cgroup.write(cgroup.v2, 'max\n')
        assert cgroup.read() == NoCgroupLimit()

    def test_v2_is_read_before_v1(self, cgroup: _CgroupFiles) -> None:
        cgroup.write(cgroup.v2, 'max\n')
        cgroup.write(cgroup.v1, '536870912\n')
        assert cgroup.read() == NoCgroupLimit()

    def test_v1_number_is_the_limit_when_v2_is_absent(
        self,
        cgroup: _CgroupFiles,
    ) -> None:
        cgroup.write(cgroup.v1, '1073741824\n')
        assert cgroup.read() == CgroupLimit(limit_bytes=1_073_741_824)

    def test_no_file_is_no_limit(self, cgroup: _CgroupFiles) -> None:
        assert cgroup.read() == NoCgroupLimit()

    @pytest.mark.parametrize('content', ['', 'lots', '-1', '12 MB', '1e9'])
    def test_unexpected_content_is_unreadable(
        self,
        cgroup: _CgroupFiles,
        content: str,
    ) -> None:
        cgroup.write(cgroup.v2, content)
        outcome = cgroup.read()
        assert isinstance(outcome, CgroupUnreadable)
        assert outcome.reason == f'{cgroup.v2}: unexpected content {content!r}'

    def test_a_file_that_cannot_be_opened_is_unreadable(
        self,
        cgroup: _CgroupFiles,
    ) -> None:
        cgroup.v2.mkdir(parents=True)
        outcome = cgroup.read()
        assert isinstance(outcome, CgroupUnreadable)
        assert outcome.reason.startswith(f'{cgroup.v2}: IsADirectoryError: ')

    def test_a_file_above_the_read_bound_is_unreadable(
        self,
        cgroup: _CgroupFiles,
    ) -> None:
        cgroup.write(cgroup.v2, '1' * (CGROUP_FILE_MAX_BYTES + 1))
        assert cgroup.read() == CgroupUnreadable(
            reason=f'{cgroup.v2}: more than {CGROUP_FILE_MAX_BYTES} bytes',
        )

    def test_a_file_at_the_read_bound_is_read(self, cgroup: _CgroupFiles) -> None:
        cgroup.write(cgroup.v2, '1' * CGROUP_FILE_MAX_BYTES)
        assert cgroup.read() == CgroupLimit(limit_bytes=int('1' * CGROUP_FILE_MAX_BYTES))

    def test_non_ascii_content_is_unreadable(self, cgroup: _CgroupFiles) -> None:
        cgroup.write(cgroup.v1, b'\xff\xfe')
        assert cgroup.read() == CgroupUnreadable(reason=f'{cgroup.v1}: not ASCII text')


_GIB = 1024 * 1024 * 1024


class TestParseProcCgroup:
    def test_the_v2_line_names_the_unified_group(self) -> None:
        parsed = parse_proc_cgroup('0::/system.slice/worker.service\n')

        assert parsed == ProcCgroup(
            v2=InCgroup(('system.slice', 'worker.service')),
            v1_memory=NoCgroupPath(),
        )
        assert parsed == parse_proc_cgroup('0::/system.slice//worker.service/\n')

    def test_a_v1_line_with_the_memory_controller_names_the_memory_group(self) -> None:
        parsed = parse_proc_cgroup(
            '12:cpu,cpuacct:/docker/abc\n'
            '4:memory:/docker/abc\n'
            '1:name=systemd:/docker/abc\n'
            '0::/docker/abc\n',
        )

        assert parsed.v1_memory == InCgroup(('docker', 'abc'))

    @pytest.mark.parametrize(
        'line',
        [
            '0::/../../user.slice',
            '0::/' + '/'.join(['g'] * (CGROUP_PATH_MAX_DEPTH + 1)),
            'not a line',
        ],
        ids=['leaves the mount point', 'too deep', 'malformed'],
    )
    def test_a_path_that_cannot_be_walked_names_no_group(self, line: str) -> None:
        assert parse_proc_cgroup(line).v2 == NoCgroupPath()


class TestCgroupOfThisProcess:
    """``/proc/self/cgroup`` names the group; the limit can sit on the group
    or on a parent group, below the mount point."""

    def test_a_systemd_memory_max_of_the_service_is_the_limit(
        self,
        cgroup: _CgroupFiles,
    ) -> None:
        cgroup.write(cgroup.proc, '0::/system.slice/worker.service\n')
        cgroup.write(cgroup.v2_group('system.slice/worker.service'), '536870912\n')

        assert cgroup.read() == CgroupLimit(limit_bytes=536_870_912)

    def test_a_limit_of_a_parent_slice_holds_for_the_group(
        self,
        cgroup: _CgroupFiles,
    ) -> None:
        cgroup.write(cgroup.proc, '0::/workers.slice/worker.service\n')
        cgroup.write(cgroup.v2_group('workers.slice/worker.service'), 'max\n')
        cgroup.write(cgroup.v2_group('workers.slice'), f'{_GIB}\n')

        assert cgroup.read() == CgroupLimit(limit_bytes=_GIB)

    @pytest.mark.parametrize(
        ('service', 'slice_', 'limit'),
        [(2 * _GIB, _GIB, _GIB), (_GIB // 2, _GIB, _GIB // 2)],
        ids=['the parent is smaller', 'the group is smaller'],
    )
    def test_the_smallest_limit_on_the_path_holds(
        self,
        cgroup: _CgroupFiles,
        service: int,
        slice_: int,
        limit: int,
    ) -> None:
        cgroup.write(cgroup.proc, '0::/workers.slice/worker.service\n')
        cgroup.write(cgroup.v2_group('workers.slice/worker.service'), f'{service}\n')
        cgroup.write(cgroup.v2_group('workers.slice'), f'{slice_}\n')

        assert cgroup.read() == CgroupLimit(limit_bytes=limit)

    def test_a_container_seen_with_the_host_cgroup_namespace(
        self,
        cgroup: _CgroupFiles,
    ) -> None:
        cgroup.write(cgroup.proc, '0::/system.slice/docker-abc.scope\n')
        cgroup.write(cgroup.v2_group('system.slice/docker-abc.scope'), f'{_GIB}\n')

        assert cgroup.read() == CgroupLimit(limit_bytes=_GIB)

    def test_a_v1_memory_group_is_read_before_v2(self, cgroup: _CgroupFiles) -> None:
        cgroup.write(cgroup.proc, '4:memory:/docker/abc\n0::/docker/abc\n')
        cgroup.write(cgroup.v1_group('docker/abc'), f'{_GIB}\n')
        cgroup.write(cgroup.v2_group('docker/abc'), 'max\n')

        assert cgroup.read() == CgroupLimit(limit_bytes=_GIB)

    def test_a_v1_group_not_mounted_inside_reads_the_mount_point(
        self,
        cgroup: _CgroupFiles,
    ) -> None:
        """A container on cgroup v1 sees its own group at the mount point,
        while ``/proc/self/cgroup`` names its path on the host."""
        cgroup.write(cgroup.proc, '4:memory:/docker/abc\n')
        cgroup.write(cgroup.v1, f'{_GIB}\n')

        assert cgroup.read() == CgroupLimit(limit_bytes=_GIB)

    def test_a_path_that_leaves_the_mount_point_reads_the_mount_point(
        self,
        cgroup: _CgroupFiles,
    ) -> None:
        cgroup.write(cgroup.proc, '0::/../outside\n')
        cgroup.write(cgroup.v2, f'{_GIB}\n')

        assert cgroup.read() == CgroupLimit(limit_bytes=_GIB)

    def test_an_unreadable_file_on_the_path_is_unreadable(
        self,
        cgroup: _CgroupFiles,
    ) -> None:
        cgroup.write(cgroup.proc, '0::/workers.slice/worker.service\n')
        cgroup.write(cgroup.v2_group('workers.slice'), 'lots\n')

        outcome = cgroup.read()

        assert outcome == CgroupUnreadable(
            reason=f"{cgroup.v2_group('workers.slice')}: unexpected content 'lots'",
        )

    def test_a_group_with_no_limit_file_on_its_path_has_no_limit(
        self,
        cgroup: _CgroupFiles,
    ) -> None:
        cgroup.write(cgroup.proc, '0::/user.slice/session.scope\n')

        assert cgroup.read() == NoCgroupLimit()


class TestUsableMemory:
    def test_a_container_limit_below_the_host_ram_wins(self) -> None:
        outcome = usable_memory(
            MachineMemory(mb=16_384),
            CgroupLimit(limit_bytes=2048 * _MIB),
        )
        assert outcome == UsableMemory(mb=2048, source=MemorySource.CONTAINER_LIMIT)

    def test_a_container_limit_equal_to_the_host_ram_is_host_ram(self) -> None:
        outcome = usable_memory(
            MachineMemory(mb=2048),
            CgroupLimit(limit_bytes=2048 * _MIB),
        )
        assert outcome == UsableMemory(mb=2048, source=MemorySource.HOST_RAM)

    def test_the_v1_no_limit_value_is_host_ram(self) -> None:
        outcome = usable_memory(
            MachineMemory(mb=16_384),
            CgroupLimit(limit_bytes=_V1_NO_LIMIT),
        )
        assert outcome == UsableMemory(mb=16_384, source=MemorySource.HOST_RAM)

    def test_no_cgroup_limit_is_host_ram(self) -> None:
        outcome = usable_memory(MachineMemory(mb=16_384), NoCgroupLimit())
        assert outcome == UsableMemory(mb=16_384, source=MemorySource.HOST_RAM)

    def test_an_unreadable_cgroup_falls_back_to_host_ram(self) -> None:
        outcome = usable_memory(
            MachineMemory(mb=16_384),
            CgroupUnreadable(reason='denied'),
        )
        assert outcome == UsableMemory(mb=16_384, source=MemorySource.HOST_RAM)

    def test_a_container_limit_stands_when_the_host_ram_is_unreadable(self) -> None:
        outcome = usable_memory(
            MachineMemoryUnreadable(reason='no /proc'),
            CgroupLimit(limit_bytes=512 * _MIB),
        )
        assert outcome == UsableMemory(mb=512, source=MemorySource.CONTAINER_LIMIT)

    @pytest.mark.parametrize(
        'cgroup',
        [NoCgroupLimit(), CgroupUnreadable(reason='denied')],
    )
    def test_nothing_readable_is_unreadable(
        self,
        cgroup: NoCgroupLimit | CgroupUnreadable,
    ) -> None:
        outcome = usable_memory(MachineMemoryUnreadable(reason='no /proc'), cgroup)
        assert outcome == UsableMemoryUnreadable(reason='no /proc')


class TestReadMachineMemory:
    def test_total_bytes_become_mb(self) -> None:
        with mock.patch('psutil.virtual_memory') as virtual_memory:
            virtual_memory.return_value.total = 3 * 1024 * 1024 * 1024 + 5
            assert read_machine_memory() == MachineMemory(mb=3072)

    def test_os_error_is_unreadable(self) -> None:
        with mock.patch('psutil.virtual_memory', side_effect=OSError('no /proc')):
            outcome = read_machine_memory()
        assert outcome == MachineMemoryUnreadable(reason='OSError: no /proc')


def _session_factory(execute: Any) -> Any:
    @asynccontextmanager
    async def session() -> AsyncGenerator[Any]:
        yield mock.MagicMock(execute=execute)

    return session


def _rows(*values: int) -> Any:
    result = mock.MagicMock()
    result.one.return_value = values
    return mock.AsyncMock(return_value=result)


@pytest.mark.asyncio
class TestReadServerConnectionLimit:
    async def test_reads_max_and_both_reserved_settings(self) -> None:
        outcome = await read_server_connection_limit(_session_factory(_rows(100, 3, 2)))
        assert outcome == ServerConnectionLimit(max_connections=100, reserved=5)

    async def test_database_error_is_unreadable(self) -> None:
        execute = mock.AsyncMock(
            side_effect=OperationalError('SELECT', {}, Exception('gone')),
        )
        outcome = await read_server_connection_limit(_session_factory(execute))
        assert isinstance(outcome, ServerConnectionLimitUnreadable)
        assert outcome.reason.startswith('OperationalError: ')

    async def test_os_error_is_unreadable(self) -> None:
        execute = mock.AsyncMock(side_effect=OSError('reset'))
        outcome = await read_server_connection_limit(_session_factory(execute))
        assert outcome == ServerConnectionLimitUnreadable(reason='OSError: reset')

    async def test_no_answer_within_the_bound_is_unreadable(self) -> None:
        async def never(*_args: Any) -> None:
            await asyncio.sleep(60)

        with mock.patch.object(profile_fit, 'CONNECTION_LIMIT_READ_TIMEOUT_S', 0.01):
            outcome = await read_server_connection_limit(_session_factory(never))
        assert outcome == ServerConnectionLimitUnreadable(reason='TimeoutError: ')


class TestConnectionNeed:
    def test_parent_pool_listener_heartbeat_and_child_pools(self) -> None:
        need = worker_connection_need(
            WorkerProfile(isolated=IsolatedChildren(count=8)),
        )
        assert need == WorkerConnectionNeed(
            parent_pool=PARENT_POOL_CONNECTIONS_FLOOR,
            listener=LISTENER_CONNECTIONS_PER_WORKER,
            runner_heartbeat=RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER,
            child_pools=(
                ModePools(
                    ChildMode.ISOLATED,
                    children=8,
                    per_child=1,
                    task_send_pool=DEFAULT_TASK_SEND_POOL_CONNECTIONS,
                ),
            ),
        )
        assert LISTENER_CONNECTIONS_PER_WORKER == 2
        assert RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER == 1
        assert need.total() == 16

    def test_task_sends_add_the_task_send_pool_of_each_kind(self) -> None:
        """A task that sends opens the task_send_pool of its child kind in
        its child, for each event loop that it sends from. Each kind counts
        its own setting, and the producer pool of PostgresConfig plays no
        part."""
        need = worker_connection_need(
            WorkerProfile(
                isolated=IsolatedChildren(
                    count=2,
                    task_send_pool=PoolSize(connections=3),
                ),
                shared_async=SharedAsyncChildren(
                    count=8,
                    seats=50,
                    task_send_pool=PoolSize(connections=7),
                ),
                shared_thread=SharedThreadChildren(count=1, seats=4),
            ),
        )

        assert [pools.task_send_pool for pools in need.child_pools] == [3, 7, 2]
        assert need.task_sends() == 2 * 3 + 8 * 7 + 1 * 2
        assert need.with_task_sends() == need.total() + 64
        assert need.task_sends_note() == (
            'not counted: task code that sends opens a pool of the '
            'task_send_pool of its child kind in its child, for each event '
            'loop that it sends from; with one such loop in each child, 2 '
            'isolated children x isolated.task_send_pool 3 + 8 shared async '
            'children x shared_async.task_send_pool 7 + 1 shared thread '
            'children x shared_thread.task_send_pool 2 = 64 more, '
            f'{need.total() + 64} in all'
        )

    @pytest.mark.parametrize(
        'profile',
        [
            WorkerProfile(
                isolated=IsolatedChildren(
                    count=1,
                    task_send_pool=PoolSize(connections=4),
                ),
            ),
            WorkerProfile(
                shared_async=SharedAsyncChildren(
                    count=1,
                    seats=10,
                    task_send_pool=PoolSize(connections=6),
                ),
            ),
            WorkerProfile(shared_thread=SharedThreadChildren(count=1, seats=10)),
        ],
    )
    def test_the_task_send_term_is_the_pool_of_the_broker_of_a_child(
        self,
        profile: WorkerProfile,
    ) -> None:
        """A child sets the task-send pool of its kind at start, so
        ``app.get_broker()`` there opens exactly the per-loop term that the
        check counts for that kind."""
        broker_config = _broker(pool_size=30, max_overflow=30)
        need = worker_connection_need(profile)
        [pools] = need.child_pools
        app = Horsies(AppConfig(broker=broker_config))
        app.set_role('worker')
        app.set_task_send_pool(pools.task_send_pool)
        broker = app.get_broker()
        try:
            assert broker.config.max_overflow == 0
            assert broker.config.pool_size == pools.task_send_pool
        finally:
            _ = broker.close()

    def test_a_fit_states_what_task_code_adds(self) -> None:
        profile = WorkerProfile(isolated=IsolatedChildren(count=8))
        limit = ServerConnectionLimit(max_connections=100, reserved=3)

        outcome = compare_connections(profile, limit)

        assert isinstance(outcome, ConnectionsFit)
        assert outcome.describe() == (
            'one worker opens up to 16 Postgres connections itself, of 97 usable '
            'on the server; not counted: task code that sends opens a pool of '
            'the task_send_pool of its child kind in its child, for each event '
            'loop that it sends from; with one such loop in each child, 8 '
            'isolated children x isolated.task_send_pool 2 = 16 more, 32 in all'
        )

    def test_each_mode_counts_its_own_resolved_pool_max(self) -> None:
        """Each child counts the resolved pool max_size of its mode: the
        isolated pool, and the pools that the shared modes derive from
        their seats."""
        need = worker_connection_need(
            WorkerProfile(
                isolated=IsolatedChildren(count=2),
                shared_async=SharedAsyncChildren(count=3, seats=200),
                shared_thread=SharedThreadChildren(count=4, seats=64),
            ),
        )
        async_pool = shared_async_pool_connections(200)
        thread_pool = shared_thread_pool_connections(64)
        send = DEFAULT_TASK_SEND_POOL_CONNECTIONS
        assert need.child_pools == (
            ModePools(ChildMode.ISOLATED, 2, 1, task_send_pool=send),
            ModePools(ChildMode.SHARED_ASYNC, 3, async_pool, task_send_pool=send),
            ModePools(ChildMode.SHARED_THREAD, 4, thread_pool, task_send_pool=send),
        )
        assert need.children() == 9
        assert need.parent_pool == parent_pool_connections(2 + 3 * 200 + 4 * 64)
        assert need.total() == (
            need.parent_pool + 2 + 1 + 2 * 1 + 3 * async_pool + 4 * thread_pool
        )

    def test_a_set_pool_size_is_counted_as_set(self) -> None:
        need = worker_connection_need(
            WorkerProfile(
                isolated=IsolatedChildren(
                    count=2,
                    pool=IsolatedPool(max_size=PoolSize(connections=5)),
                ),
                shared_async=SharedAsyncChildren(
                    count=1,
                    seats=200,
                    pool=SharedPool(max_size=PoolSize(connections=7)),
                ),
                shared_thread=SharedThreadChildren(
                    count=3,
                    seats=1,
                    pool=SharedPool(max_size=PoolSize(connections=1)),
                ),
            ),
        )
        assert [(pools.mode, pools.per_child) for pools in need.child_pools] == [
            (ChildMode.ISOLATED, 5),
            (ChildMode.SHARED_ASYNC, 7),
            (ChildMode.SHARED_THREAD, 1),
        ]
        assert need.total() == 20 + 2 + 1 + 2 * 5 + 1 * 7 + 3 * 1

    def test_a_mode_without_children_has_no_term(self) -> None:
        need = worker_connection_need(
            WorkerProfile(shared_thread=SharedThreadChildren(count=2, seats=8)),
        )
        thread_pool = shared_thread_pool_connections(8)
        assert need.child_pools == (
            ModePools(
                ChildMode.SHARED_THREAD,
                children=2,
                per_child=thread_pool,
                task_send_pool=DEFAULT_TASK_SEND_POOL_CONNECTIONS,
            ),
        )
        assert need.formula() == (
            'parent pool max_size 5 + listener 2 + runner heartbeat 1 + 2 shared thread '
            f'children x shared_thread.pool max_size {thread_pool} = '
            f'{8 + 2 * thread_pool}'
        )

    def test_the_formula_names_each_mode_and_its_pool_field(self) -> None:
        need = worker_connection_need(
            WorkerProfile(
                isolated=IsolatedChildren(count=4),
                shared_async=SharedAsyncChildren(
                    count=2,
                    seats=10,
                    pool=SharedPool(max_size=PoolSize(connections=3)),
                ),
                shared_thread=SharedThreadChildren(
                    count=1,
                    seats=4,
                    pool=SharedPool(max_size=PoolSize(connections=6)),
                ),
            ),
        )
        assert need.formula() == (
            'parent pool max_size 5 + listener 2 + runner heartbeat 1 + 4 isolated '
            'children x isolated.pool max_size 1 + 2 shared async children x '
            'shared_async.pool max_size 3 + 1 shared thread children x '
            'shared_thread.pool max_size 6 = 24'
        )
        assert need.task_sends() == 7 * DEFAULT_TASK_SEND_POOL_CONNECTIONS

    def test_the_parent_pool_derived_from_the_seats_is_counted(self) -> None:
        """One shared async child of 1000 seats: the worker process derives
        a pool of 20 from the seats, and the check counts all of it."""
        need = worker_connection_need(
            WorkerProfile(shared_async=SharedAsyncChildren(count=1, seats=1000)),
        )
        assert need.parent_pool == 20
        assert need.total() == 20 + 2 + 1 + 1 * shared_async_pool_connections(1000)
        assert need.formula().startswith('parent pool max_size 20 + listener 2')

    def test_a_set_parent_pool_is_counted_as_set(self) -> None:
        need = worker_connection_need(
            WorkerProfile(
                shared_async=SharedAsyncChildren(count=1, seats=1000),
                pool=ParentPool(max_size=PoolSize(connections=40)),
            ),
        )
        assert need.parent_pool == 40
        assert need.total() == 40 + 2 + 1 + shared_async_pool_connections(1000)

    def test_usable_slots_never_go_below_zero(self) -> None:
        assert ServerConnectionLimit(max_connections=3, reserved=5).usable() == 0

    def test_need_above_usable_exceeds(self) -> None:
        profile = WorkerProfile(isolated=IsolatedChildren(count=8))
        limit = ServerConnectionLimit(max_connections=18, reserved=3)
        outcome = compare_connections(profile, limit)
        assert isinstance(outcome, ConnectionsExceedServer)
        assert outcome.describe() == (
            'one worker opens up to 16 Postgres connections itself, not '
            'counting task code; the server has 15 usable slots '
            '(max_connections 18 - reserved 3)'
        )

    def test_need_equal_to_usable_fits(self) -> None:
        profile = WorkerProfile(isolated=IsolatedChildren(count=8))
        limit = ServerConnectionLimit(max_connections=19, reserved=3)
        assert isinstance(compare_connections(profile, limit), ConnectionsFit)


def _app(
    profiles: dict[str, WorkerProfile],
    **broker_fields: Any,
) -> Horsies:
    return Horsies(
        AppConfig(broker=_broker(**broker_fields), worker_profiles=profiles),
    )


class TestCheckLiveWorkerConnections:
    """``app.check(live=True)``: each profile against the server limit."""

    def test_a_profile_above_the_limit_is_an_error_with_its_path(self) -> None:
        app = _app(
            {
                'big': WorkerProfile(isolated=IsolatedChildren(count=20)),
                'small': WorkerProfile(isolated=IsolatedChildren(count=1)),
            }
        )
        reader = mock.AsyncMock(
            return_value=ServerConnectionLimit(max_connections=30, reserved=3),
        )
        with mock.patch('horsies.core.app.read_server_connection_limit', reader):
            errors = app._check_worker_connections()  # pyright: ignore[reportPrivateUsage]

        assert [error.code for error in errors] == [ErrorCode.CHECK_WORKER_CONNECTIONS]
        assert errors[0].message == (
            "worker_profiles['big']: one worker opens up to 28 Postgres "
            'connections itself, not counting task code; the server has 27 '
            'usable slots (max_connections 30 - reserved 3)'
        )
        assert errors[0].notes == [
            'parent pool max_size 5 + listener 2 + runner heartbeat 1 + 20 isolated '
            'children x isolated.pool max_size 1 = 28',
            'not counted: task code that sends opens a pool of the '
            'task_send_pool of its child kind in its child, for each event '
            'loop that it sends from; with one such loop in each child, 20 '
            'isolated children x isolated.task_send_pool 2 = 40 more, 68 in '
            'all',
        ]
        assert errors[0].location is None

    def test_profiles_that_fit_have_no_error(self) -> None:
        app = _app({'small': WorkerProfile(isolated=IsolatedChildren(count=1))})
        reader = mock.AsyncMock(
            return_value=ServerConnectionLimit(max_connections=100, reserved=3),
        )
        with mock.patch('horsies.core.app.read_server_connection_limit', reader):
            assert app._check_worker_connections() == []  # pyright: ignore[reportPrivateUsage]
        reader.assert_awaited_once()

    def test_an_unreadable_limit_is_one_error(self) -> None:
        app = _app({'small': WorkerProfile(isolated=IsolatedChildren(count=1))})
        reader = mock.AsyncMock(
            return_value=ServerConnectionLimitUnreadable(reason='OSError: reset'),
        )
        with mock.patch('horsies.core.app.read_server_connection_limit', reader):
            errors = app._check_worker_connections()  # pyright: ignore[reportPrivateUsage]
        assert [(error.code, error.notes) for error in errors] == [
            (ErrorCode.CHECK_WORKER_CONNECTIONS, ['OSError: reset']),
        ]

    def test_no_profiles_reads_nothing(self) -> None:
        reader = mock.AsyncMock()
        with mock.patch('horsies.core.app.read_server_connection_limit', reader):
            assert _app({})._check_worker_connections() == []  # pyright: ignore[reportPrivateUsage]
        reader.assert_not_awaited()

    def test_a_transaction_pooler_reads_nothing(self) -> None:
        app = _app(
            {'small': WorkerProfile(isolated=IsolatedChildren(count=1))},
            pgbouncer_transaction_mode=True,
            session_database_url=SecretStr(
                'postgresql+psycopg://user:pass@localhost:5433/db',
            ),
        )
        reader = mock.AsyncMock()
        with mock.patch('horsies.core.app.read_server_connection_limit', reader):
            assert app._check_worker_connections() == []  # pyright: ignore[reportPrivateUsage]
        reader.assert_not_awaited()

    def test_check_live_runs_the_phase_after_connectivity(self) -> None:
        app = _app({'big': WorkerProfile(isolated=IsolatedChildren(count=20))})
        reader = mock.AsyncMock(
            return_value=ServerConnectionLimit(max_connections=30, reserved=3),
        )
        with (
            mock.patch.object(app, '_check_broker_connectivity', return_value=[]),
            mock.patch('horsies.core.app.read_server_connection_limit', reader),
        ):
            errors = app.check(live=True)
        assert [error.code for error in errors] == [ErrorCode.CHECK_WORKER_CONNECTIONS]

    def test_check_live_skips_the_phase_when_connectivity_fails(self) -> None:
        app = _app({'big': WorkerProfile(isolated=IsolatedChildren(count=20))})
        reader = mock.AsyncMock()
        connectivity_error = mock.MagicMock(code=ErrorCode.BROKER_INVALID_URL)
        with (
            mock.patch.object(
                app,
                '_check_broker_connectivity',
                return_value=[connectivity_error],
            ),
            mock.patch('horsies.core.app.read_server_connection_limit', reader),
        ):
            errors = app.check(live=True)
        assert errors == [connectivity_error]
        reader.assert_not_awaited()

    def test_check_without_live_reads_nothing(self) -> None:
        app = _app({'big': WorkerProfile(isolated=IsolatedChildren(count=20))})
        reader = mock.AsyncMock()
        with mock.patch('horsies.core.app.read_server_connection_limit', reader):
            assert app.check(live=False) == []
        reader.assert_not_awaited()


def test_no_isolated_children_has_no_memory_sum_to_exceed() -> None:
    """A profile with no children sums to 0; the compare does not need a limit."""
    outcome = compare_memory_limits(
        WorkerProfile(isolated=NoIsolatedChildren()),
        _host_ram(1),
    )
    assert outcome == MemoryLimitsFit(total_mb=0, machine_mb=1)
