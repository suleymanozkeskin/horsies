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
from horsies.core.models.worker_profile import (
    IsolatedChildren,
    IsolatedRecycle,
    MemoryLimit,
    NoIsolatedChildren,
    WorkerProfile,
)
from horsies.core.worker import profile_fit
from horsies.core.worker.profile_fit import (
    CGROUP_FILE_MAX_BYTES,
    LISTENER_CONNECTIONS_PER_WORKER,
    RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER,
    CgroupLimit,
    CgroupUnreadable,
    ConnectionsExceedServer,
    ConnectionsFit,
    MachineMemory,
    MachineMemoryUnreadable,
    MemoryLimitsExceedMachine,
    MemoryLimitsFit,
    MemoryLimitsNotSet,
    MemorySource,
    NoCgroupLimit,
    ServerConnectionLimit,
    ServerConnectionLimitUnreadable,
    UsableMemory,
    UsableMemoryUnreadable,
    WorkerConnectionNeed,
    compare_connections,
    compare_memory_limits,
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


_MIB = 1024 * 1024
# The cgroup v1 value for "no limit" on 64-bit Linux with 4 KiB pages.
_V1_NO_LIMIT = 9_223_372_036_854_771_712


class _CgroupFiles:
    """Fake cgroup v2 and v1 limit files under ``tmp_path``."""

    def __init__(self, root: Path) -> None:
        self.v2 = root / 'v2' / 'memory.max'
        self.v1 = root / 'v1' / 'memory.limit_in_bytes'

    def write(self, path: Path, content: str | bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        match content:
            case str():
                path.write_text(content)
            case bytes():
                path.write_bytes(content)

    def read(self) -> CgroupLimit | NoCgroupLimit | CgroupUnreadable:
        return read_cgroup_memory_limit(v2_path=self.v2, v1_path=self.v1)


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
            _broker(),
        )
        assert need == WorkerConnectionNeed(
            parent_pool=3 + 2,
            listener=LISTENER_CONNECTIONS_PER_WORKER,
            runner_heartbeat=RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER,
            children=8,
            per_child=2,
        )
        assert LISTENER_CONNECTIONS_PER_WORKER == 2
        assert RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER == 1
        assert need.total() == 24

    def test_parent_pool_inherits_pool_size_when_worker_pool_is_none(self) -> None:
        need = worker_connection_need(
            WorkerProfile(isolated=IsolatedChildren(count=1)),
            _broker(worker_pool_size=None, worker_max_overflow=None),
        )
        assert need.parent_pool == 5 + 10

    def test_usable_slots_never_go_below_zero(self) -> None:
        assert ServerConnectionLimit(max_connections=3, reserved=5).usable() == 0

    def test_need_above_usable_exceeds(self) -> None:
        profile = WorkerProfile(isolated=IsolatedChildren(count=8))
        limit = ServerConnectionLimit(max_connections=26, reserved=3)
        outcome = compare_connections(profile, _broker(), limit)
        assert isinstance(outcome, ConnectionsExceedServer)
        assert outcome.describe() == (
            'one worker can open 24 Postgres connections; the server has 23 '
            'usable slots (max_connections 26 - reserved 3)'
        )

    def test_need_equal_to_usable_fits(self) -> None:
        profile = WorkerProfile(isolated=IsolatedChildren(count=8))
        limit = ServerConnectionLimit(max_connections=27, reserved=3)
        assert isinstance(compare_connections(profile, _broker(), limit), ConnectionsFit)


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
            "worker_profiles['big']: one worker can open 48 Postgres "
            'connections; the server has 27 usable slots (max_connections 30 '
            '- reserved 3)'
        )
        assert errors[0].notes == [
            'parent pool 5 + listener 2 + runner heartbeat 1 + 20 children x '
            'worker_child_pool_max_size 2 = 48',
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
