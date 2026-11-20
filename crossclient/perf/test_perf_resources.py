"""The CPU readers of the overhead measurement: the worker tree, the
``cpu.stat`` file of the PostgreSQL cgroup, and the meter across a unit."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Final

import pytest

from crossclient.perf.resources import (
    MAX_CPU_STAT_BYTES,
    CgroupCpuStat,
    CpuSeconds,
    CpuStatMalformed,
    CpuStatUnreadable,
    CpuUsage,
    NoPostgresCpuSource,
    NotMeasured,
    PostgresCpuMeter,
    WorkerCpu,
    WorkerCpuUnreadable,
    parse_cpu_stat,
    read_cgroup_cpu,
    read_worker_cpu,
)

pytestmark = [pytest.mark.unit]

PROCESS_TIMEOUT_S: Final = 30

CPU_STAT: Final = (
    'usage_usec 1250000\n' 'user_usec 1000000\n' 'system_usec 250000\n' 'nr_periods 0\n'
)


class TestCpuStat:
    def test_the_usage_line_gives_the_microseconds(self) -> None:
        assert parse_cpu_stat(CPU_STAT) == CpuUsage(microseconds=1_250_000)

    def test_a_text_without_the_usage_line_is_malformed(self) -> None:
        assert parse_cpu_stat('user_usec 1\n') == CpuStatMalformed(
            reason='no usage_usec line'
        )

    def test_a_usage_that_is_not_a_whole_number_is_malformed(self) -> None:
        match parse_cpu_stat('usage_usec 12.5\n'):
            case CpuStatMalformed(reason=reason):
                assert 'not a whole number' in reason
            case other:
                pytest.fail(f'expected CpuStatMalformed, got {other!r}')

    def test_a_file_is_read(self, tmp_path: Path) -> None:
        path = tmp_path / 'cpu.stat'
        path.write_text(CPU_STAT, encoding='ascii')
        assert read_cgroup_cpu(path) == CpuUsage(microseconds=1_250_000)

    def test_a_missing_file_is_unreadable(self, tmp_path: Path) -> None:
        match read_cgroup_cpu(tmp_path / 'cpu.stat'):
            case CpuStatUnreadable(cause=cause):
                assert 'FileNotFoundError' in cause
            case other:
                pytest.fail(f'expected CpuStatUnreadable, got {other!r}')

    def test_a_file_over_the_bound_is_unreadable(self, tmp_path: Path) -> None:
        path = tmp_path / 'cpu.stat'
        path.write_text('x' * (MAX_CPU_STAT_BYTES + 1), encoding='ascii')
        match read_cgroup_cpu(path):
            case CpuStatUnreadable(cause=cause):
                assert str(MAX_CPU_STAT_BYTES) in cause
            case other:
                pytest.fail(f'expected CpuStatUnreadable, got {other!r}')


class TestPostgresCpuMeter:
    def test_no_source_is_not_measured(self) -> None:
        meter = PostgresCpuMeter(NoPostgresCpuSource())
        meter.begin()
        assert meter.finish() == NotMeasured(reason='no --postgres-cpu-stat')

    def test_the_reading_is_the_usage_between_begin_and_finish(
        self,
        tmp_path: Path,
    ) -> None:
        path = tmp_path / 'cpu.stat'
        path.write_text('usage_usec 1000000\n', encoding='ascii')
        meter = PostgresCpuMeter(CgroupCpuStat(path=path))
        meter.begin()
        path.write_text('usage_usec 3500000\n', encoding='ascii')
        assert meter.finish() == CpuSeconds(seconds=2.5)

    def test_an_unreadable_file_stops_the_run(self, tmp_path: Path) -> None:
        meter = PostgresCpuMeter(CgroupCpuStat(path=tmp_path / 'cpu.stat'))
        with pytest.raises(RuntimeError, match='PostgreSQL CPU'):
            meter.begin()


@pytest.fixture
def parent_with_one_child() -> Iterator[subprocess.Popen[str]]:
    """A Python process that starts one child, prints a line once the child
    runs, and both wait for stdin to close."""
    process = subprocess.Popen(
        [
            sys.executable,
            '-c',
            'import subprocess, sys\n'
            'child = subprocess.Popen([sys.executable, "-c", '
            '"import sys; sys.stdin.read()"], stdin=subprocess.PIPE)\n'
            'print("ready", flush=True)\n'
            'sys.stdin.read()\n'
            'child.stdin.close()\n'
            'child.wait()\n',
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    assert process.stdout.readline().strip() == 'ready'
    try:
        yield process
    finally:
        assert process.stdin is not None
        process.stdin.close()
        _ = process.wait(PROCESS_TIMEOUT_S)


class TestWorkerCpu:
    def test_the_parent_and_each_process_below_it_are_read(
        self,
        parent_with_one_child: subprocess.Popen[str],
    ) -> None:
        match read_worker_cpu(parent_with_one_child.pid):
            case WorkerCpu(parent_s=parent_s, children_s=children_s, children=children):
                assert children == 1
                assert parent_s > 0
                assert children_s > 0
            case other:
                pytest.fail(f'expected WorkerCpu, got {other!r}')

    def test_a_process_that_ended_is_unreadable(self) -> None:
        ended = subprocess.Popen([sys.executable, '-c', 'pass'])
        _ = ended.wait(PROCESS_TIMEOUT_S)
        match read_worker_cpu(ended.pid):
            case WorkerCpuUnreadable(pid=pid):
                assert pid == ended.pid
            case other:
                pytest.fail(f'expected WorkerCpuUnreadable, got {other!r}')
