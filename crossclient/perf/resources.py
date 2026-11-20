"""The CPU time of the processes under measurement.

- A worker: the CPU time of the worker parent and of each process below it
  that runs, read with ``psutil`` at the end of a drain. A process below it
  that ended before the read is not counted; the drain replaces no child.
  Linux counts the CPU time of a process in whole clock ticks (10 ms at a
  ``USER_HZ`` of 100), the user and the system time apart: a process that
  ran under one tick in each reads 0 seconds, and the read counts it.
- A producer: the CPU time of the harness process, which makes the calls of
  a public call scenario (``time.process_time``), across one block.
- PostgreSQL: the ``usage_usec`` of the ``cpu.stat`` file of the cgroup v2
  group of the server (``--postgres-cpu-stat``), read before and after each
  drain or block. The kernel counts every process of the group, also a
  backend that ended. Linux only; without the option the column is not
  measured.

The readers do I/O and return their failure as a value. The runner stops the
run on a failure: a cell without its CPU column is not a measurement.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final, assert_never

import psutil

MAX_WORKER_PROCESSES: Final = 256
"""The most processes below one worker that a read counts. A drain worker has
its task children, its preload process and its handler child."""

MAX_CPU_STAT_BYTES: Final = 64 * 1024
"""The most bytes of a ``cpu.stat`` file that a read takes. The file of
cgroup v2 has under 20 lines."""

CPU_STAT_USAGE_KEY: Final = 'usage_usec'
"""The line of ``cpu.stat`` with the CPU time of the group in microseconds."""

MICROSECONDS_PER_SECOND: Final = 1_000_000


@dataclass(frozen=True, slots=True)
class WorkerCpu:
    """The CPU seconds (user and system) of a worker at one read: its parent
    process, and the sum of the processes below it. ``children_pids`` holds
    the pid of each process below it that the read counted, in ascending
    order; a tuple, so that the raw JSON of a run can hold it."""

    parent_s: float
    children_s: float
    children_pids: tuple[int, ...]

    @property
    def total_s(self) -> float:
        return self.parent_s + self.children_s


@dataclass(frozen=True, slots=True)
class WorkerCpuUnreadable:
    """The CPU time of worker ``pid`` could not be read: the process ended,
    the OS refused the read, or more than ``MAX_WORKER_PROCESSES`` processes
    are below it."""

    pid: int
    cause: str


def read_worker_cpu(pid: int) -> WorkerCpu | WorkerCpuUnreadable:
    """The CPU seconds of process ``pid`` and of every process below it that
    runs now. A process below it that ends during the read is left out."""
    try:
        parent = psutil.Process(pid)
        below = parent.children(recursive=True)
        if len(below) > MAX_WORKER_PROCESSES:
            return WorkerCpuUnreadable(
                pid=pid,
                cause=f'{len(below)} processes below it, over {MAX_WORKER_PROCESSES}',
            )
        parent_times = parent.cpu_times()
    except psutil.Error as error:
        return WorkerCpuUnreadable(pid=pid, cause=f'{type(error).__name__}: {error}')
    children_s = 0.0
    counted: list[int] = []
    for process in below:
        try:
            times = process.cpu_times()
        except psutil.NoSuchProcess:
            continue
        except psutil.Error as error:
            return WorkerCpuUnreadable(
                pid=pid,
                cause=f'process {process.pid}: {type(error).__name__}: {error}',
            )
        children_s += times.user + times.system
        counted.append(process.pid)
    return WorkerCpu(
        parent_s=parent_times.user + parent_times.system,
        children_s=children_s,
        children_pids=tuple(sorted(counted)),
    )


def producer_cpu_now() -> float:
    """The CPU seconds of the harness process, every thread, now."""
    return time.process_time()


@dataclass(frozen=True, slots=True)
class CgroupCpuStat:
    """The ``cpu.stat`` file of the cgroup v2 group of the PostgreSQL server."""

    path: Path


@dataclass(frozen=True, slots=True)
class NoPostgresCpuSource:
    """No ``--postgres-cpu-stat``: PostgreSQL CPU is not measured."""


type PostgresCpuSource = CgroupCpuStat | NoPostgresCpuSource


@dataclass(frozen=True, slots=True)
class CpuUsage:
    """The CPU time of a cgroup since it was made, in microseconds."""

    microseconds: int


@dataclass(frozen=True, slots=True)
class CpuStatUnreadable:
    """``path`` could not be read, or holds no ``usage_usec`` line with a
    whole number."""

    path: Path
    cause: str


@dataclass(frozen=True, slots=True)
class CpuStatMalformed:
    """A ``cpu.stat`` text with no ``usage_usec`` line of a whole number."""

    reason: str


def parse_cpu_stat(text: str) -> CpuUsage | CpuStatMalformed:
    """The ``usage_usec`` of a ``cpu.stat`` text. Pure."""
    for line in text.splitlines():
        key, _, value = line.partition(' ')
        if key != CPU_STAT_USAGE_KEY:
            continue
        match value.strip().isdigit():
            case True:
                return CpuUsage(microseconds=int(value.strip()))
            case False:
                return CpuStatMalformed(
                    reason=f'{CPU_STAT_USAGE_KEY} is not a whole number: {value!r}'
                )
    return CpuStatMalformed(reason=f'no {CPU_STAT_USAGE_KEY} line')


def read_cgroup_cpu(path: Path) -> CpuUsage | CpuStatUnreadable:
    """The ``usage_usec`` of the ``cpu.stat`` file at ``path``."""
    try:
        with path.open('rb') as stat_file:
            data = stat_file.read(MAX_CPU_STAT_BYTES + 1)
    except OSError as error:
        return CpuStatUnreadable(path=path, cause=f'{type(error).__name__}: {error}')
    if len(data) > MAX_CPU_STAT_BYTES:
        return CpuStatUnreadable(path=path, cause=f'over {MAX_CPU_STAT_BYTES} bytes')
    match parse_cpu_stat(data.decode('ascii', 'replace')):
        case CpuUsage() as usage:
            return usage
        case CpuStatMalformed(reason=reason):
            return CpuStatUnreadable(path=path, cause=reason)
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class CpuSeconds:
    """CPU seconds that a reading measured."""

    seconds: float


@dataclass(frozen=True, slots=True)
class NotMeasured:
    """The run does not measure this CPU time: ``reason`` says why."""

    reason: str


type CpuReading = CpuSeconds | NotMeasured


class PostgresCpuMeter:
    """Reads the PostgreSQL CPU time across one drain or one block."""

    def __init__(self, source: PostgresCpuSource) -> None:
        self._source = source
        self._started: CpuUsage | None = None

    def begin(self) -> None:
        """Read the CPU time at the start of a measured unit.

        Raises:
            RuntimeError: the ``cpu.stat`` file could not be read; it names
                the path and the cause. The run stops.
        """
        match self._source:
            case NoPostgresCpuSource():
                return
            case CgroupCpuStat(path=path):
                self._started = _required_usage(path)
            case _ as unreachable:
                assert_never(unreachable)

    def finish(self) -> CpuReading:
        """The CPU seconds since ``begin``.

        Raises:
            RuntimeError: the ``cpu.stat`` file could not be read. The run
                stops.
            AssertionError: ``finish`` before ``begin``: a defect of the
                runner.
        """
        match self._source:
            case NoPostgresCpuSource():
                return NotMeasured(reason='no --postgres-cpu-stat')
            case CgroupCpuStat(path=path):
                started = self._started
                assert started is not None, 'PostgresCpuMeter.finish before begin'
                ended = _required_usage(path)
                self._started = None
                return CpuSeconds(
                    seconds=(ended.microseconds - started.microseconds)
                    / MICROSECONDS_PER_SECOND
                )
            case _ as unreachable:
                assert_never(unreachable)


def _required_usage(path: Path) -> CpuUsage:
    match read_cgroup_cpu(path):
        case CpuUsage() as usage:
            return usage
        case CpuStatUnreadable(cause=cause):
            raise RuntimeError(f'PostgreSQL CPU: {path} is not readable: {cause}')
        case _ as unreachable:
            assert_never(unreachable)
