"""Where the worker process and its children run: ``ParentCpu``.

With ``ParentCpu.OWN_CORE`` (Linux), the worker process runs on one physical
core of the CPUs it may use, both SMT threads of it, and every child on the
other CPUs. The worker process dispatches every task and writes every
outcome; a child on the other thread of its core slows it.

- ``plan_cpu_placement`` reads the CPUs that the worker may use and their
  SMT siblings, and splits them (``split_own_core``).
- ``place_process`` sets the CPUs of a child by its pid. The worker calls it
  after it starts a child and before it sends the init frame: the child has
  started no thread of its own, so every later thread of the child gets the
  same CPUs.
- ``place_worker_threads`` sets the CPUs of every thread of the worker
  process. A thread that a placed thread starts later gets the same CPUs.
"""

from __future__ import annotations

import enum
import errno
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final, assert_never

from horsies.core.types.result import Err, Ok, Result

CPU_TOPOLOGY_DIR: Final = Path('/sys/devices/system/cpu')
"""Where Linux lists the SMT siblings of each CPU
(``cpu<N>/topology/thread_siblings_list``)."""

TASK_DIR: Final = Path('/proc/self/task')
"""One entry per thread of this process, named by its thread id."""

MAX_CPU_ID: Final = 65_535
"""The highest CPU id that a sibling list may name. A larger one is a list
that this module cannot read."""


def _allowed_cpus() -> frozenset[int]:
    """The CPUs that this process may use. ``OSError`` off Linux."""
    if sys.platform != 'linux':
        raise OSError(errno.ENOSYS, 'sched_getaffinity needs Linux')
    return frozenset(os.sched_getaffinity(0))


def _set_cpus(target: int, cpus: frozenset[int]) -> None:
    """Set the CPUs of thread or process ``target``. ``OSError`` off Linux
    and when the OS refuses."""
    if sys.platform != 'linux':
        raise OSError(errno.ENOSYS, 'sched_setaffinity needs Linux')
    os.sched_setaffinity(target, cpus)


class ParentCpu(enum.Enum):
    """Where the worker process runs, a setting of the worker profile.

    SHARED: the worker process and its children run on the CPUs they inherit.
    OWN_CORE: the worker process runs on one physical core, both SMT threads;
    the children run on the other CPUs that the worker may use. Linux only.
    """

    SHARED = 'SHARED'
    OWN_CORE = 'OWN_CORE'


@dataclass(frozen=True, slots=True)
class SharedCpus:
    """No placement: every process keeps the CPUs it inherits."""


@dataclass(frozen=True, slots=True)
class OwnCore:
    """The CPUs of the worker process and of its children. Disjoint, and
    each one holds at least one CPU."""

    parent: frozenset[int]
    children: frozenset[int]

    def __post_init__(self) -> None:
        assert self.parent and self.children, self
        assert not self.parent & self.children, self


type CpuPlacement = SharedCpus | OwnCore


@dataclass(frozen=True, slots=True)
class NotLinux:
    """``OWN_CORE`` runs on Linux only."""

    platform: str


@dataclass(frozen=True, slots=True)
class TooFewCores:
    """The CPUs that the worker may use hold fewer than 2 physical cores."""

    allowed: frozenset[int]
    cores: int


@dataclass(frozen=True, slots=True)
class TopologyUnreadable:
    """The sibling list of a CPU could not be read or parsed."""

    cpu: int
    cause: str


type PlacementRefused = NotLinux | TooFewCores | TopologyUnreadable


@dataclass(frozen=True, slots=True)
class PlaceRefused:
    """The OS refused to set the CPUs of a thread or a process."""

    target: int
    cause: str


def describe_refusal(refused: PlacementRefused) -> str:
    """The refusal as one sentence, for the start error. Pure."""
    match refused:
        case NotLinux(platform=platform):
            return f'parent_cpu=OWN_CORE runs on Linux only; this platform is {platform}'
        case TooFewCores(allowed=allowed, cores=cores):
            return (
                'parent_cpu=OWN_CORE needs at least 2 physical cores in the CPUs '
                f'that the worker may use; CPUs {sorted(allowed)} hold {cores}'
            )
        case TopologyUnreadable(cpu=cpu, cause=cause):
            return f'the SMT siblings of CPU {cpu} could not be read: {cause}'
        case _ as unreachable:
            assert_never(unreachable)


def parse_cpu_list(text: str) -> Result[frozenset[int], str]:
    """The CPUs of a Linux CPU list such as ``0-3,8-11`` or ``6,7``. Pure.

    ``Err``: the text is not a CPU list, or names a CPU above ``MAX_CPU_ID``.
    """
    cpus: set[int] = set()
    for part in text.strip().split(','):
        first, dash, last = part.partition('-')
        if not first.isdecimal() or (dash and not last.isdecimal()):
            return Err(f'not a CPU list: {text!r}')
        low = int(first)
        high = int(last) if dash else low
        if high < low or high > MAX_CPU_ID:
            return Err(f'not a CPU list: {text!r}')
        cpus.update(range(low, high + 1))
    return Ok(frozenset(cpus))


def split_own_core(
    allowed: frozenset[int],
    siblings: Mapping[int, frozenset[int]],
) -> Result[OwnCore, TooFewCores]:
    """Split ``allowed`` into the worker's core and the children's CPUs. Pure.

    A physical core is a sibling set, cut to ``allowed``. The worker process
    takes the core of the lowest allowed CPU; the children take every other
    allowed CPU. ``siblings`` holds every CPU of ``allowed``; a CPU missing
    from it is a defect (``AssertionError``).
    """
    cores = {siblings[cpu] & allowed for cpu in allowed}
    if len(cores) < 2:
        return Err(TooFewCores(allowed=allowed, cores=len(cores)))
    parent = siblings[min(allowed)] & allowed
    return Ok(OwnCore(parent=parent, children=allowed - parent))


def read_siblings(
    allowed: frozenset[int],
    topology_dir: Path = CPU_TOPOLOGY_DIR,
) -> Result[dict[int, frozenset[int]], TopologyUnreadable]:
    """The SMT siblings of each CPU of ``allowed``, from ``topology_dir``."""
    siblings: dict[int, frozenset[int]] = {}
    for cpu in sorted(allowed):
        path = topology_dir / f'cpu{cpu}' / 'topology' / 'thread_siblings_list'
        try:
            text = path.read_text(encoding='ascii')
        except (OSError, UnicodeDecodeError) as exc:
            return Err(TopologyUnreadable(cpu=cpu, cause=f'{type(exc).__name__}: {exc}'))
        match parse_cpu_list(text):
            case Ok(listed) if cpu in listed:
                siblings[cpu] = listed
            case Ok(listed):
                return Err(
                    TopologyUnreadable(cpu=cpu, cause=f'its list {sorted(listed)} omits it'),
                )
            case Err(reason):
                return Err(TopologyUnreadable(cpu=cpu, cause=reason))
            case _ as unreachable:
                assert_never(unreachable)
    return Ok(siblings)


def plan_cpu_placement(setting: ParentCpu) -> Result[CpuPlacement, PlacementRefused]:
    """The placement of ``setting`` on this host: the CPUs that this process
    may use (``os.sched_getaffinity``), split by ``split_own_core``. Reads
    the host; changes nothing."""
    match setting:
        case ParentCpu.SHARED:
            return Ok(SharedCpus())
        case ParentCpu.OWN_CORE:
            pass
        case _ as unreachable:
            assert_never(unreachable)
    if sys.platform != 'linux':
        return Err(NotLinux(platform=sys.platform))
    allowed = _allowed_cpus()
    match read_siblings(allowed, CPU_TOPOLOGY_DIR):
        case Ok(siblings):
            pass
        case Err(unreadable):
            return Err(unreadable)
    match split_own_core(allowed, siblings):
        case Ok(own_core):
            return Ok(own_core)
        case Err(too_few):
            return Err(too_few)


def place_process(pid: int, cpus: frozenset[int]) -> Result[None, PlaceRefused]:
    """Set the CPUs of the main thread of process ``pid``. Threads that it
    starts later get the same CPUs."""
    try:
        _set_cpus(pid, cpus)
    except OSError as exc:
        return Err(PlaceRefused(target=pid, cause=f'{type(exc).__name__}: {exc}'))
    return Ok(None)


def place_worker_threads(
    cpus: frozenset[int],
    task_dir: Path = TASK_DIR,
) -> Result[int, PlaceRefused]:
    """Set the CPUs of every thread of this process; return how many. A
    thread that ended between the listing and the call is skipped."""
    placed = 0
    for entry in sorted(task_dir.iterdir()):
        thread_id = int(entry.name)
        try:
            _set_cpus(thread_id, cpus)
        except ProcessLookupError:
            continue
        except OSError as exc:
            return Err(PlaceRefused(target=thread_id, cause=f'{type(exc).__name__}: {exc}'))
        placed += 1
    return Ok(placed)
