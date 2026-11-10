"""Named worker profiles: the settings of one worker, kept in the app config.

``horsies worker <app> --profile <name>`` starts a worker with the profile of
that name from ``AppConfig.worker_profiles``. A profile states:

- how many children the worker runs for each execution mode,
- when a child is replaced (recycled),
- how many tasks the worker claims,
- how long a stopping worker waits for running tasks,
- what a Linux worker imports before it forks children.

This release has one execution mode, isolated: an isolated child runs one
task at a time in its own process. The number of children of a worker is the
sum of the counts of its modes; no separate total exists.

Each value is bounded where it is built. The checks that need more than one
value (at least one child, claim limits against capacity) run in
``AppConfig`` validation, which knows the profile name and reports the field
path, for example ``worker_profiles['batch'].claim.claimed_per_worker``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import timedelta
from typing import Annotated, Final, Literal, Union, assert_never

from pydantic import BaseModel, ConfigDict, Field

from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.worker.config import ChildPreload

MAX_ISOLATED_CHILDREN: Final = 1024
"""The most isolated children that one profile starts."""

MAX_TASK_LIMIT: Final = 2_147_483_647
"""The largest task count of a limit: the largest Postgres ``integer``.

The claim limits go to the claim function as that type."""

MAX_MEMORY_LIMIT_MB: Final = 16 * 1024 * 1024
"""The largest memory limit of one child: 16 TiB, in MB."""

DEFAULT_RECYCLE_AFTER_TASKS: Final = 100
"""Tasks after which a child is replaced when the profile does not say."""

DEFAULT_DRAIN_TIMEOUT: Final = timedelta(seconds=300)
"""Time that a stopping worker waits for running tasks by default."""

MAX_DRAIN_TIMEOUT: Final = timedelta(hours=24)
"""The longest time that a stopping worker waits for running tasks."""

PROFILE_NAME_MAX_LENGTH: Final = 64
"""The most characters in a profile name."""

_PROFILE_NAME: Final = re.compile(r'[A-Za-z0-9_][A-Za-z0-9_.-]*')
"""Letters, digits, ``_``, ``.`` and ``-``; the first is not ``.`` or ``-``,
so the name cannot read as a command-line option."""


class TaskLimit(BaseModel):
    """A limit of ``tasks`` tasks, from 1 to ``MAX_TASK_LIMIT``."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['task_limit'] = 'task_limit'
    tasks: int = Field(ge=1, le=MAX_TASK_LIMIT, description='Number of tasks')


class NoTaskLimit(BaseModel):
    """No limit on the number of tasks."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['no_task_limit'] = 'no_task_limit'


class MemoryLimit(BaseModel):
    """A limit of ``mb`` MB of resident memory (RSS), from 1 to
    ``MAX_MEMORY_LIMIT_MB``.

    No fixed floor applies here: a constant cannot know the memory of a
    booted child of an app. At startup the worker refuses a limit at or
    below the measured child baseline.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['memory_limit'] = 'memory_limit'
    mb: int = Field(ge=1, le=MAX_MEMORY_LIMIT_MB, description='Megabytes of RSS')


class NoMemoryLimit(BaseModel):
    """No limit on the resident memory of a child."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['no_memory_limit'] = 'no_memory_limit'


TaskLimitSetting = Annotated[
    Union[TaskLimit, NoTaskLimit],
    Field(discriminator='kind'),
]
MemoryLimitSetting = Annotated[
    Union[MemoryLimit, NoMemoryLimit],
    Field(discriminator='kind'),
]


def _default_recycle_after() -> TaskLimit:
    return TaskLimit(tasks=DEFAULT_RECYCLE_AFTER_TASKS)


class IsolatedRecycle(BaseModel):
    """When the worker replaces an isolated child with a new one.

    Each child has its own counters, so children are replaced at different
    times, not all at once.

    - ``after``: replace a child after it completes this many tasks.
    - ``above``: replace a child when its own RSS reaches this limit. The
      child reports its RSS with each result. A task can go above the limit
      while it runs: this bounds memory that stays after a task, not the
      peak of a task.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    after: TaskLimitSetting = Field(default_factory=_default_recycle_after)
    above: MemoryLimitSetting = Field(default_factory=NoMemoryLimit)


class IsolatedChildren(BaseModel):
    """``count`` isolated children: each runs one task at a time in its own
    process. ``count`` is from 1 to ``MAX_ISOLATED_CHILDREN``."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['isolated_children'] = 'isolated_children'
    count: int = Field(
        ge=1,
        le=MAX_ISOLATED_CHILDREN,
        description='Number of isolated children',
    )
    recycle: IsolatedRecycle = Field(default_factory=IsolatedRecycle)


class NoIsolatedChildren(BaseModel):
    """The profile starts no isolated children."""

    model_config = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['no_isolated_children'] = 'no_isolated_children'


IsolatedSetting = Annotated[
    Union[IsolatedChildren, NoIsolatedChildren],
    Field(discriminator='kind'),
]


class ClaimLimits(BaseModel):
    """Limits on the tasks that the worker claims.

    The capacity of a worker is its number of children plus
    ``AppConfig.prefetch_buffer``: it never holds more tasks than that. A
    limit above the capacity has no effect, and validation refuses it.

    - ``per_queue_per_pass``: the most tasks that one claim pass takes from
      one queue. With ``NoTaskLimit()``, one pass fills the free capacity.
    - ``claimed_per_worker``: the most CLAIMED tasks (claimed, not yet
      running) that the worker holds. With ``NoTaskLimit()``, the capacity
      is the limit.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    per_queue_per_pass: TaskLimitSetting = Field(default_factory=NoTaskLimit)
    claimed_per_worker: TaskLimitSetting = Field(default_factory=NoTaskLimit)


def _default_drain_timeout() -> timedelta:
    return DEFAULT_DRAIN_TIMEOUT


class WorkerProfile(BaseModel):
    """The settings of one worker.

    - ``isolated``: the isolated children. At least one child is required.
    - ``claim``: the claim limits.
    - ``drain_timeout``: how long a stopping worker waits for running tasks,
      above 0 and at most ``MAX_DRAIN_TIMEOUT``. At this deadline it kills
      the children that still run a task, and each of those tasks takes the
      crash path. Set it below the stop window of the supervisor.
    - ``preload``: what a Linux worker imports once before it forks
      children. Other platforms start every child with ``exec``.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    isolated: IsolatedSetting = Field(default_factory=NoIsolatedChildren)
    claim: ClaimLimits = Field(default_factory=ClaimLimits)
    drain_timeout: timedelta = Field(
        default_factory=_default_drain_timeout,
        gt=timedelta(0),
        le=MAX_DRAIN_TIMEOUT,
    )
    preload: ChildPreload = ChildPreload.LIBRARY

    def child_count(self) -> int:
        """The number of children of the worker: the sum over the modes."""
        match self.isolated:
            case IsolatedChildren(count=count):
                return count
            case NoIsolatedChildren():
                return 0
            case _ as unreachable:
                assert_never(unreachable)


def _describe_task_limit(limit: TaskLimit | NoTaskLimit) -> str:
    match limit:
        case TaskLimit(tasks=tasks):
            return f'{tasks} tasks'
        case NoTaskLimit():
            return 'no limit'
        case _ as unreachable:
            assert_never(unreachable)


def _describe_memory_limit(limit: MemoryLimit | NoMemoryLimit) -> str:
    match limit:
        case MemoryLimit(mb=mb):
            return f'{mb} MB'
        case NoMemoryLimit():
            return 'no limit'
        case _ as unreachable:
            assert_never(unreachable)


def _describe_isolated(isolated: IsolatedChildren | NoIsolatedChildren) -> str:
    match isolated:
        case IsolatedChildren(count=count, recycle=recycle):
            return (
                f'isolated={count} (recycle after '
                f'{_describe_task_limit(recycle.after)}, above '
                f'{_describe_memory_limit(recycle.above)})'
            )
        case NoIsolatedChildren():
            return 'isolated=none'
        case _ as unreachable:
            assert_never(unreachable)


def describe_worker_profile(profile: WorkerProfile) -> str:
    """One line that states every setting of a profile, for logs."""
    return (
        f'{_describe_isolated(profile.isolated)}, '
        f'claim per queue per pass: '
        f'{_describe_task_limit(profile.claim.per_queue_per_pass)}, '
        f'claimed per worker: '
        f'{_describe_task_limit(profile.claim.claimed_per_worker)}, '
        f'drain_timeout={profile.drain_timeout.total_seconds():g}s, '
        f'preload={profile.preload.value}'
    )


def profile_path(name: str) -> str:
    """The config path of a profile, as a user writes it in Python."""
    return f'worker_profiles[{name!r}]'


def _profile_error(
    message: str, notes: list[str], help_text: str
) -> ConfigurationError:
    return ConfigurationError(
        message=message,
        code=ErrorCode.CONFIG_INVALID_WORKER_PROFILE,
        notes=notes,
        help_text=help_text,
    )


def profile_name_errors(name: str) -> list[ConfigurationError]:
    """Refuse a name that a ``--profile`` argument cannot carry as it is."""
    if len(name) <= PROFILE_NAME_MAX_LENGTH and _PROFILE_NAME.fullmatch(name):
        return []
    return [
        _profile_error(
            f'{profile_path(name)}: the profile name is not valid',
            [
                f'a name has 1 to {PROFILE_NAME_MAX_LENGTH} characters: '
                "letters, digits, '_', '.' and '-'",
                "the first character is not '.' or '-'",
            ],
            'rename the profile, for example to "default" or "batch-high-memory"',
        ),
    ]


def profile_children_errors(
    name: str, profile: WorkerProfile
) -> list[ConfigurationError]:
    """Refuse a profile that starts no children."""
    if profile.child_count() >= 1:
        return []
    return [
        _profile_error(
            f'{profile_path(name)}: the profile starts no children',
            ['a worker needs at least one child to run a task'],
            f'set {profile_path(name)}.isolated, for example '
            'isolated=IsolatedChildren(count=4)',
        ),
    ]


def _claim_limit_error(
    path: str,
    tasks: int,
    children: int,
    prefetch_buffer: int,
) -> ConfigurationError:
    capacity = children + prefetch_buffer
    return _profile_error(
        f'{path}: {tasks} tasks is above the capacity of the worker ({capacity})',
        [
            f'capacity = {children} children + prefetch_buffer {prefetch_buffer}',
            'the worker never holds more tasks than its capacity, so this '
            'limit has no effect',
        ],
        f'set the limit to {capacity} or less, or use NoTaskLimit()',
    )


def profile_claim_errors(
    name: str,
    profile: WorkerProfile,
    prefetch_buffer: int,
) -> list[ConfigurationError]:
    """Refuse a claim limit above the capacity of the worker.

    ``prefetch_buffer`` is the validated ``AppConfig.prefetch_buffer``
    (0 or more). The capacity is the number of children plus it.
    """
    children = profile.child_count()
    claim_path = f'{profile_path(name)}.claim'
    limits = {
        f'{claim_path}.per_queue_per_pass': profile.claim.per_queue_per_pass,
        f'{claim_path}.claimed_per_worker': profile.claim.claimed_per_worker,
    }
    errors: list[ConfigurationError] = []
    for path, limit in limits.items():
        match limit:
            case TaskLimit(tasks=tasks) if tasks > children + prefetch_buffer:
                errors.append(
                    _claim_limit_error(path, tasks, children, prefetch_buffer),
                )
            case TaskLimit() | NoTaskLimit():
                pass
            case _ as unreachable:
                assert_never(unreachable)
    return errors


def worker_profiles_shape_errors(
    profiles: Mapping[str, WorkerProfile],
) -> list[ConfigurationError]:
    """The name and children errors of every profile, in name order."""
    errors: list[ConfigurationError] = []
    for name in sorted(profiles):
        errors.extend(profile_name_errors(name))
        errors.extend(profile_children_errors(name, profiles[name]))
    return errors


def worker_profiles_claim_errors(
    profiles: Mapping[str, WorkerProfile],
    prefetch_buffer: int,
) -> list[ConfigurationError]:
    """The claim errors of every profile with children, in name order.

    ``prefetch_buffer`` is the validated ``AppConfig.prefetch_buffer`` (0 or
    more). A profile with no children is skipped: the shape check already
    refuses it, and it has no capacity to compare with.
    """
    errors: list[ConfigurationError] = []
    for name in sorted(profiles):
        profile = profiles[name]
        if profile.child_count() >= 1:
            errors.extend(profile_claim_errors(name, profile, prefetch_buffer))
    return errors
