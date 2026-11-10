"""Worker profiles that the e2e app instances share.

A test starts a worker with ``run_worker(instance, profile=...)``. Each
instance module sets ``worker_profiles=E2E_WORKER_PROFILES`` (or a dict that
adds to it), so every name below exists in every instance.

- ``isolated-N``: N isolated children, every other setting at its default.
- ``recycle-off-2``: 2 children that are never replaced after a task count.
- ``recycle-after-2-tasks``: 2 children, each replaced after 2 tasks.
- ``recycle-above-memory-2``: 2 children, each replaced when its RSS reaches
  ``MEMORY_RECYCLE_THRESHOLD_MB``.
- ``claim-batch-1``: 1 child; one claim pass takes at most 1 task per queue.
- ``split-isolated-1-shared-2x8``: 1 isolated child, and 2 shared async
  children with 8 seats each.
- ``shared-only-1x4``: no isolated child, and 1 shared async child with 4
  seats. It runs only ``SHARED_ASYNC`` tasks.
- ``split-isolated-1-shared-1x4``: 1 isolated child, and 1 shared async child
  with 4 seats, so the shared tasks of a test share one process.
- ``split-isolated-1-thread-2x4``: 1 isolated child, and 2 shared thread
  children with 4 seats each.
- ``thread-only-1x4``: no isolated child, and 1 shared thread child with 4
  seats. It runs only ``SHARED_THREAD`` tasks.

The child isolation instances add ``isolated-N-<preload>`` and
``drain-1s-<preload>`` for each ``ChildPreload`` mode, so each of their tests
runs on every way a child can start.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Final

from horsies.core.models.worker_profile import (
    ClaimLimits,
    IsolatedChildren,
    IsolatedRecycle,
    MemoryLimit,
    NoIsolatedChildren,
    NoTaskLimit,
    SharedAsyncChildren,
    SharedThreadChildren,
    TaskLimit,
    WorkerProfile,
)
from horsies.core.worker.config import ChildPreload

ISOLATED_COUNTS: Final = (1, 2, 3, 4, 6, 8)
"""The child counts that have an ``isolated-N`` profile."""

MEMORY_RECYCLE_THRESHOLD_MB: Final = 300
"""Above the warmed child baseline with margin, so the startup baseline
guard does not refuse the worker."""

RECYCLE_OFF: Final = 'recycle-off-2'
RECYCLE_AFTER_2_TASKS: Final = 'recycle-after-2-tasks'
RECYCLE_ABOVE_MEMORY: Final = 'recycle-above-memory-2'
CLAIM_BATCH_1: Final = 'claim-batch-1'
CLAIM_BATCH_1_TASKS: Final = 1
"""The per-queue-per-pass claim limit of ``claim-batch-1``."""
SPLIT_SHARED_2X8: Final = 'split-isolated-1-shared-2x8'
SPLIT_SHARED_SEATS: Final = 8
"""The seats of each shared async child of ``split-isolated-1-shared-2x8``."""
SHARED_ONLY_1X4: Final = 'shared-only-1x4'
SPLIT_SHARED_1X4: Final = 'split-isolated-1-shared-1x4'
SPLIT_THREAD_2X4: Final = 'split-isolated-1-thread-2x4'
SPLIT_THREAD_SEATS: Final = 4
"""The seats of each shared thread child of ``split-isolated-1-thread-2x4``."""
THREAD_ONLY_1X4: Final = 'thread-only-1x4'


def isolated_profile_name(count: int) -> str:
    """The name of the profile with ``count`` isolated children."""
    assert count in ISOLATED_COUNTS, f'no isolated-{count} profile'
    return f'isolated-{count}'


def isolated_profile(
    count: int,
    *,
    preload: ChildPreload = ChildPreload.LIBRARY,
) -> WorkerProfile:
    """``count`` isolated children with the default recycle and claim limits."""
    return WorkerProfile(isolated=IsolatedChildren(count=count), preload=preload)


E2E_WORKER_PROFILES: Final[dict[str, WorkerProfile]] = {
    **{
        isolated_profile_name(count): isolated_profile(count)
        for count in ISOLATED_COUNTS
    },
    RECYCLE_OFF: WorkerProfile(
        isolated=IsolatedChildren(
            count=2,
            recycle=IsolatedRecycle(after=NoTaskLimit()),
        ),
    ),
    RECYCLE_AFTER_2_TASKS: WorkerProfile(
        isolated=IsolatedChildren(
            count=2,
            recycle=IsolatedRecycle(after=TaskLimit(tasks=2)),
        ),
    ),
    RECYCLE_ABOVE_MEMORY: WorkerProfile(
        isolated=IsolatedChildren(
            count=2,
            recycle=IsolatedRecycle(
                above=MemoryLimit(mb=MEMORY_RECYCLE_THRESHOLD_MB),
            ),
        ),
    ),
    CLAIM_BATCH_1: WorkerProfile(
        isolated=IsolatedChildren(count=1),
        claim=ClaimLimits(per_queue_per_pass=TaskLimit(tasks=CLAIM_BATCH_1_TASKS)),
    ),
    SPLIT_SHARED_2X8: WorkerProfile(
        isolated=IsolatedChildren(count=1),
        shared_async=SharedAsyncChildren(count=2, seats=SPLIT_SHARED_SEATS),
    ),
    SHARED_ONLY_1X4: WorkerProfile(
        isolated=NoIsolatedChildren(),
        shared_async=SharedAsyncChildren(count=1, seats=4),
    ),
    SPLIT_THREAD_2X4: WorkerProfile(
        isolated=IsolatedChildren(count=1),
        shared_thread=SharedThreadChildren(count=2, seats=SPLIT_THREAD_SEATS),
    ),
    THREAD_ONLY_1X4: WorkerProfile(
        isolated=NoIsolatedChildren(),
        shared_thread=SharedThreadChildren(count=1, seats=4),
    ),
    SPLIT_SHARED_1X4: WorkerProfile(
        isolated=IsolatedChildren(count=1),
        shared_async=SharedAsyncChildren(count=1, seats=4),
    ),
}
"""The profiles that every e2e app instance defines."""


PRELOAD_COUNTS: Final = (1, 3, 4)
"""The child counts that have an ``isolated-N-<preload>`` profile."""

DRAIN_TIMEOUT_1S: Final = timedelta(seconds=1)


def preload_profile_name(count: int, preload: ChildPreload) -> str:
    """The name of the profile with ``count`` children started by ``preload``."""
    assert count in PRELOAD_COUNTS, f'no isolated-{count}-<preload> profile'
    return f'isolated-{count}-{preload.value}'


def drain_profile_name(preload: ChildPreload) -> str:
    """The name of the 1-child profile with a 1 s drain timeout."""
    return f'drain-1s-{preload.value}'


def preload_profiles() -> dict[str, WorkerProfile]:
    """The ``isolated-N-<preload>`` and ``drain-1s-<preload>`` profiles."""
    profiles: dict[str, WorkerProfile] = {}
    for preload in ChildPreload:
        for count in PRELOAD_COUNTS:
            profiles[preload_profile_name(count, preload)] = isolated_profile(
                count,
                preload=preload,
            )
        profiles[drain_profile_name(preload)] = WorkerProfile(
            isolated=IsolatedChildren(count=1),
            drain_timeout=DRAIN_TIMEOUT_1S,
            preload=preload,
        )
    return profiles
