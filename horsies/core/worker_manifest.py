"""Resolve the Pydantic app config into the core's worker manifest.

`to_worker_manifest` is the one place where the user config of a worker
(Pydantic) meets the input of the core's worker (the manifest generated from
horsies-core, `horsies/_catalog/worker_manifest.py`). It maps every worker
profile, the database settings, the queue and claim settings, the
registered task classes and the child launch. It does not pick the profile:
the core checks the profile set and picks the requested profile, so each
client sends the set and the `--profile` value as they are.

Pydantic refuses every user error first. The manifest bounds a few values
that the config does not bound above (`prefetch_buffer`, the claim lease,
the claim renew age, `cluster_wide_cap`, a queue's `max_concurrency`): a
value above its manifest bound is a `ValueAboveManifestBound`, the only
failure of `to_worker_manifest`. Any other refusal by the core is a defect
of this module.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Final, assert_never

from horsies._catalog import worker_manifest as wire
from horsies.core.manifest import core_database_url, reservation_window_micros
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import SQLALCHEMY_NEVER_RECYCLE, PostgresConfig
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.core.models.tasks import Execution
from horsies.core.models.worker_profile import (
    ExecutorThreads,
    ExecutorThreadsFromSeats,
    IsolatedChildren,
    MemoryLimit,
    NoIsolatedChildren,
    NoMemoryLimit,
    NoSharedAsyncChildren,
    NoSharedThreadChildren,
    NoTaskLimit,
    PoolSize,
    PoolSizeFromSeats,
    SharedAsyncChildren,
    SharedPool,
    SharedRecycle,
    SharedThreadChildren,
    TaskLimit,
    WorkerProfile,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.cpu_placement import ParentCpu

CHILD_MAIN_MODULE: Final = 'horsies.core.worker.child_main'
"""The module that a Python child runs (`python -m <module> <fd> <worker pid>`)."""

_ONE_MILLISECOND: Final = timedelta(milliseconds=1)


@dataclass(frozen=True, slots=True)
class ValueAboveManifestBound:
    """A config value that Pydantic accepted and that is above the bound of
    its manifest field.

    `path` names the field as the user writes it in `AppConfig`, for example
    `custom_queues[0].max_concurrency`. The claim function of the core takes
    each count as a Postgres `integer`, so a larger count never claims.
    """

    path: str
    value: int
    maximum: int


def to_worker_manifest(
    config: AppConfig,
    *,
    profile: str | None,
    tasks: Mapping[str, Execution],
    child_launch: wire.ChildLaunchManifest,
    idempotency_reservation_window: timedelta,
) -> Result[wire.WorkerManifest, ValueAboveManifestBound]:
    """The worker manifest of `config`.

    Pure: no I/O. `profile` is the `--profile` value (`None` when the option
    was not passed); `tasks` is the execution class of each registered task
    by name; `child_launch` comes from `python_child_launch`; the window is
    one that `validate_reservation_window` accepted.

    Errors: `ValueAboveManifestBound` for the first claim value above its
    manifest bound (`worker_claim_manifest`).
    """
    match worker_claim_manifest(config):
        case Ok(claim):
            pass
        case Err(above):
            return Err(above)
        case _ as unreachable:
            assert_never(unreachable)
    return Ok(
        wire.WorkerManifest(
            manifest_version=wire.MANIFEST_VERSION,
            database=worker_database_manifest(config.broker),
            send=wire.SendManifest(
                retain_rerun_input_default=config.broker.retain_rerun_input_default,
                idempotency_reservation_window=reservation_window_micros(
                    idempotency_reservation_window,
                ),
            ),
            profiles={
                name: worker_profile_manifest(worker_profile)
                for name, worker_profile in config.worker_profiles.items()
            },
            profile=profile_request(profile),
            claim=claim,
            tasks={name: execution_of(execution) for name, execution in tasks.items()},
            child_launch=child_launch,
        ),
    )


def profile_request(profile: str | None) -> wire.ProfileRequest:
    """The request of the `--profile` value; `None` is no request. Pure."""
    match profile:
        case None:
            return wire.ProfileRequestNotGiven()
        case name:
            return wire.ProfileRequestNamed(name=name)


def python_child_launch(
    *,
    executable: str,
    app_locator: str,
    imports: Sequence[str],
    sys_path_roots: Sequence[str],
    parent_sys_path: Sequence[str],
    parent_argv: Sequence[str],
    log_level: wire.WorkerLogLevel,
) -> wire.ChildLaunchManifest:
    """How a Python child of this worker starts: `executable -m
    horsies.core.worker.child_main`, importing the app at `app_locator`.

    Pure: the caller reads `sys.executable`, `sys.path` and `sys.argv`.
    """
    return wire.ChildLaunchManifest(
        command=wire.ChildCommandManifest(
            program=executable,
            args=('-m', CHILD_MAIN_MODULE),
        ),
        program=wire.ChildProgramPython(
            app_locator=app_locator,
            imports=tuple(imports),
            sys_path_roots=tuple(sys_path_roots),
            parent_sys_path=tuple(parent_sys_path),
            parent_argv=tuple(parent_argv),
        ),
        log_level=log_level,
    )


def worker_database_manifest(config: PostgresConfig) -> wire.WorkerDatabaseManifest:
    """The database section of a worker. `pool_size` and `max_overflow` do not
    apply: the pool of the worker process is the resolved pool of its
    profile with no overflow. Pure.
    """
    return wire.WorkerDatabaseManifest(
        database_url=core_database_url(config.database_url.get_secret_value()),
        session_database=_session_database(config),
        pgbouncer_transaction_mode=config.pgbouncer_transaction_mode,
        pool_pre_ping=config.pool_pre_ping,
        worker_pool_pre_ping=config.worker_pool_pre_ping,
        pool_timeout=config.pool_timeout,
        pool_recycle=_pool_recycle(config.pool_recycle),
        echo=config.echo,
    )


def worker_profile_manifest(profile: WorkerProfile) -> wire.WorkerProfileManifest:
    """`profile` with every value given: each default of the Pydantic model is
    the value it holds. Pure.
    """
    return wire.WorkerProfileManifest(
        isolated=_isolated(profile.isolated),
        shared_async=_shared_async(profile.shared_async),
        shared_thread=_shared_thread(profile.shared_thread),
        pool=wire.ParentPoolManifest(max_size=_pool_max_size(profile.pool.max_size)),
        claim=wire.ClaimLimitsManifest(
            per_queue_per_pass=_task_limit(profile.claim.per_queue_per_pass),
            claimed_per_worker=_task_limit(profile.claim.claimed_per_worker),
        ),
        drain_timeout_ms=drain_timeout_ms(profile.drain_timeout),
        preload=_preload(profile.preload),
        parent_cpu=_parent_cpu(profile.parent_cpu),
    )


def drain_timeout_ms(drain_timeout: timedelta) -> int:
    """`drain_timeout` in whole milliseconds, rounded up: the worker never
    waits less than the profile asks.

    The `WorkerProfile` field admits only a timedelta above 0 and at most 24
    hours, so a count outside `DRAIN_TIMEOUT_MS_MIN` to `DRAIN_TIMEOUT_MS_MAX`
    is a defect and raises `AssertionError`.
    """
    milliseconds = -(-drain_timeout // _ONE_MILLISECOND)
    assert wire.DRAIN_TIMEOUT_MS_MIN <= milliseconds <= wire.DRAIN_TIMEOUT_MS_MAX, (
        f'a validated drain timeout is from {wire.DRAIN_TIMEOUT_MS_MIN} to '
        f'{wire.DRAIN_TIMEOUT_MS_MAX} milliseconds; got {milliseconds}'
    )
    return milliseconds


def worker_claim_manifest(
    config: AppConfig,
) -> Result[wire.WorkerClaimManifest, ValueAboveManifestBound]:
    """The queue and claim settings of `config`. Pure.

    Errors: `ValueAboveManifestBound` for the first value above its manifest
    bound, in this order: `prefetch_buffer`, `claim_lease_ms`,
    `max_claim_renew_age_ms`, `cluster_wide_cap`, then each queue's
    `max_concurrency`. AppConfig validation keeps the renew age at or above
    the lease, so a lease above its bound comes with a renew age above it.
    """
    match _at_most('prefetch_buffer', config.prefetch_buffer, wire.CLAIM_COUNT_MAX):
        case Ok():
            pass
        case Err(above):
            return Err(above)
        case _ as unreachable:
            assert_never(unreachable)
    match _claim_lease(config.claim_lease_ms):
        case Ok(claim_lease):
            pass
        case Err(above):
            return Err(above)
        case _ as unreachable:
            assert_never(unreachable)
    renew_age = _at_most(
        'max_claim_renew_age_ms',
        config.max_claim_renew_age_ms,
        wire.MILLISECONDS_MAX,
    )
    match renew_age:
        case Ok():
            pass
        case Err(above):
            return Err(above)
        case _ as unreachable:
            assert_never(unreachable)
    match _cluster_wide_cap(config.cluster_wide_cap):
        case Ok(cluster_wide_cap):
            pass
        case Err(above):
            return Err(above)
        case _ as unreachable:
            assert_never(unreachable)
    match _queues(config.queue_mode, config.custom_queues):
        case Ok(queues):
            pass
        case Err(above):
            return Err(above)
        case _ as unreachable:
            assert_never(unreachable)
    return Ok(
        wire.WorkerClaimManifest(
            queues=queues,
            prefetch_buffer=config.prefetch_buffer,
            claim_lease=claim_lease,
            max_claim_renew_age_ms=config.max_claim_renew_age_ms,
            cluster_wide_cap=cluster_wide_cap,
        ),
    )


def execution_of(execution: Execution) -> wire.Execution:
    """The manifest value of a registered task's class. Pure."""
    match execution:
        case Execution.ISOLATED:
            return 'ISOLATED'
        case Execution.SHARED_ASYNC:
            return 'SHARED_ASYNC'
        case Execution.SHARED_THREAD:
            return 'SHARED_THREAD'
        case _ as unreachable:
            assert_never(unreachable)


def log_level_of(level: str) -> wire.WorkerLogLevel:
    """The manifest value of a `--loglevel` choice, in any case.

    The command line admits only the five levels, so another text is a defect
    and raises `AssertionError`.
    """
    match level.upper():
        case 'DEBUG':
            return 'DEBUG'
        case 'INFO':
            return 'INFO'
        case 'WARNING':
            return 'WARNING'
        case 'ERROR':
            return 'ERROR'
        case 'CRITICAL':
            return 'CRITICAL'
        case other:
            raise AssertionError(f'--loglevel admits five levels, not {other!r}')


def _at_most(
    path: str, value: int, maximum: int
) -> Result[int, ValueAboveManifestBound]:
    match value <= maximum:
        case True:
            return Ok(value)
        case False:
            return Err(ValueAboveManifestBound(path=path, value=value, maximum=maximum))


def _claim_lease(
    claim_lease_ms: int | None,
) -> Result[wire.ClaimLease, ValueAboveManifestBound]:
    match claim_lease_ms:
        case None:
            return Ok(wire.ClaimLeaseWorkerDefault())
        case milliseconds:
            match _at_most('claim_lease_ms', milliseconds, wire.MILLISECONDS_MAX):
                case Ok(ms):
                    return Ok(wire.ClaimLeaseMilliseconds(ms=ms))
                case Err() as above:
                    return above
                case _ as unreachable:
                    assert_never(unreachable)


def _cluster_wide_cap(
    cluster_wide_cap: int | None,
) -> Result[wire.ClusterWideCap, ValueAboveManifestBound]:
    match cluster_wide_cap:
        case None:
            return Ok(wire.ClusterWideCapNoLimit())
        case cap:
            match _at_most('cluster_wide_cap', cap, wire.TASK_LIMIT_MAX):
                case Ok(tasks):
                    return Ok(wire.ClusterWideCapTasks(tasks=tasks))
                case Err() as above:
                    return above
                case _ as unreachable:
                    assert_never(unreachable)


def _queues(
    queue_mode: QueueMode,
    custom_queues: Sequence[CustomQueueConfig] | None,
) -> Result[wire.Queues, ValueAboveManifestBound]:
    match queue_mode:
        case QueueMode.DEFAULT:
            return Ok(wire.QueuesDefault())
        case QueueMode.CUSTOM:
            pass
        case _ as unreachable:
            assert_never(unreachable)
    assert (
        custom_queues is not None
    ), 'AppConfig validation requires custom_queues in CUSTOM mode'
    manifests: list[wire.CustomQueueManifest] = []
    for position, queue in enumerate(custom_queues):
        match _queue_concurrency(position, queue.max_concurrency):
            case Ok(max_concurrency):
                manifests.append(
                    wire.CustomQueueManifest(
                        name=queue.name,
                        priority=queue.priority,
                        max_concurrency=max_concurrency,
                    ),
                )
            case Err() as above:
                return above
            case _ as unreachable:
                assert_never(unreachable)
    return Ok(wire.QueuesCustom(queues=tuple(manifests)))


def _queue_concurrency(
    position: int,
    max_concurrency: int | None,
) -> Result[wire.QueueConcurrency, ValueAboveManifestBound]:
    match max_concurrency:
        case None:
            return Ok(wire.QueueConcurrencyNoLimit())
        case limit:
            path = f'custom_queues[{position}].max_concurrency'
            match _at_most(path, limit, wire.QUEUE_CONCURRENCY_MAX):
                case Ok(tasks):
                    return Ok(wire.QueueConcurrencyTasks(tasks=tasks))
                case Err() as above:
                    return above
                case _ as unreachable:
                    assert_never(unreachable)


def _session_database(config: PostgresConfig) -> wire.SessionDatabase:
    match config.session_database_url:
        case None:
            return wire.SessionDatabaseSameAsDatabaseUrl()
        case session_url:
            return wire.SessionDatabaseSeparate(
                url=core_database_url(session_url.get_secret_value()),
            )


def _pool_recycle(value: int) -> wire.PoolRecycle:
    """The typed form of a validated `pool_recycle` (as
    `horsies.core.manifest.pool_recycle_of`, with the classes of this module).
    """
    match value:
        case _ if value == SQLALCHEMY_NEVER_RECYCLE:
            return wire.PoolRecycleNever()
        case _:
            assert value >= 0, (
                f'pool_recycle={value} passed the PostgresConfig validator, which '
                'admits only -1 and a number of seconds'
            )
            return wire.PoolRecycleAfterSeconds(seconds=value)


def _task_limit(setting: TaskLimit | NoTaskLimit) -> wire.TaskLimit:
    match setting:
        case TaskLimit(tasks=tasks):
            return wire.TaskLimitTasks(tasks=tasks)
        case NoTaskLimit():
            return wire.TaskLimitNoLimit()
        case _ as unreachable:
            assert_never(unreachable)


def _memory_limit(setting: MemoryLimit | NoMemoryLimit) -> wire.MemoryLimit:
    match setting:
        case MemoryLimit(mb=mb):
            return wire.MemoryLimitMegabytes(mb=mb)
        case NoMemoryLimit():
            return wire.MemoryLimitNoLimit()
        case _ as unreachable:
            assert_never(unreachable)


def _pool_max_size(setting: PoolSize | PoolSizeFromSeats) -> wire.PoolMaxSize:
    match setting:
        case PoolSize(connections=connections):
            return wire.PoolMaxSizeConnections(connections=connections)
        case PoolSizeFromSeats():
            return wire.PoolMaxSizeFromSeats()
        case _ as unreachable:
            assert_never(unreachable)


def _default_executor(
    setting: ExecutorThreads | ExecutorThreadsFromSeats,
) -> wire.DefaultExecutor:
    match setting:
        case ExecutorThreads(threads=threads):
            return wire.DefaultExecutorThreads(threads=threads)
        case ExecutorThreadsFromSeats():
            return wire.DefaultExecutorFromSeats()
        case _ as unreachable:
            assert_never(unreachable)


def _shared_recycle(recycle: SharedRecycle) -> wire.SharedRecycleManifest:
    return wire.SharedRecycleManifest(
        after_per_seat=_task_limit(recycle.after_per_seat),
        above=_memory_limit(recycle.above),
    )


def _shared_pool(pool: SharedPool) -> wire.SharedPoolManifest:
    return wire.SharedPoolManifest(
        min_size=pool.min_size,
        max_size=_pool_max_size(pool.max_size),
        check=pool.check,
    )


def _isolated(setting: IsolatedChildren | NoIsolatedChildren) -> wire.Isolated:
    match setting:
        case IsolatedChildren() as children:
            return wire.IsolatedChildren(
                count=children.count,
                recycle=wire.IsolatedRecycleManifest(
                    after=_task_limit(children.recycle.after),
                    above=_memory_limit(children.recycle.above),
                ),
                pool=wire.IsolatedPoolManifest(
                    min_size=children.pool.min_size,
                    max_size=children.pool.max_size.connections,
                    check=children.pool.check,
                ),
                task_send_pool=children.task_send_pool.connections,
            )
        case NoIsolatedChildren():
            return wire.IsolatedNoChildren()
        case _ as unreachable:
            assert_never(unreachable)


def _shared_async(
    setting: SharedAsyncChildren | NoSharedAsyncChildren,
) -> wire.SharedAsync:
    match setting:
        case SharedAsyncChildren() as children:
            return wire.SharedAsyncChildren(
                count=children.count,
                seats=children.seats,
                recycle=_shared_recycle(children.recycle),
                pool=_shared_pool(children.pool),
                executor=_default_executor(children.executor),
                task_send_pool=children.task_send_pool.connections,
            )
        case NoSharedAsyncChildren():
            return wire.SharedAsyncNoChildren()
        case _ as unreachable:
            assert_never(unreachable)


def _shared_thread(
    setting: SharedThreadChildren | NoSharedThreadChildren,
) -> wire.SharedThread:
    match setting:
        case SharedThreadChildren() as children:
            return wire.SharedThreadChildren(
                count=children.count,
                seats=children.seats,
                recycle=_shared_recycle(children.recycle),
                pool=_shared_pool(children.pool),
                task_send_pool=children.task_send_pool.connections,
            )
        case NoSharedThreadChildren():
            return wire.SharedThreadNoChildren()
        case _ as unreachable:
            assert_never(unreachable)


def _preload(preload: ChildPreload) -> wire.Preload:
    match preload:
        case ChildPreload.LIBRARY:
            return 'LIBRARY'
        case ChildPreload.APP:
            return 'APP'
        case ChildPreload.NONE:
            return 'NO_PRELOAD'
        case _ as unreachable:
            assert_never(unreachable)


def _parent_cpu(parent_cpu: ParentCpu) -> wire.ParentCpu:
    match parent_cpu:
        case ParentCpu.SHARED:
            return 'SHARED'
        case ParentCpu.OWN_CORE:
            return 'OWN_CORE'
        case _ as unreachable:
            assert_never(unreachable)
