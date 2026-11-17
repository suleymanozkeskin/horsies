"""Resolve the Pydantic app config into the core's worker manifest.

`to_worker_manifest` is the one place where the user config of a worker
(Pydantic) meets the input of the core's worker (the manifest generated from
horsies-core, `python/horsies/_catalog/worker_manifest.py`). It maps every worker
profile, the database settings, the queue and claim settings, the recovery,
retention, resilience and payload settings, the registered task classes and
the child launch. It does not pick the profile: the core checks the profile
set and picks the requested profile, so each client sends the set and the
`--profile` value as they are.

AppConfig validation refuses every user error first, the values above a
bound of the worker manifest included (`prefetch_buffer`, the claim lease,
the claim renew age, `cluster_wide_cap`, a queue's `max_concurrency`, the
payload thresholds; `horsies.core.models.app.worker_bound_errors`,
`PayloadPolicy`). `to_worker_manifest` has no failure: a value above its
manifest bound is a defect and fails an assertion that names the path, the
value and the bound. Any refusal by the core is a defect of this module.
"""

from __future__ import annotations

import dataclasses
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Final, assert_never

from horsies._catalog import worker_manifest as wire
from horsies.core.history.periods import parse_retention_period
from horsies.core.manifest import reservation_window_micros
from horsies.core.manifest_database import (
    core_database_url,
    pool_recycle_with,
    session_database_with,
    tcp_keepalives_with,
    tcp_user_timeout_with,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.resilience import WorkerResilienceConfig
from horsies.core.models.retention import RetentionConfig
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
from horsies.core.types.result import Err, Ok
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.cpu_placement import ParentCpu

CHILD_MAIN_MODULE: Final = 'horsies.core.worker.child_main'
"""The module that a Python child runs (`python -m <module> <fd> <worker pid>`)."""

PRELOAD_MAIN_MODULE: Final = 'horsies.core.worker.preload_main'
"""The module that the preload process runs
(`python -m <module> <fd> <worker pid> <mode>`)."""

_ONE_MILLISECOND: Final = timedelta(milliseconds=1)
_ONE_MICROSECOND: Final = timedelta(microseconds=1)


def to_worker_manifest(
    config: AppConfig,
    *,
    profile: str | None,
    tasks: Mapping[str, Execution],
    child_launch: wire.ChildLaunchManifest,
    idempotency_reservation_window: timedelta,
) -> wire.WorkerManifest:
    """The worker manifest of `config`.

    Pure: no I/O. `profile` is the `--profile` value (`None` when the option
    was not passed); `tasks` is the execution class of each registered task
    by name; `child_launch` comes from `python_child_launch`; the window is
    one that `validate_reservation_window` accepted. `config` passed
    AppConfig validation, which keeps every value within its manifest bound;
    a value above it fails an assertion (`worker_claim_manifest`,
    `payload_manifest`).
    """
    return wire.WorkerManifest(
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
        claim=worker_claim_manifest(config),
        recovery=recovery_manifest(config.recovery),
        retention=retention_manifest(config.retention),
        resilience=resilience_manifest(config.resilience),
        payload=payload_manifest(config.payload),
        tasks={name: execution_of(execution) for name, execution in tasks.items()},
        child_launch=child_launch,
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
    interpreter_flags: Sequence[str],
    app_locator: str,
    imports: Sequence[str],
    sys_path_roots: Sequence[str],
    parent_sys_path: Sequence[str],
    parent_argv: Sequence[str],
    log_level: wire.WorkerLogLevel,
) -> wire.ChildLaunchManifest:
    """How a Python child of this worker and its preload process start:
    `executable <interpreter_flags> -m horsies.core.worker.child_main` and
    `executable <interpreter_flags> -m horsies.core.worker.preload_main`,
    importing the app at `app_locator`.

    Pure: the caller reads `sys.executable`, the flags
    (`current_interpreter_flags`), `sys.path` and `sys.argv`.
    """
    flags = tuple(interpreter_flags)
    return wire.ChildLaunchManifest(
        command=wire.ChildCommandManifest(
            program=executable,
            args=(*flags, '-m', CHILD_MAIN_MODULE),
        ),
        preload=wire.PreloadLaunchCommand(
            command=wire.ChildCommandManifest(
                program=executable,
                args=(*flags, '-m', PRELOAD_MAIN_MODULE),
            ),
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


def current_interpreter_flags() -> tuple[str, ...]:
    """The command-line flags of this interpreter, rebuilt from `sys.flags`
    and `sys.warnoptions`.

    A plain `exec` does not inherit them, and `-O` changes what `assert`
    does in task code, so each child and the preload process start with
    them. Reads process state; no I/O.
    """
    flags = sys.flags
    args: list[str] = []
    if flags.optimize:
        args.append('-' + 'O' * flags.optimize)
    if flags.dont_write_bytecode:
        args.append('-B')
    match flags.isolated:
        case 0:
            if flags.ignore_environment:
                args.append('-E')
            if flags.no_user_site:
                args.append('-s')
            if flags.safe_path:
                args.append('-P')
        case _:
            args.append('-I')
    if flags.no_site:
        args.append('-S')
    if flags.dev_mode:
        args.extend(('-X', 'dev'))
    args.extend(arg for option in sys.warnoptions for arg in ('-W', option))
    return tuple(args)


def recovery_manifest(config: RecoveryConfig) -> wire.RecoveryManifest:
    """The recovery section: every field of `config`, each default as the
    value it holds. Pure; Pydantic bounds every field.
    """
    return wire.RecoveryManifest(
        auto_requeue_stale_claimed=config.auto_requeue_stale_claimed,
        claimed_stale_threshold_ms=config.claimed_stale_threshold_ms,
        auto_terminate_orphaned_workflow_tasks=config.auto_terminate_orphaned_workflow_tasks,
        auto_fail_stale_running=config.auto_fail_stale_running,
        running_stale_threshold_ms=config.running_stale_threshold_ms,
        finalizing_stale_threshold_ms=config.finalizing_stale_threshold_ms,
        crashed_worker_recovery_grace_ms=config.crashed_worker_recovery_grace_ms,
        phase2_quarantine_after_attempts=config.phase2_quarantine_after_attempts,
        workflow_recovery_failure_limit=config.workflow_recovery_failure_limit,
        check_interval_ms=config.check_interval_ms,
        orphan_task_audit_interval_ms=config.orphan_task_audit_interval_ms,
        runner_heartbeat_interval_ms=config.runner_heartbeat_interval_ms,
        claimer_heartbeat_interval_ms=config.claimer_heartbeat_interval_ms,
        worker_state_snapshot_interval_ms=config.worker_state_snapshot_interval_ms,
    )


def retention_manifest(config: RetentionConfig) -> wire.RetentionManifest:
    """The retention section of `config`. A `None` that turns pruning or the
    paused expiry off is its own variant; a period is whole days. Pure;
    Pydantic bounds every field, the paused age at `PAUSED_AGE_MICROS_MAX`
    microseconds included.
    """
    return wire.RetentionManifest(
        worker_state_retention=_worker_state_retention(
            config.worker_state_retention_hours,
        ),
        terminal_record_retention=_terminal_record_retention(
            config.terminal_record_retention_hours,
        ),
        paused_workflow_auto_cancel_after=_paused_expiry(
            config.paused_workflow_auto_cancel_after,
        ),
        history_leaf_horizon_days=config.history_leaf_horizon_days,
        heartbeat_leaf_horizon_hours=config.heartbeat_leaf_horizon_hours,
        retention_periods=tuple(
            _period_days(period) for period in config.retention_periods
        ),
        queue_retention={
            queue: _queue_retention(period)
            for queue, period in config.queue_retention.items()
        },
        partition_maintenance_interval_s=config.partition_maintenance_interval_s,
        retention_sweep_interval_s=config.retention_sweep_interval_s,
        retention_delete_batch_size=config.retention_delete_batch_size,
    )


def resilience_manifest(config: WorkerResilienceConfig) -> wire.ResilienceManifest:
    """The resilience section of `config`; `db_retry_max_attempts=0` (no
    limit) is its own variant. Pure.
    """
    return wire.ResilienceManifest(
        db_retry_initial_ms=config.db_retry_initial_ms,
        db_retry_max_ms=config.db_retry_max_ms,
        db_retry_max_attempts=_retry_attempt_limit(config.db_retry_max_attempts),
        notify_poll_interval_ms=config.notify_poll_interval_ms,
    )


def payload_manifest(config: PayloadPolicy) -> wire.PayloadManifest:
    """The payload section of `config`; a `None` threshold is no limit. Pure.

    `PayloadPolicy` validation keeps each threshold at or below
    `PAYLOAD_BYTES_MAX`; a larger one fails an assertion (`_at_most`).
    """
    return wire.PayloadManifest(
        warn_bytes=_payload_limit('payload.warn_bytes', config.warn_bytes),
        reject_bytes=_payload_limit('payload.reject_bytes', config.reject_bytes),
    )


def worker_database_manifest(config: PostgresConfig) -> wire.WorkerDatabaseManifest:
    """The database section of a worker. `pool_size` and `max_overflow` do not
    apply: the pool of the worker process is the resolved pool of its
    profile with no overflow. The TCP keepalive and user timeout settings
    apply to every core connection of the worker. Pure.
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
        tcp_keepalives=tcp_keepalives_of(config),
        tcp_user_timeout=tcp_user_timeout_of(config.tcp_user_timeout_ms),
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


def dump_worker_profiles(profiles: Mapping[str, WorkerProfile]) -> str:
    """The JSON text of `profiles` in the form of the `profiles` member of
    the worker manifest: each profile as `worker_profile_manifest` gives it,
    by name, written as `dump_worker_manifest` writes the manifest. The live
    check sends it to the core (`CoreHandle.check_worker_connections`). Pure.
    """
    return json.dumps(
        {
            name: dataclasses.asdict(worker_profile_manifest(profile))
            for name, profile in profiles.items()
        },
        separators=(',', ':'),
        allow_nan=False,
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


def worker_claim_manifest(config: AppConfig) -> wire.WorkerClaimManifest:
    """The queue and claim settings of `config`. Pure.

    AppConfig validation keeps `prefetch_buffer`, `claim_lease_ms`,
    `max_claim_renew_age_ms`, `cluster_wide_cap` and each queue's
    `max_concurrency` at or below their manifest bounds
    (`worker_bound_errors`); a larger value fails an assertion (`_at_most`).
    """
    return wire.WorkerClaimManifest(
        queues=_queues(config.queue_mode, config.custom_queues),
        prefetch_buffer=_at_most(
            'prefetch_buffer', config.prefetch_buffer, wire.CLAIM_COUNT_MAX,
        ),
        claim_lease=_claim_lease(config.claim_lease_ms),
        max_claim_renew_age_ms=_at_most(
            'max_claim_renew_age_ms',
            config.max_claim_renew_age_ms,
            wire.MILLISECONDS_MAX,
        ),
        cluster_wide_cap=_cluster_wide_cap(config.cluster_wide_cap),
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


def _at_most(path: str, value: int, maximum: int) -> int:
    """`value`, which validation keeps at or below `maximum`, the bound of
    its manifest field. A larger value is a defect of that validation: it
    fails an assertion that names `path` (as the user writes it in
    `AppConfig`), the value and the bound. Pure."""
    assert value <= maximum, (
        f'{path} = {value} is above the bound that a worker takes ({maximum}); '
        'config validation refuses such a value'
    )
    return value


def _claim_lease(claim_lease_ms: int | None) -> wire.ClaimLease:
    match claim_lease_ms:
        case None:
            return wire.ClaimLeaseWorkerDefault()
        case milliseconds:
            return wire.ClaimLeaseMilliseconds(
                ms=_at_most('claim_lease_ms', milliseconds, wire.MILLISECONDS_MAX),
            )


def _cluster_wide_cap(cluster_wide_cap: int | None) -> wire.ClusterWideCap:
    match cluster_wide_cap:
        case None:
            return wire.ClusterWideCapNoLimit()
        case cap:
            return wire.ClusterWideCapTasks(
                tasks=_at_most('cluster_wide_cap', cap, wire.TASK_LIMIT_MAX),
            )


def _queues(
    queue_mode: QueueMode,
    custom_queues: Sequence[CustomQueueConfig] | None,
) -> wire.Queues:
    match queue_mode:
        case QueueMode.DEFAULT:
            return wire.QueuesDefault()
        case QueueMode.CUSTOM:
            pass
        case _ as unreachable:
            assert_never(unreachable)
    assert (
        custom_queues is not None
    ), 'AppConfig validation requires custom_queues in CUSTOM mode'
    return wire.QueuesCustom(
        queues=tuple(
            wire.CustomQueueManifest(
                name=queue.name,
                priority=queue.priority,
                max_concurrency=_queue_concurrency(position, queue.max_concurrency),
            )
            for position, queue in enumerate(custom_queues)
        ),
    )


def _queue_concurrency(
    position: int,
    max_concurrency: int | None,
) -> wire.QueueConcurrency:
    match max_concurrency:
        case None:
            return wire.QueueConcurrencyNoLimit()
        case limit:
            return wire.QueueConcurrencyTasks(
                tasks=_at_most(
                    f'custom_queues[{position}].max_concurrency',
                    limit,
                    wire.QUEUE_CONCURRENCY_MAX,
                ),
            )


def _paused_expiry(age: timedelta | None) -> wire.PausedWorkflowExpiry:
    """The paused expiry of an age that `RetentionConfig` validation
    accepted.

    Validation refuses an age of zero or less and an age above
    `PAUSED_AGE_MICROS_MAX` microseconds, so such an age here is a defect
    and raises `AssertionError`.
    """
    match age:
        case None:
            return wire.PausedWorkflowExpiryNoExpiry()
        case positive:
            micros = positive // _ONE_MICROSECOND
            if not wire.PAUSED_AGE_MICROS_MIN <= micros <= wire.PAUSED_AGE_MICROS_MAX:
                raise AssertionError(
                    'a validated paused workflow age is from '
                    f'{wire.PAUSED_AGE_MICROS_MIN} to '
                    f'{wire.PAUSED_AGE_MICROS_MAX} microseconds, got {micros}',
                )
            return wire.PausedWorkflowExpiryAfter(micros=micros)


def _worker_state_retention(hours: int | None) -> wire.WorkerStateRetention:
    match hours:
        case None:
            return wire.WorkerStateRetentionNoPruning()
        case kept:
            return wire.WorkerStateRetentionHours(hours=kept)


def _terminal_record_retention(hours: int | None) -> wire.TerminalRecordRetention:
    match hours:
        case None:
            return wire.TerminalRecordRetentionNoPruning()
        case kept:
            return wire.TerminalRecordRetentionHours(hours=kept)


def _period_days(period: timedelta) -> int:
    """The days of a period that `RetentionConfig` validation accepted.

    Validation refuses a period that is not whole days in bounds, so a
    refusal here is a defect and raises `AssertionError`.
    """
    match parse_retention_period(period):
        case Ok(days):
            return days
        case Err(refused):
            raise AssertionError(
                f'a validated retention period is whole days in bounds: {refused.reason}',
            )
        case _ as unreachable:
            assert_never(unreachable)


def _queue_retention(period: timedelta | None) -> wire.QueueRetention:
    match period:
        case None:
            return wire.QueueRetentionForever()
        case finite:
            return wire.QueueRetentionDays(days=_period_days(finite))


def _retry_attempt_limit(attempts: int) -> wire.RetryAttemptLimit:
    match attempts:
        case 0:
            return wire.RetryAttemptLimitNoLimit()
        case limit:
            return wire.RetryAttemptLimitAttempts(attempts=limit)


def _payload_limit(path: str, threshold: int | None) -> wire.PayloadLimit:
    match threshold:
        case None:
            return wire.PayloadLimitNoLimit()
        case limit:
            return wire.PayloadLimitBytes(
                bytes=_at_most(path, limit, wire.PAYLOAD_BYTES_MAX),
            )


def _session_database(config: PostgresConfig) -> wire.SessionDatabase:
    return session_database_with(
        config,
        same_as_database_url=wire.SessionDatabaseSameAsDatabaseUrl,
        separate=wire.SessionDatabaseSeparate,
    )


def _pool_recycle(value: int) -> wire.PoolRecycle:
    """`pool_recycle_with` with the classes of the worker manifest."""
    return pool_recycle_with(
        value,
        never=wire.PoolRecycleNever,
        after_seconds=wire.PoolRecycleAfterSeconds,
    )


def tcp_keepalives_of(config: PostgresConfig) -> wire.TcpKeepalivesConfig:
    """`tcp_keepalives_with` with the classes of the worker manifest."""
    return tcp_keepalives_with(
        config,
        off=wire.TcpKeepalivesConfigOff,
        on=wire.TcpKeepalivesConfigOn,
    )


def tcp_user_timeout_of(milliseconds: int) -> wire.TcpUserTimeoutConfig:
    """`tcp_user_timeout_with` with the classes of the worker manifest."""
    return tcp_user_timeout_with(
        milliseconds,
        system_default=wire.TcpUserTimeoutConfigSystemDefault,
        after=wire.TcpUserTimeoutConfigAfter,
    )


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
