"""Resolve the Pydantic app config into the core's worker manifest.

`to_worker_manifest` is the one place where the user config of a worker
(Pydantic) meets the input of the core's worker (the manifest generated from
horsies-core, `clients/python/horsies/_catalog/worker_manifest.py`). It maps every worker
profile, the database settings, the queue and claim settings, the recovery,
retention, resilience and payload settings, the facts of each registered
task (its execution class and its declared error codes) and the child
launch, the event handlers of the app, and the worker-ended span switch of
its tracing setting. It does not pick the profile: the core checks the
profile set and picks the requested profile, so each client sends the set
and the `--profile` value as they are.

AppConfig validation refuses every user error first, the values above a
bound of the worker manifest included (`prefetch_buffer`, the claim lease,
the claim renew age, `cluster_wide_cap`, a queue's `max_concurrency`, the
payload thresholds; `horsies.core.models.app.worker_bound_errors`,
`PayloadPolicy`). `to_worker_manifest` has no failure: a value above its
manifest bound is a defect and fails an assertion that names the path, the
value and the bound. Any refusal by the core is a defect of this module.

The child launch has the rules of the Python worker of 0.7.0: each child
restores the `sys.path` of the worker (`child_sys_path`) and its `sys.argv`
(`child_argv`) as text that the manifest can carry, and an app locator in
`__main__` is refused (`python_child_launch`).
"""

from __future__ import annotations

import dataclasses
import json
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Final, assert_never, cast

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
from horsies.core.models.tracing import (
    NoTracing,
    OtelTracing,
    WorkerEndedSpans,
    worker_ended_spans_of,
)
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
from horsies.core.worker.child_frames import MAX_EVENT_HANDLERS, MAX_HANDLER_NAME_CHARS
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.cpu_placement import ParentCpu

CHILD_MAIN_MODULE: Final = 'horsies.core.worker.child_main'
"""The module that a Python child runs (`python -m <module> <fd> <worker pid>`)."""

PRELOAD_MAIN_MODULE: Final = 'horsies.core.worker.preload_main'
"""The module that the preload process runs
(`python -m <module> <fd> <worker pid> <mode>`)."""

_ONE_MILLISECOND: Final = timedelta(milliseconds=1)
_ONE_MICROSECOND: Final = timedelta(microseconds=1)


@dataclasses.dataclass(frozen=True, slots=True)
class TaskFacts:
    """The facts of one registered task that the worker manifest carries:
    its execution class and the error codes that it declares, in the order
    of the declaration (`task_error_codes`, which the decorator checked)."""

    execution: Execution
    error_codes: tuple[str, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class HandlerFacts:
    """The facts of one registered event handler that the worker manifest
    carries: its name (`module.qualname`) and the event type that it takes.
    `Horsies.on_event` checked both (`check_event_handler`)."""

    name: str
    event_type: wire.EventTypeName


def to_worker_manifest(
    config: AppConfig,
    *,
    profile: str | None,
    metrics_endpoint: wire.MetricsEndpoint,
    tasks: Mapping[str, TaskFacts],
    handlers: Sequence[HandlerFacts],
    child_launch: wire.ChildLaunchManifest,
    idempotency_reservation_window: timedelta,
) -> wire.WorkerManifest:
    """The worker manifest of `config`.

    Pure: no I/O. `profile` is the `--profile` value (`None` when the option
    was not passed); `metrics_endpoint` is the `--metrics-listen` value
    (`MetricsEndpointNoMetricsEndpoint` when the option was not passed);
    `tasks` holds the facts of each registered task by name; `handlers`
    holds the facts of each event handler, in registration order
    (`handlers_manifest`); `child_launch` comes from `python_child_launch`;
    the window is one that `validate_reservation_window` accepted. `config`
    passed AppConfig validation, which keeps every value within its
    manifest bound; a value above it fails an assertion
    (`worker_claim_manifest`, `payload_manifest`). `worker_ended_spans`
    comes from the tracing setting (`worker_ended_spans_manifest`): with
    `MAKE_SPANS` the worker starts its handler child also when the app has
    no handler.
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
        metrics_endpoint=metrics_endpoint,
        claim=worker_claim_manifest(config),
        recovery=recovery_manifest(config.recovery),
        retention=retention_manifest(config.retention),
        resilience=resilience_manifest(config.resilience),
        payload=payload_manifest(config.payload),
        tasks={name: task_manifest(facts) for name, facts in tasks.items()},
        handlers=handlers_manifest(handlers),
        worker_ended_spans=worker_ended_spans_manifest(config.tracing),
        child_launch=child_launch,
    )


def task_manifest(facts: TaskFacts) -> wire.TaskManifest:
    """The manifest record of one registered task. Pure. The core parser
    checks the codes again and keeps them sorted by text."""
    return wire.TaskManifest(
        execution=execution_of(facts.execution),
        error_codes=facts.error_codes,
    )


def worker_ended_spans_manifest(tracing: OtelTracing | NoTracing) -> wire.WorkerEndedSpans:
    """The `worker_ended_spans` field of the manifest: `MAKE_SPANS` for
    `OtelTracing(worker_ended_spans=WorkerEndedSpans.MAKE_SPANS)`, the
    default of `OtelTracing`; `NO_SPANS` for `NoTracing` and for
    `WorkerEndedSpans.NO_SPANS`. Pure."""
    match worker_ended_spans_of(tracing):
        case WorkerEndedSpans.MAKE_SPANS:
            return 'MAKE_SPANS'
        case WorkerEndedSpans.NO_SPANS:
            return 'NO_SPANS'


def handlers_manifest(handlers: Sequence[HandlerFacts]) -> wire.EventHandlers:
    """The event handlers of the manifest: `NO_HANDLERS` for none, else
    `HANDLERS` with each handler in the order of `handlers`. Pure.

    `Horsies.on_event` keeps at most `MAX_EVENT_HANDLERS` handlers, each name
    once and within `MAX_HANDLER_NAME_CHARS`; a list that breaks a bound is a
    defect and fails an assertion.
    """
    names = [facts.name for facts in handlers]
    assert len(names) <= MAX_EVENT_HANDLERS, (
        f'handlers: {len(names)} handlers, above the bound {MAX_EVENT_HANDLERS}'
    )
    assert len(set(names)) == len(names), f'handlers: a name is repeated in {names}'
    assert all(1 <= len(name) <= MAX_HANDLER_NAME_CHARS for name in names), (
        f'handlers: a name is empty or above {MAX_HANDLER_NAME_CHARS} characters'
    )
    match handlers:
        case []:
            return wire.EventHandlersNoHandlers()
        case _:
            return wire.EventHandlersHandlers(
                handlers=tuple(
                    wire.HandlerManifest(name=facts.name, event_type=facts.event_type)
                    for facts in handlers
                ),
            )


def profile_request(profile: str | None) -> wire.ProfileRequest:
    """The request of the `--profile` value; `None` is no request. Pure."""
    match profile:
        case None:
            return wire.ProfileRequestNotGiven()
        case name:
            return wire.ProfileRequestNamed(name=name)


@dataclasses.dataclass(frozen=True, slots=True)
class CurrentDirectory:
    """The current directory of the worker process."""

    path: str


@dataclasses.dataclass(frozen=True, slots=True)
class CurrentDirectoryUnreadable:
    """`os.getcwd()` failed: the directory was removed, or a part of its
    path cannot be read. `cause` names the OS error."""

    cause: str


type CurrentDirectoryRead = CurrentDirectory | CurrentDirectoryUnreadable
"""What a read of the current directory of the worker process gave."""


@dataclasses.dataclass(frozen=True, slots=True)
class LeftOutEntry:
    """An entry of the worker's `sys.path` that no child gets: `entry` is
    its `repr`, `reason` says why."""

    entry: str
    reason: str


@dataclasses.dataclass(frozen=True, slots=True)
class ChildSysPath:
    """The `sys.path` that each child restores (`child_sys_path`), and the
    entries of the worker's `sys.path` that it leaves out."""

    entries: tuple[str, ...]
    left_out: tuple[LeftOutEntry, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class ChildArgv:
    """The `sys.argv` that each child restores (`child_argv`)."""

    args: tuple[str, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class MainModuleLocator:
    """The app locator names the module `__main__`. A child is a new
    program with its own `__main__`, so the app is not there."""

    app_locator: str


_NOT_A_PATH: Final = 'it is not a str, bytes or os.PathLike path'
_NOT_UTF8: Final = 'it is not valid UTF-8'
_SURROGATES: Final = range(0xD800, 0xE000)
"""The code points that UTF-8 cannot encode. `os.fsdecode` writes each
byte that the file system encoding cannot decode as one of them."""


def child_sys_path(
    parent_sys_path: Sequence[object],
    current_directory: CurrentDirectoryRead,
) -> ChildSysPath:
    """The `sys.path` of the worker process as each child restores it, in
    order (Python 0.7.0 `_frame_sys_path`). Pure: the caller reads
    `sys.path` and the current directory.

    `sys.path` may hold any object; Python's import system skips an entry
    that it cannot use. Each entry:

    - `str`: kept.
    - `bytes` or `os.PathLike` (a `pathlib.Path`): decoded with
      `os.fsdecode`.
    - `''` (the current directory, also after the decode): the path of
      `current_directory`; left out when the directory is unreadable.
    - An object of another type, or an `os.PathLike` whose `__fspath__`
      gives neither `str` nor `bytes`: left out.
    - Text that is not valid UTF-8 (a lone surrogate from a byte that the
      file system encoding could not decode): left out. The manifest is
      UTF-8 JSON, so the entry could not be restored as it was.
    """
    entries: list[str] = []
    left_out: list[LeftOutEntry] = []
    for entry in parent_sys_path:
        match _child_entry(entry, current_directory):
            case str() as text:
                entries.append(text)
            case LeftOutEntry() as left:
                left_out.append(left)
            case _ as unreachable:
                assert_never(unreachable)
    return ChildSysPath(entries=tuple(entries), left_out=tuple(left_out))


def _child_entry(
    entry: object,
    current_directory: CurrentDirectoryRead,
) -> str | LeftOutEntry:
    """One entry of `child_sys_path`. Pure."""
    match _path_text(entry):
        case LeftOutEntry() as left:
            return left
        case '':
            match current_directory:
                case CurrentDirectory(path=path):
                    text = path
                case CurrentDirectoryUnreadable(cause=cause):
                    return LeftOutEntry(
                        repr(entry),
                        f'the current directory cannot be read: {cause}',
                    )
                case _ as unreachable:
                    assert_never(unreachable)
        case str() as decoded:
            text = decoded
        case _ as unreachable:
            assert_never(unreachable)
    if any(ord(char) in _SURROGATES for char in text):
        return LeftOutEntry(repr(entry), _NOT_UTF8)
    return text


def _path_text(entry: object) -> str | LeftOutEntry:
    """The text of one `sys.path` entry, as `os.fsdecode` gives it, or why
    the entry has none. Pure."""
    match entry:
        case str():
            return entry
        case bytes():
            return os.fsdecode(entry)
        case os.PathLike():
            # The class pattern cannot see the type argument; `os.fsdecode`
            # checks what `__fspath__` gives.
            return _path_like_text(cast('os.PathLike[str]', entry))
        case _:
            return LeftOutEntry(repr(entry), _NOT_A_PATH)


def _path_like_text(path: os.PathLike[str]) -> str | LeftOutEntry:
    """The text of an `os.PathLike` entry. Pure.

    `LeftOutEntry`: `os.fsdecode` raised `TypeError`, as `__fspath__` gave
    neither `str` nor `bytes`.
    """
    try:
        return os.fsdecode(path)
    except TypeError:
        return LeftOutEntry(repr(path), _NOT_A_PATH)


def child_argv(parent_argv: Sequence[object]) -> ChildArgv:
    """The `sys.argv` of the worker process as each child restores it
    (Python 0.7.0 `_frame_text`). Pure: the caller reads `sys.argv`.

    Each item becomes `str(item)`. A code point that is not valid UTF-8 (a
    lone surrogate from a byte that the file system encoding could not
    decode) is written as its backslash escape, `\\udcff`, so the manifest
    can carry it. No item is left out, so each keeps its position.
    """
    return ChildArgv(
        args=tuple(
            str(item).encode('utf-8', 'backslashreplace').decode('utf-8')
            for item in parent_argv
        ),
    )


def python_child_launch(
    *,
    executable: str,
    interpreter_flags: Sequence[str],
    app_locator: str,
    imports: Sequence[str],
    sys_path_roots: Sequence[str],
    parent_sys_path: ChildSysPath,
    parent_argv: ChildArgv,
    log_level: wire.WorkerLogLevel,
) -> Result[wire.ChildLaunchManifest, MainModuleLocator]:
    """How a Python child of this worker and its preload process start:
    `executable <interpreter_flags> -m horsies.core.worker.child_main` and
    `executable <interpreter_flags> -m horsies.core.worker.preload_main`,
    importing the app at `app_locator`.

    Pure: the caller reads `sys.executable` and the flags
    (`current_interpreter_flags`); `parent_sys_path` and `parent_argv` come
    from `child_sys_path` and `child_argv`.

    Err: `MainModuleLocator` for a locator in the module `__main__`
    (`__main__:app`). The Python worker of 0.7.0 refused it at its start.
    """
    match app_locator.partition(':'):
        case ('__main__', ':', _):
            return Err(MainModuleLocator(app_locator))
        case _:
            pass
    flags = tuple(interpreter_flags)
    return Ok(
        wire.ChildLaunchManifest(
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
                parent_sys_path=parent_sys_path.entries,
                parent_argv=parent_argv.args,
            ),
            log_level=log_level,
        ),
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
