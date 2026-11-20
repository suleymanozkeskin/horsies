"""Contract between the Pydantic app config and the core's worker manifest.

`to_worker_manifest` maps `AppConfig` into the manifest generated from
horsies-core (`clients/python/horsies/_catalog/worker_manifest.py`). These tests pin that
the bounds of the profile models are the bounds of the manifest, that every
profile field is mapped with each Pydantic default as a value, that the
drain timeout becomes whole milliseconds, that every `PostgresConfig` field
is mapped or named as outside the worker manifest, which claim values the
manifest cannot hold, that the recovery, retention, resilience and payload
sections map every field with the Pydantic bounds, how a Python child and
its preload process start, and the form of the dumped document.
"""

from __future__ import annotations

import dataclasses
import json
import os
import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Final, cast

import pytest
from annotated_types import Ge, Le
from pydantic import SecretStr, ValidationError

from horsies._catalog import worker_manifest as wire
from horsies.core.errors import ConfigurationError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.child_sizing import (
    DEFAULT_ISOLATED_POOL_CONNECTIONS,
    DEFAULT_TASK_SEND_POOL_CONNECTIONS,
)
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.core.models.recovery import (
    WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS,
    RecoveryConfig,
)
from horsies.core.models.resilience import WorkerResilienceConfig
from horsies.core.models.retention import PAUSED_WORKFLOW_AGE_MAX, RetentionConfig
from horsies.core.models.tasks import Execution
from horsies.core.models.worker_profile import (
    DEFAULT_DRAIN_TIMEOUT,
    DEFAULT_RECYCLE_AFTER_TASKS,
    DEFAULT_RECYCLE_AFTER_TASKS_PER_SEAT,
    MAX_CHILD_POOL_CONNECTIONS,
    MAX_DRAIN_TIMEOUT,
    MAX_EXECUTOR_THREADS,
    MAX_ISOLATED_CHILDREN,
    MAX_MEMORY_LIMIT_MB,
    MAX_SEATS_PER_SHARED_ASYNC_CHILD,
    MAX_SEATS_PER_SHARED_THREAD_CHILD,
    MAX_SHARED_ASYNC_CHILDREN,
    MAX_SHARED_THREAD_CHILDREN,
    MAX_TASK_LIMIT,
    ClaimLimits,
    ExecutorThreads,
    IsolatedChildren,
    IsolatedPool,
    IsolatedRecycle,
    MemoryLimit,
    NoMemoryLimit,
    NoTaskLimit,
    ParentPool,
    PoolSize,
    SharedAsyncChildren,
    SharedPool,
    SharedRecycle,
    SharedThreadChildren,
    TaskLimit,
    WorkerProfile,
)
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.cpu_placement import ParentCpu
from horsies.core.worker_manifest import (
    CHILD_MAIN_MODULE,
    PRELOAD_MAIN_MODULE,
    ChildArgv,
    CurrentDirectory,
    CurrentDirectoryUnreadable,
    LeftOutEntry,
    MainModuleLocator,
    TaskFacts,
    child_argv,
    child_sys_path,
    current_interpreter_flags,
    drain_timeout_ms,
    execution_of,
    log_level_of,
    payload_manifest,
    profile_request,
    python_child_launch,
    recovery_manifest,
    resilience_manifest,
    retention_manifest,
    tcp_keepalives_of,
    tcp_user_timeout_of,
    to_worker_manifest,
    worker_claim_manifest,
    worker_database_manifest,
    worker_profile_manifest,
)

pytestmark = [pytest.mark.unit]

MAIN_URL: Final = 'postgresql://user:secret@pooler:6432/app'
SESSION_URL: Final = 'postgresql://user:secret@direct:5432/app'

# WorkerProfile field -> the WorkerProfileManifest field it fills.
PROFILE_FIELDS: Final[dict[str, str]] = {
    'isolated': 'isolated',
    'shared_async': 'shared_async',
    'shared_thread': 'shared_thread',
    'pool': 'pool',
    'claim': 'claim',
    'drain_timeout': 'drain_timeout_ms',
    'preload': 'preload',
    'parent_cpu': 'parent_cpu',
}

# PostgresConfig field -> the worker manifest path it fills.
BROKER_FIELDS: Final[dict[str, str]] = {
    'database_url': 'database.database_url',
    'session_database_url': 'database.session_database',
    'pgbouncer_transaction_mode': 'database.pgbouncer_transaction_mode',
    'pool_pre_ping': 'database.pool_pre_ping',
    'worker_pool_pre_ping': 'database.worker_pool_pre_ping',
    'pool_timeout': 'database.pool_timeout',
    'pool_recycle': 'database.pool_recycle',
    'echo': 'database.echo',
    'tcp_keepalives': 'database.tcp_keepalives',
    'tcp_keepalives_idle': 'database.tcp_keepalives.idle_seconds',
    'tcp_keepalives_interval': 'database.tcp_keepalives.interval_seconds',
    'tcp_keepalives_count': 'database.tcp_keepalives.probes',
    'tcp_user_timeout_ms': 'database.tcp_user_timeout',
    'retain_rerun_input_default': 'send.retain_rerun_input_default',
}

# PostgresConfig fields that the worker manifest does not hold: the pool of
# the worker process is the resolved pool of its profile with no overflow.
OUTSIDE_WORKER_MANIFEST: Final[frozenset[str]] = frozenset(
    {'pool_size', 'max_overflow'},
)

WINDOW: Final = timedelta(hours=24)


def _app_config(**overrides: Any) -> AppConfig:
    return AppConfig(
        broker=PostgresConfig(database_url=SecretStr(MAIN_URL)),
        **overrides,
    )


# RecoveryConfig field -> (its manifest bound, the low end of the field).
# Each field that is not a flag has a bound, and the bound is Pydantic's.
RECOVERY_BOUNDS: Final[dict[str, tuple[str, int]]] = {
    'claimed_stale_threshold_ms': ('CLAIMED_STALE_THRESHOLD_MS', 1_000),
    'running_stale_threshold_ms': ('STALE_THRESHOLD_MS', 1_000),
    'finalizing_stale_threshold_ms': ('STALE_THRESHOLD_MS', 1_000),
    'crashed_worker_recovery_grace_ms': ('RECOVERY_GRACE_MS', 0),
    'phase2_quarantine_after_attempts': ('RECOVERY_PASS_LIMIT', 3),
    'workflow_recovery_failure_limit': ('RECOVERY_PASS_LIMIT', 3),
    'check_interval_ms': ('CHECK_INTERVAL_MS', 1_000),
    'orphan_task_audit_interval_ms': ('AUDIT_INTERVAL_MS', 1_000),
    'runner_heartbeat_interval_ms': ('HEARTBEAT_INTERVAL_MS', 1_000),
    'claimer_heartbeat_interval_ms': ('HEARTBEAT_INTERVAL_MS', 1_000),
}

RECOVERY_FLAGS: Final[frozenset[str]] = frozenset(
    {
        'auto_requeue_stale_claimed',
        'auto_terminate_orphaned_workflow_tasks',
        'auto_fail_stale_running',
    },
)

# RetentionConfig field -> the RetentionManifest field it fills.
RETENTION_FIELDS: Final[dict[str, str]] = {
    'worker_state_retention_hours': 'worker_state_retention',
    'terminal_record_retention_hours': 'terminal_record_retention',
    'paused_workflow_auto_cancel_after': 'paused_workflow_auto_cancel_after',
    'history_leaf_horizon_days': 'history_leaf_horizon_days',
    'heartbeat_leaf_horizon_hours': 'heartbeat_leaf_horizon_hours',
    'retention_periods': 'retention_periods',
    'queue_retention': 'queue_retention',
    'partition_maintenance_interval_s': 'partition_maintenance_interval_s',
    'retention_sweep_interval_s': 'retention_sweep_interval_s',
    'retention_delete_batch_size': 'retention_delete_batch_size',
}

# RetentionConfig field with a Pydantic bound -> its manifest bound.
RETENTION_BOUNDS: Final[dict[str, str]] = {
    'worker_state_retention_hours': 'WORKER_STATE_RETENTION_HOURS',
    'terminal_record_retention_hours': 'TERMINAL_RECORD_RETENTION_HOURS',
    'history_leaf_horizon_days': 'HISTORY_LEAF_HORIZON_DAYS',
    'heartbeat_leaf_horizon_hours': 'HEARTBEAT_LEAF_HORIZON_HOURS',
    'partition_maintenance_interval_s': 'MAINTENANCE_INTERVAL_SECONDS',
    'retention_sweep_interval_s': 'SWEEP_INTERVAL_SECONDS',
    'retention_delete_batch_size': 'DELETE_BATCH_SIZE',
}

# WorkerResilienceConfig field -> its manifest bound.
RESILIENCE_BOUNDS: Final[dict[str, str]] = {
    'db_retry_initial_ms': 'RETRY_INITIAL_MS',
    'db_retry_max_ms': 'RETRY_MAX_MS',
    'notify_poll_interval_ms': 'POLL_INTERVAL_MS',
}

FLAGS: Final = ('-O', '-X', 'dev')


def _pydantic_bounds(model: Any, field: str) -> tuple[list[object], list[object]]:
    """The `ge` and `le` values of a field's metadata."""
    metadata = model.model_fields[field].metadata
    lower: list[object] = [item.ge for item in metadata if isinstance(item, Ge)]
    upper: list[object] = [item.le for item in metadata if isinstance(item, Le)]
    return lower, upper


def _manifest_bounds(name: str) -> tuple[int, int]:
    return getattr(wire, f'{name}_MIN'), getattr(wire, f'{name}_MAX')


def _interpreter_state(*, isolated: int) -> SimpleNamespace:
    """A `sys.flags` stand-in with every flag that a child takes set."""
    return SimpleNamespace(
        optimize=2,
        dont_write_bytecode=1,
        isolated=isolated,
        ignore_environment=1,
        no_user_site=1,
        safe_path=True,
        no_site=1,
        dev_mode=True,
    )


CURRENT_DIRECTORY: Final = CurrentDirectory('/srv/cwd')


def _launch() -> wire.ChildLaunchManifest:
    return python_child_launch(
        executable='/usr/bin/python3',
        interpreter_flags=FLAGS,
        app_locator='app.main:app',
        imports=['app.tasks'],
        sys_path_roots=['/srv/app'],
        parent_sys_path=child_sys_path(
            ['/srv/app', '/usr/lib/python3.13'], CURRENT_DIRECTORY,
        ),
        parent_argv=child_argv(['horsies', 'worker', 'app.main:app']),
        log_level='INFO',
    ).unwrap()


def _full_profile() -> WorkerProfile:
    return WorkerProfile(
        isolated=IsolatedChildren(
            count=2,
            recycle=IsolatedRecycle(
                after=TaskLimit(tasks=50), above=MemoryLimit(mb=512)
            ),
            pool=IsolatedPool(min_size=1, max_size=PoolSize(connections=3), check=True),
            task_send_pool=PoolSize(connections=4),
        ),
        shared_async=SharedAsyncChildren(
            count=1,
            seats=20,
            recycle=SharedRecycle(after_per_seat=NoTaskLimit(), above=NoMemoryLimit()),
            pool=SharedPool(min_size=0, max_size=PoolSize(connections=5), check=False),
            executor=ExecutorThreads(threads=6),
            task_send_pool=PoolSize(connections=2),
        ),
        shared_thread=SharedThreadChildren(count=1, seats=8),
        pool=ParentPool(max_size=PoolSize(connections=9)),
        claim=ClaimLimits(
            per_queue_per_pass=TaskLimit(tasks=5), claimed_per_worker=NoTaskLimit()
        ),
        drain_timeout=timedelta(seconds=90),
        preload=ChildPreload.APP,
        parent_cpu=ParentCpu.OWN_CORE,
    )


class TestBounds:
    def test_the_profile_bounds_are_the_manifest_bounds(self) -> None:
        assert (wire.TASK_LIMIT_MIN, wire.TASK_LIMIT_MAX) == (1, MAX_TASK_LIMIT)
        assert (wire.MEMORY_LIMIT_MB_MIN, wire.MEMORY_LIMIT_MB_MAX) == (
            1,
            MAX_MEMORY_LIMIT_MB,
        )
        assert wire.ISOLATED_CHILDREN_MAX == MAX_ISOLATED_CHILDREN
        assert wire.SHARED_ASYNC_CHILDREN_MAX == MAX_SHARED_ASYNC_CHILDREN
        assert wire.SHARED_ASYNC_SEATS_MAX == MAX_SEATS_PER_SHARED_ASYNC_CHILD
        assert wire.SHARED_THREAD_CHILDREN_MAX == MAX_SHARED_THREAD_CHILDREN
        assert wire.SHARED_THREAD_SEATS_MAX == MAX_SEATS_PER_SHARED_THREAD_CHILD
        assert (wire.CHILD_POOL_CONNECTIONS_MIN, wire.CHILD_POOL_CONNECTIONS_MAX) == (
            1,
            MAX_CHILD_POOL_CONNECTIONS,
        )
        assert (wire.CHILD_POOL_MIN_SIZE_MIN, wire.CHILD_POOL_MIN_SIZE_MAX) == (
            0,
            MAX_CHILD_POOL_CONNECTIONS,
        )
        assert wire.DEFAULT_EXECUTOR_THREADS_MAX == MAX_EXECUTOR_THREADS
        assert wire.DRAIN_TIMEOUT_MS_MAX == MAX_DRAIN_TIMEOUT // timedelta(
            milliseconds=1
        )

    def test_the_queue_priority_bounds_are_the_config_bounds(self) -> None:
        metadata = CustomQueueConfig.model_fields['priority'].metadata
        lower = [item.ge for item in metadata if isinstance(item, Ge)]
        upper = [item.le for item in metadata if isinstance(item, Le)]
        assert (lower, upper) == ([wire.QUEUE_PRIORITY_MIN], [wire.QUEUE_PRIORITY_MAX])


class TestProfile:
    def test_every_profile_field_has_a_manifest_field(self) -> None:
        assert set(WorkerProfile.model_fields) == set(PROFILE_FIELDS)
        manifest_fields = {
            field.name for field in dataclasses.fields(wire.WorkerProfileManifest)
        }
        assert manifest_fields == set(PROFILE_FIELDS.values())

    def test_a_profile_with_every_setting_maps_field_by_field(self) -> None:
        assert worker_profile_manifest(_full_profile()) == wire.WorkerProfileManifest(
            isolated=wire.IsolatedChildren(
                count=2,
                recycle=wire.IsolatedRecycleManifest(
                    after=wire.TaskLimitTasks(tasks=50),
                    above=wire.MemoryLimitMegabytes(mb=512),
                ),
                pool=wire.IsolatedPoolManifest(min_size=1, max_size=3, check=True),
                task_send_pool=4,
            ),
            shared_async=wire.SharedAsyncChildren(
                count=1,
                seats=20,
                recycle=wire.SharedRecycleManifest(
                    after_per_seat=wire.TaskLimitNoLimit(),
                    above=wire.MemoryLimitNoLimit(),
                ),
                pool=wire.SharedPoolManifest(
                    min_size=0,
                    max_size=wire.PoolMaxSizeConnections(connections=5),
                    check=False,
                ),
                executor=wire.DefaultExecutorThreads(threads=6),
                task_send_pool=2,
            ),
            shared_thread=wire.SharedThreadChildren(
                count=1,
                seats=8,
                recycle=wire.SharedRecycleManifest(
                    after_per_seat=wire.TaskLimitTasks(
                        tasks=DEFAULT_RECYCLE_AFTER_TASKS_PER_SEAT
                    ),
                    above=wire.MemoryLimitNoLimit(),
                ),
                pool=wire.SharedPoolManifest(
                    min_size=0,
                    max_size=wire.PoolMaxSizeFromSeats(),
                    check=False,
                ),
                task_send_pool=DEFAULT_TASK_SEND_POOL_CONNECTIONS,
            ),
            pool=wire.ParentPoolManifest(
                max_size=wire.PoolMaxSizeConnections(connections=9)
            ),
            claim=wire.ClaimLimitsManifest(
                per_queue_per_pass=wire.TaskLimitTasks(tasks=5),
                claimed_per_worker=wire.TaskLimitNoLimit(),
            ),
            drain_timeout_ms=90_000,
            preload='APP',
            parent_cpu='OWN_CORE',
        )

    def test_each_default_of_the_model_is_a_value_of_the_manifest(self) -> None:
        manifest = worker_profile_manifest(
            WorkerProfile(isolated=IsolatedChildren(count=3))
        )
        assert manifest == wire.WorkerProfileManifest(
            isolated=wire.IsolatedChildren(
                count=3,
                recycle=wire.IsolatedRecycleManifest(
                    after=wire.TaskLimitTasks(tasks=DEFAULT_RECYCLE_AFTER_TASKS),
                    above=wire.MemoryLimitNoLimit(),
                ),
                pool=wire.IsolatedPoolManifest(
                    min_size=0,
                    max_size=DEFAULT_ISOLATED_POOL_CONNECTIONS,
                    check=False,
                ),
                task_send_pool=DEFAULT_TASK_SEND_POOL_CONNECTIONS,
            ),
            shared_async=wire.SharedAsyncNoChildren(),
            shared_thread=wire.SharedThreadNoChildren(),
            pool=wire.ParentPoolManifest(max_size=wire.PoolMaxSizeFromSeats()),
            claim=wire.ClaimLimitsManifest(
                per_queue_per_pass=wire.TaskLimitNoLimit(),
                claimed_per_worker=wire.TaskLimitNoLimit(),
            ),
            drain_timeout_ms=DEFAULT_DRAIN_TIMEOUT // timedelta(milliseconds=1),
            preload='LIBRARY',
            parent_cpu='SHARED',
        )

    def test_every_preload_and_parent_cpu_has_its_manifest_value(self) -> None:
        values = {
            preload: worker_profile_manifest(
                WorkerProfile(isolated=IsolatedChildren(count=1), preload=preload),
            ).preload
            for preload in ChildPreload
        }
        assert values == {
            ChildPreload.LIBRARY: 'LIBRARY',
            ChildPreload.APP: 'APP',
            ChildPreload.NONE: 'NO_PRELOAD',
        }
        cpus = {
            cpu: worker_profile_manifest(
                WorkerProfile(isolated=IsolatedChildren(count=1), parent_cpu=cpu),
            ).parent_cpu
            for cpu in ParentCpu
        }
        assert cpus == {ParentCpu.SHARED: 'SHARED', ParentCpu.OWN_CORE: 'OWN_CORE'}

    def test_a_derived_pool_size_and_executor_stay_derived(self) -> None:
        manifest = worker_profile_manifest(
            WorkerProfile(shared_async=SharedAsyncChildren(count=1, seats=4)),
        )
        match manifest.shared_async:
            case wire.SharedAsyncChildren(pool=pool, executor=executor):
                assert pool.max_size == wire.PoolMaxSizeFromSeats()
                assert executor == wire.DefaultExecutorFromSeats()
            case wire.SharedAsyncNoChildren():
                pytest.fail('the profile has shared async children')


class TestDrainTimeout:
    @pytest.mark.parametrize(
        ('drain_timeout', 'milliseconds'),
        [
            (timedelta(microseconds=1), 1),
            (timedelta(microseconds=1_500), 2),
            (timedelta(milliseconds=1), 1),
            (timedelta(seconds=300), 300_000),
            (MAX_DRAIN_TIMEOUT, wire.DRAIN_TIMEOUT_MS_MAX),
            (MAX_DRAIN_TIMEOUT - timedelta(microseconds=1), wire.DRAIN_TIMEOUT_MS_MAX),
        ],
    )
    def test_a_drain_timeout_is_rounded_up_to_whole_milliseconds(
        self,
        drain_timeout: timedelta,
        milliseconds: int,
    ) -> None:
        assert drain_timeout_ms(drain_timeout) == milliseconds

    def test_a_drain_timeout_outside_the_model_bounds_is_a_defect(self) -> None:
        with pytest.raises(AssertionError, match='a validated drain timeout'):
            drain_timeout_ms(timedelta(0))
        with pytest.raises(AssertionError, match='a validated drain timeout'):
            drain_timeout_ms(MAX_DRAIN_TIMEOUT + timedelta(milliseconds=1))


class TestClaim:
    def test_the_default_queue_mode_has_no_queue_list(self) -> None:
        assert worker_claim_manifest(_app_config()) == wire.WorkerClaimManifest(
            queues=wire.QueuesDefault(),
            prefetch_buffer=0,
            claim_lease=wire.ClaimLeaseWorkerDefault(),
            max_claim_renew_age_ms=180_000,
            cluster_wide_cap=wire.ClusterWideCapNoLimit(),
        )

    def test_custom_queues_keep_their_order_priority_and_limit(self) -> None:
        config = _app_config(
            queue_mode=QueueMode.CUSTOM,
            custom_queues=[
                CustomQueueConfig(name='high', priority=1, max_concurrency=0),
                CustomQueueConfig(name='low', priority=100, max_concurrency=None),
            ],
            claim_lease_ms=90_000,
            cluster_wide_cap=20,
        )
        assert worker_claim_manifest(config) == wire.WorkerClaimManifest(
            queues=wire.QueuesCustom(
                queues=(
                    wire.CustomQueueManifest(
                        name='high',
                        priority=1,
                        max_concurrency=wire.QueueConcurrencyTasks(tasks=0),
                    ),
                    wire.CustomQueueManifest(
                        name='low',
                        priority=100,
                        max_concurrency=wire.QueueConcurrencyNoLimit(),
                    ),
                ),
            ),
            prefetch_buffer=0,
            claim_lease=wire.ClaimLeaseMilliseconds(ms=90_000),
            max_claim_renew_age_ms=180_000,
            cluster_wide_cap=wire.ClusterWideCapTasks(tasks=20),
        )

    @pytest.mark.parametrize(
        ('overrides', 'path', 'maximum'),
        [
            (
                {'prefetch_buffer': wire.CLAIM_COUNT_MAX + 1},
                'prefetch_buffer',
                wire.CLAIM_COUNT_MAX,
            ),
            (
                {'max_claim_renew_age_ms': wire.MILLISECONDS_MAX + 1},
                'max_claim_renew_age_ms',
                wire.MILLISECONDS_MAX,
            ),
            (
                {'claim_lease_ms': wire.MILLISECONDS_MAX + 1},
                'claim_lease_ms',
                wire.MILLISECONDS_MAX,
            ),
            (
                {'cluster_wide_cap': wire.TASK_LIMIT_MAX + 1},
                'cluster_wide_cap',
                wire.TASK_LIMIT_MAX,
            ),
            (
                {
                    'queue_mode': QueueMode.CUSTOM,
                    'custom_queues': [
                        CustomQueueConfig(name='a', max_concurrency=1),
                        CustomQueueConfig(
                            name='b', max_concurrency=wire.QUEUE_CONCURRENCY_MAX + 1
                        ),
                    ],
                },
                'custom_queues[1].max_concurrency',
                wire.QUEUE_CONCURRENCY_MAX,
            ),
        ],
    )
    def test_a_value_above_its_manifest_bound_is_a_defect_that_names_it(
        self,
        overrides: dict[str, Any],
        path: str,
        maximum: int,
    ) -> None:
        # AppConfig validation refuses such a value (test_app_config.py,
        # TestWorkerManifestBounds); model_copy skips validation, so the value
        # reaches the manifest only through a defect.
        config = _app_config().model_copy(update=overrides)
        with pytest.raises(AssertionError) as defect:
            worker_claim_manifest(config)
        assert str(defect.value).startswith(
            f'{path} = {maximum + 1} is above the bound that a worker takes ({maximum})'
        )

    def test_the_largest_values_that_fit_are_kept(self) -> None:
        # cluster_wide_cap needs the hard cap mode (prefetch_buffer=0).
        soft_cap = _app_config(
            prefetch_buffer=wire.CLAIM_COUNT_MAX,
            claim_lease_ms=wire.MILLISECONDS_MAX,
            max_claim_renew_age_ms=wire.MILLISECONDS_MAX,
        )
        claim = worker_claim_manifest(soft_cap)
        assert claim.prefetch_buffer == wire.CLAIM_COUNT_MAX
        assert claim.claim_lease == wire.ClaimLeaseMilliseconds(ms=wire.MILLISECONDS_MAX)
        assert claim.max_claim_renew_age_ms == wire.MILLISECONDS_MAX
        capped = worker_claim_manifest(_app_config(cluster_wide_cap=wire.TASK_LIMIT_MAX))
        assert capped.cluster_wide_cap == wire.ClusterWideCapTasks(
            tasks=wire.TASK_LIMIT_MAX
        )


class TestDatabase:
    def test_the_worker_database_keeps_both_pre_ping_settings(self) -> None:
        config = PostgresConfig(
            database_url=SecretStr(MAIN_URL),
            session_database_url=SecretStr(SESSION_URL),
            pgbouncer_transaction_mode=True,
            pool_pre_ping=False,
            worker_pool_pre_ping=True,
            pool_timeout=11,
            pool_recycle=-1,
            echo=True,
        )
        assert worker_database_manifest(config) == wire.WorkerDatabaseManifest(
            database_url='postgresql://user:secret@pooler:6432/app',
            session_database=wire.SessionDatabaseSeparate(
                url='postgresql://user:secret@direct:5432/app',
            ),
            pgbouncer_transaction_mode=True,
            pool_pre_ping=False,
            worker_pool_pre_ping=True,
            pool_timeout=11,
            pool_recycle=wire.PoolRecycleNever(),
            echo=True,
            tcp_keepalives=wire.TcpKeepalivesConfigOn(
                idle_seconds=30,
                interval_seconds=10,
                probes=3,
            ),
            tcp_user_timeout=wire.TcpUserTimeoutConfigAfter(milliseconds=30_000),
        )

    def test_the_pool_size_and_overflow_of_the_app_do_not_reach_a_worker(self) -> None:
        names = {
            field.name for field in dataclasses.fields(wire.WorkerDatabaseManifest)
        }
        assert 'pool_size' not in names
        assert 'max_overflow' not in names

    def test_every_broker_field_is_mapped_or_named_as_outside(self) -> None:
        assert set(BROKER_FIELDS).isdisjoint(OUTSIDE_WORKER_MANIFEST)
        assert set(PostgresConfig.model_fields) == (
            set(BROKER_FIELDS) | OUTSIDE_WORKER_MANIFEST
        )

    def test_every_database_field_has_a_broker_field_as_its_source(self) -> None:
        names = {
            field.name for field in dataclasses.fields(wire.WorkerDatabaseManifest)
        }
        assert {f'database.{name}' for name in names} <= set(BROKER_FIELDS.values())

    def test_keepalives_on_carry_the_three_values(self) -> None:
        config = PostgresConfig(
            database_url=SecretStr(MAIN_URL),
            tcp_keepalives=True,
            tcp_keepalives_idle=45,
            tcp_keepalives_interval=7,
            tcp_keepalives_count=5,
        )
        assert worker_database_manifest(config).tcp_keepalives == (
            wire.TcpKeepalivesConfigOn(idle_seconds=45, interval_seconds=7, probes=5)
        )

    def test_keepalives_off_is_the_off_variant_whatever_the_three_values(self) -> None:
        config = PostgresConfig.model_validate(
            {
                'database_url': MAIN_URL,
                'tcp_keepalives': False,
                'tcp_keepalives_idle': 0,
                'tcp_keepalives_interval': 0,
                'tcp_keepalives_count': 0,
            },
        )
        assert worker_database_manifest(config).tcp_keepalives == (
            wire.TcpKeepalivesConfigOff()
        )

    def test_a_tcp_user_timeout_of_zero_is_the_system_default(self) -> None:
        config = PostgresConfig(database_url=SecretStr(MAIN_URL), tcp_user_timeout_ms=0)
        assert worker_database_manifest(config).tcp_user_timeout == (
            wire.TcpUserTimeoutConfigSystemDefault()
        )

    @pytest.mark.parametrize('milliseconds', [1, 30_000])
    def test_a_tcp_user_timeout_is_a_number_of_milliseconds(
        self,
        milliseconds: int,
    ) -> None:
        config = PostgresConfig(
            database_url=SecretStr(MAIN_URL),
            tcp_user_timeout_ms=milliseconds,
        )
        assert worker_database_manifest(config).tcp_user_timeout == (
            wire.TcpUserTimeoutConfigAfter(milliseconds=milliseconds)
        )

    def test_a_keepalive_value_that_the_validator_refuses_is_a_defect_here(
        self,
    ) -> None:
        with pytest.raises(ConfigurationError):
            PostgresConfig.model_validate(
                {
                    'database_url': MAIN_URL,
                    'tcp_keepalives': True,
                    'tcp_keepalives_count': 0,
                },
            )
        unvalidated = PostgresConfig.model_construct(
            database_url=SecretStr(MAIN_URL),
            tcp_keepalives=True,
            tcp_keepalives_idle=30,
            tcp_keepalives_interval=10,
            tcp_keepalives_count=0,
        )
        with pytest.raises(AssertionError, match='keepalive values'):
            tcp_keepalives_of(unvalidated)

    def test_a_tcp_user_timeout_that_the_validator_refuses_is_a_defect_here(
        self,
    ) -> None:
        with pytest.raises(ConfigurationError):
            PostgresConfig.model_validate(
                {'database_url': MAIN_URL, 'tcp_user_timeout_ms': -1},
            )
        with pytest.raises(AssertionError, match='tcp_user_timeout_ms=-1'):
            tcp_user_timeout_of(-1)


class TestValues:
    def test_the_profile_request_is_the_profile_option(self) -> None:
        assert profile_request(None) == wire.ProfileRequestNotGiven()
        assert profile_request('batch') == wire.ProfileRequestNamed(name='batch')

    def test_every_execution_class_has_its_manifest_value(self) -> None:
        assert {execution: execution_of(execution) for execution in Execution} == {
            Execution.ISOLATED: 'ISOLATED',
            Execution.SHARED_ASYNC: 'SHARED_ASYNC',
            Execution.SHARED_THREAD: 'SHARED_THREAD',
        }

    def test_every_log_level_choice_has_its_manifest_value(self) -> None:
        for level in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
            assert log_level_of(level) == level
            assert log_level_of(level.lower()) == level
        with pytest.raises(AssertionError, match='five levels'):
            log_level_of('TRACE')

    def test_a_python_child_runs_the_child_module(self) -> None:
        launch = _launch()
        assert launch.command == wire.ChildCommandManifest(
            program='/usr/bin/python3',
            args=(*FLAGS, '-m', CHILD_MAIN_MODULE),
        )
        assert launch.program == wire.ChildProgramPython(
            app_locator='app.main:app',
            imports=('app.tasks',),
            sys_path_roots=('/srv/app',),
            parent_sys_path=('/srv/app', '/usr/lib/python3.13'),
            parent_argv=('horsies', 'worker', 'app.main:app'),
        )

    def test_the_preload_process_runs_the_preload_module_with_the_flags(
        self,
    ) -> None:
        assert _launch().preload == wire.PreloadLaunchCommand(
            command=wire.ChildCommandManifest(
                program='/usr/bin/python3',
                args=(*FLAGS, '-m', PRELOAD_MAIN_MODULE),
            ),
        )

    def test_every_interpreter_flag_is_rebuilt_from_the_process_state(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(sys, 'flags', _interpreter_state(isolated=0))
        monkeypatch.setattr(sys, 'warnoptions', ['ignore', 'error::DeprecationWarning'])
        assert current_interpreter_flags() == (
            '-OO',
            '-B',
            '-E',
            '-s',
            '-P',
            '-S',
            '-X',
            'dev',
            '-W',
            'ignore',
            '-W',
            'error::DeprecationWarning',
        )

    def test_isolated_mode_stands_for_its_three_flags(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(sys, 'flags', _interpreter_state(isolated=1))
        monkeypatch.setattr(sys, 'warnoptions', [])
        assert current_interpreter_flags() == ('-OO', '-B', '-I', '-S', '-X', 'dev')

    def test_a_plain_interpreter_has_no_flags(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            sys,
            'flags',
            SimpleNamespace(
                optimize=0,
                dont_write_bytecode=0,
                isolated=0,
                ignore_environment=0,
                no_user_site=0,
                safe_path=False,
                no_site=0,
                dev_mode=False,
            ),
        )
        monkeypatch.setattr(sys, 'warnoptions', [])
        assert current_interpreter_flags() == ()


class _NoTextPath:
    """An `os.PathLike` whose `__fspath__` gives neither `str` nor `bytes`."""

    def __fspath__(self) -> str:
        return cast('str', 7)

    def __repr__(self) -> str:
        return '_NoTextPath()'


def _child_launch(
    sys_path: list[object],
    argv: list[object],
    app_locator: str = 'app.main:app',
) -> wire.ChildLaunchManifest:
    return python_child_launch(
        executable='/usr/bin/python3',
        interpreter_flags=(),
        app_locator=app_locator,
        imports=(),
        sys_path_roots=(),
        parent_sys_path=child_sys_path(sys_path, CURRENT_DIRECTORY),
        parent_argv=child_argv(argv),
        log_level='INFO',
    ).unwrap()


class TestChildSysPath:
    """`child_sys_path`: the `sys.path` of the worker as each child restores
    it (Python 0.7.0 `_frame_sys_path`)."""

    def test_text_entries_are_kept_in_their_order(self) -> None:
        restored = child_sys_path(['/srv/app', '/usr/lib'], CURRENT_DIRECTORY)
        assert restored.entries == ('/srv/app', '/usr/lib')
        assert restored.left_out == ()

    def test_the_empty_entry_is_the_current_directory(self) -> None:
        restored = child_sys_path(['', '/srv/app'], CURRENT_DIRECTORY)
        assert restored.entries == ('/srv/cwd', '/srv/app')

    def test_the_empty_entry_is_left_out_when_the_current_directory_is_unreadable(
        self,
    ) -> None:
        unreadable = CurrentDirectoryUnreadable('FileNotFoundError: gone')
        restored = child_sys_path(['', '/srv/app'], unreadable)
        assert restored.entries == ('/srv/app',)
        assert restored.left_out == (
            LeftOutEntry(
                "''",
                'the current directory cannot be read: FileNotFoundError: gone',
            ),
        )

    def test_a_path_object_and_bytes_become_their_file_system_text(self) -> None:
        restored = child_sys_path(
            [Path('/srv/app'), b'/srv/lib', Path('')], CURRENT_DIRECTORY,
        )
        assert restored.entries == ('/srv/app', '/srv/lib', '.')
        assert restored.left_out == ()

    def test_an_object_that_is_not_a_path_is_left_out(self) -> None:
        restored = child_sys_path(['/srv/app', 7, None, bytearray(b'/x')], CURRENT_DIRECTORY)
        assert restored.entries == ('/srv/app',)
        assert [left.entry for left in restored.left_out] == [
            '7',
            'None',
            "bytearray(b'/x')",
        ]
        assert {left.reason for left in restored.left_out} == {
            'it is not a str, bytes or os.PathLike path',
        }

    def test_a_path_like_whose_fspath_gives_no_text_is_left_out(self) -> None:
        restored = child_sys_path([_NoTextPath(), '/srv/app'], CURRENT_DIRECTORY)
        assert restored.entries == ('/srv/app',)
        assert restored.left_out == (
            LeftOutEntry('_NoTextPath()', 'it is not a str, bytes or os.PathLike path'),
        )

    def test_text_that_is_not_utf8_is_left_out(self) -> None:
        undecodable = os.fsdecode(b'/srv/\xff')
        restored = child_sys_path(
            ['bad\udcff', b'/srv/\xff', Path(undecodable), '/srv/app'],
            CURRENT_DIRECTORY,
        )
        assert restored.entries == ('/srv/app',)
        assert [left.reason for left in restored.left_out] == [
            'it is not valid UTF-8',
        ] * 3

    def test_a_current_directory_that_is_not_utf8_is_left_out(self) -> None:
        restored = child_sys_path(['', '/srv/app'], CurrentDirectory('/srv/\udcff'))
        assert restored.entries == ('/srv/app',)
        assert restored.left_out == (LeftOutEntry("''", 'it is not valid UTF-8'),)


class TestChildArgv:
    """`child_argv`: the `sys.argv` of the worker as each child restores it
    (Python 0.7.0 `_frame_text`)."""

    def test_utf8_items_are_kept_in_their_order(self) -> None:
        assert child_argv(['horsies', 'worker', 'app.main:app', 'ünï']) == ChildArgv(
            ('horsies', 'worker', 'app.main:app', 'ünï'),
        )

    def test_a_code_point_that_is_not_utf8_is_written_as_its_escape(self) -> None:
        assert child_argv(['horsies', 'bad\udcff']) == ChildArgv(
            ('horsies', 'bad\\udcff'),
        )

    def test_an_item_that_is_not_text_is_its_str(self) -> None:
        assert child_argv(['horsies', Path('/srv/app.py'), 3]) == ChildArgv(
            ('horsies', '/srv/app.py', '3'),
        )


class TestMainModuleLocator:
    """`python_child_launch` refuses an app locator in `__main__`: a child
    is a new program with its own `__main__`."""

    def test_a_locator_in_main_is_refused(self) -> None:
        refused = python_child_launch(
            executable='/usr/bin/python3',
            interpreter_flags=(),
            app_locator='__main__:app',
            imports=(),
            sys_path_roots=(),
            parent_sys_path=child_sys_path([], CURRENT_DIRECTORY),
            parent_argv=child_argv([]),
            log_level='INFO',
        )
        assert refused.unwrap_err() == MainModuleLocator('__main__:app')

    @pytest.mark.parametrize(
        'locator',
        ['app.__main__:app', '/srv/__main__.py:app', '__main__app:app'],
    )
    def test_a_module_or_file_named_after_main_is_accepted(self, locator: str) -> None:
        launch = _child_launch([], [], app_locator=locator)
        assert isinstance(launch.program, wire.ChildProgramPython)
        assert launch.program.app_locator == locator


def _document_launch(launch: wire.ChildLaunchManifest) -> dict[str, Any]:
    """The child launch of the dumped document of a worker with `launch`."""
    manifest = to_worker_manifest(
        _app_config(worker_profiles={'batch': _full_profile()}),
        profile='batch',
        metrics_endpoint=wire.MetricsEndpointNoMetricsEndpoint(),
        tasks={},
        child_launch=launch,
        idempotency_reservation_window=WINDOW,
    )
    document = cast('dict[str, Any]', json.loads(wire.dump_worker_manifest(manifest)))
    return cast('dict[str, Any]', document['child_launch'])


def _utf8_text(values: list[Any]) -> bool:
    """Whether every value is text that UTF-8 can encode, as the parser of
    the core takes it (`serde_json` refuses a lone surrogate)."""
    return all(
        isinstance(value, str) and not any(0xD800 <= ord(char) < 0xE000 for char in value)
        for value in values
    )


class TestChildLaunchDocument:
    """Regressions of #1950: the dumped document of each case is one that
    the core takes. Before the fix, a `Path` on `sys.path` stopped the dump,
    a number reached the document where the core reads text, and a lone
    surrogate reached it as an escape that `serde_json` refuses."""

    def test_a_path_object_on_sys_path_reaches_the_document_as_text(self) -> None:
        launch = _child_launch([Path('/srv/app'), '/srv/lib'], ['horsies'])
        program = _document_launch(launch)['program']
        assert program['parent_sys_path'] == ['/srv/app', '/srv/lib']

    def test_an_entry_that_is_not_a_path_does_not_reach_the_document(self) -> None:
        launch = _child_launch([7, '/srv/lib'], ['horsies'])
        program = _document_launch(launch)['program']
        assert program['parent_sys_path'] == ['/srv/lib']
        assert _utf8_text(program['parent_sys_path'])

    def test_text_that_is_not_utf8_reaches_the_document_as_utf8_only(self) -> None:
        launch = _child_launch(['bad\udcff', '/srv/lib'], ['horsies', 'bad\udcff'])
        program = _document_launch(launch)['program']
        assert program['parent_sys_path'] == ['/srv/lib']
        assert program['parent_argv'] == ['horsies', 'bad\\udcff']
        assert _utf8_text(program['parent_sys_path'])
        assert _utf8_text(program['parent_argv'])


class TestRecovery:
    def test_every_field_is_a_field_of_the_section(self) -> None:
        fields = {field.name for field in dataclasses.fields(wire.RecoveryManifest)}
        assert set(RecoveryConfig.model_fields) == fields
        assert fields == set(RECOVERY_BOUNDS) | RECOVERY_FLAGS | {
            'worker_state_snapshot_interval_ms',
        }

    def test_the_bounds_are_the_pydantic_bounds(self) -> None:
        for field, (bound, low) in RECOVERY_BOUNDS.items():
            lower, upper = _pydantic_bounds(RecoveryConfig, field)
            assert lower == [low], field
            assert _manifest_bounds(bound) == (lower[0], upper[0]), field

    def test_the_snapshot_interval_bound_is_half_the_live_window(self) -> None:
        lower, _ = _pydantic_bounds(RecoveryConfig, 'worker_state_snapshot_interval_ms')
        assert _manifest_bounds('SNAPSHOT_INTERVAL_MS') == (
            lower[0],
            WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS,
        )

    def test_each_default_is_a_value_of_the_section(self) -> None:
        config = RecoveryConfig()
        section = recovery_manifest(config)
        for field in RecoveryConfig.model_fields:
            assert getattr(section, field) == getattr(config, field), field

    def test_a_value_of_its_own_in_each_field_is_kept(self) -> None:
        config = RecoveryConfig(
            auto_requeue_stale_claimed=False,
            auto_fail_stale_running=False,
            auto_terminate_orphaned_workflow_tasks=False,
            claimed_stale_threshold_ms=3_600_000,
            running_stale_threshold_ms=7_200_000,
            finalizing_stale_threshold_ms=2_000,
            crashed_worker_recovery_grace_ms=0,
            phase2_quarantine_after_attempts=1_000,
            workflow_recovery_failure_limit=3,
            check_interval_ms=600_000,
            orphan_task_audit_interval_ms=86_400_000,
            runner_heartbeat_interval_ms=1_000,
            claimer_heartbeat_interval_ms=120_000,
            worker_state_snapshot_interval_ms=1_000,
        )
        section = recovery_manifest(config)
        for field in RecoveryConfig.model_fields:
            assert getattr(section, field) == getattr(config, field), field


class TestRetention:
    def test_every_field_is_a_field_of_the_section(self) -> None:
        assert set(RetentionConfig.model_fields) == set(RETENTION_FIELDS)
        fields = {field.name for field in dataclasses.fields(wire.RetentionManifest)}
        assert fields == set(RETENTION_FIELDS.values())

    def test_the_bounds_are_the_pydantic_bounds(self) -> None:
        for field, bound in RETENTION_BOUNDS.items():
            lower, upper = _pydantic_bounds(RetentionConfig, field)
            assert _manifest_bounds(bound) == (lower[0], upper[0]), field

    def test_a_period_is_whole_days_in_the_period_bounds(self) -> None:
        assert _manifest_bounds('RETENTION_DAYS') == (1, 1_825)

    def test_the_defaults_turn_the_paused_expiry_off_and_keep_records_hours(
        self,
    ) -> None:
        assert retention_manifest(RetentionConfig()) == wire.RetentionManifest(
            worker_state_retention=wire.WorkerStateRetentionHours(hours=24 * 7),
            terminal_record_retention=wire.TerminalRecordRetentionHours(
                hours=24 * 30,
            ),
            paused_workflow_auto_cancel_after=wire.PausedWorkflowExpiryNoExpiry(),
            history_leaf_horizon_days=3,
            heartbeat_leaf_horizon_hours=6,
            retention_periods=(),
            queue_retention={},
            partition_maintenance_interval_s=900,
            retention_sweep_interval_s=300,
            retention_delete_batch_size=500,
        )

    def test_none_is_no_pruning_and_a_period_is_its_days(self) -> None:
        config = RetentionConfig(
            worker_state_retention_hours=None,
            terminal_record_retention_hours=None,
            paused_workflow_auto_cancel_after=timedelta(days=2, microseconds=7),
            retention_periods=(timedelta(days=7), timedelta(days=90)),
            queue_retention={'high': timedelta(days=14), 'low': None},
        )
        section = retention_manifest(config)
        assert section.worker_state_retention == wire.WorkerStateRetentionNoPruning()
        assert section.terminal_record_retention == (
            wire.TerminalRecordRetentionNoPruning()
        )
        assert section.paused_workflow_auto_cancel_after == (
            wire.PausedWorkflowExpiryAfter(micros=2 * 86_400_000_000 + 7)
        )
        assert section.retention_periods == (7, 90)
        assert section.queue_retention == {
            'high': wire.QueueRetentionDays(days=14),
            'low': wire.QueueRetentionForever(),
        }

    def test_the_paused_age_bound_is_the_manifest_bound(self) -> None:
        """Pydantic refuses an age above the bound, so the longest age it
        accepts is the longest age the manifest holds."""
        assert PAUSED_WORKFLOW_AGE_MAX == timedelta(days=365_000)
        assert PAUSED_WORKFLOW_AGE_MAX // timedelta(microseconds=1) == (
            wire.PAUSED_AGE_MICROS_MAX
        )
        config = RetentionConfig(
            paused_workflow_auto_cancel_after=PAUSED_WORKFLOW_AGE_MAX,
        )
        assert retention_manifest(config).paused_workflow_auto_cancel_after == (
            wire.PausedWorkflowExpiryAfter(micros=wire.PAUSED_AGE_MICROS_MAX)
        )

    def test_a_paused_age_that_validation_refuses_is_a_defect_here(self) -> None:
        unvalidated = RetentionConfig.model_construct(
            paused_workflow_auto_cancel_after=timedelta.max,
        )
        with pytest.raises(AssertionError, match='validated paused workflow age'):
            retention_manifest(unvalidated)

    def test_a_period_that_validation_refuses_is_a_defect_here(self) -> None:
        unvalidated = RetentionConfig.model_construct(
            retention_periods=(timedelta(hours=36),),
        )
        with pytest.raises(AssertionError, match='whole days in bounds'):
            retention_manifest(unvalidated)


class TestResilience:
    def test_every_field_is_a_field_of_the_section(self) -> None:
        fields = {field.name for field in dataclasses.fields(wire.ResilienceManifest)}
        assert set(WorkerResilienceConfig.model_fields) == fields

    def test_the_bounds_are_the_pydantic_bounds(self) -> None:
        for field, bound in RESILIENCE_BOUNDS.items():
            lower, upper = _pydantic_bounds(WorkerResilienceConfig, field)
            assert _manifest_bounds(bound) == (lower[0], upper[0]), field
        lower, upper = _pydantic_bounds(WorkerResilienceConfig, 'db_retry_max_attempts')
        assert (lower, upper) == ([0], [wire.RETRY_ATTEMPTS_MAX])
        assert wire.RETRY_ATTEMPTS_MIN == 1

    def test_zero_attempts_is_no_limit(self) -> None:
        assert resilience_manifest(WorkerResilienceConfig()) == wire.ResilienceManifest(
            db_retry_initial_ms=500,
            db_retry_max_ms=30_000,
            db_retry_max_attempts=wire.RetryAttemptLimitNoLimit(),
            notify_poll_interval_ms=5_000,
        )
        limited = WorkerResilienceConfig(db_retry_max_attempts=10_000)
        assert resilience_manifest(limited).db_retry_max_attempts == (
            wire.RetryAttemptLimitAttempts(attempts=10_000)
        )


class TestPayload:
    def test_every_field_is_a_field_of_the_section(self) -> None:
        fields = {field.name for field in dataclasses.fields(wire.PayloadManifest)}
        assert set(PayloadPolicy.model_fields) == fields

    def test_the_bounds_are_the_pydantic_bounds(self) -> None:
        for field in PayloadPolicy.model_fields:
            lower, upper = _pydantic_bounds(PayloadPolicy, field)
            assert (lower, upper) == (
                [wire.PAYLOAD_BYTES_MIN],
                [wire.PAYLOAD_BYTES_MAX],
            ), field
        assert wire.PAYLOAD_BYTES_MAX == 2**64 - 1

    def test_none_is_no_limit(self) -> None:
        assert payload_manifest(PayloadPolicy()) == wire.PayloadManifest(
            warn_bytes=wire.PayloadLimitBytes(bytes=1_048_576),
            reject_bytes=wire.PayloadLimitNoLimit(),
        )
        config = PayloadPolicy(warn_bytes=None, reject_bytes=wire.PAYLOAD_BYTES_MAX)
        assert payload_manifest(config) == wire.PayloadManifest(
            warn_bytes=wire.PayloadLimitNoLimit(),
            reject_bytes=wire.PayloadLimitBytes(bytes=wire.PAYLOAD_BYTES_MAX),
        )

    @pytest.mark.parametrize('field', ['warn_bytes', 'reject_bytes'])
    def test_a_threshold_above_its_bound_is_refused_by_the_model(
        self, field: str,
    ) -> None:
        with pytest.raises(ValidationError) as refused:
            PayloadPolicy.model_validate({field: wire.PAYLOAD_BYTES_MAX + 1})
        [error] = refused.value.errors()
        assert error['loc'] == (field,)
        assert error['type'] == 'less_than_equal'

    def test_a_threshold_above_its_bound_is_a_defect_that_names_it(self) -> None:
        above = wire.PAYLOAD_BYTES_MAX + 1
        config = PayloadPolicy().model_copy(update={'reject_bytes': above})
        with pytest.raises(AssertionError) as defect:
            payload_manifest(config)
        assert str(defect.value).startswith(
            f'payload.reject_bytes = {above} is above the bound that a worker '
            f'takes ({wire.PAYLOAD_BYTES_MAX})'
        )


class TestDocument:
    def _document(
        self,
        metrics_endpoint: wire.MetricsEndpoint = wire.MetricsEndpointNoMetricsEndpoint(),
    ) -> dict[str, Any]:
        config = _app_config(worker_profiles={'batch': _full_profile()})
        manifest = to_worker_manifest(
            config,
            profile='batch',
            metrics_endpoint=metrics_endpoint,
            tasks={
                'app.tasks.add': TaskFacts(execution=Execution.ISOLATED, error_codes=()),
                'app.tasks.fetch': TaskFacts(
                    execution=Execution.SHARED_ASYNC,
                    error_codes=('RATE_LIMITED', 'NOT_FOUND'),
                ),
            },
            child_launch=_launch(),
            idempotency_reservation_window=WINDOW,
        )
        return cast('dict[str, Any]', json.loads(wire.dump_worker_manifest(manifest)))

    def test_the_document_holds_every_section(self) -> None:
        document = self._document()
        assert set(document) == {
            field.name for field in dataclasses.fields(wire.WorkerManifest)
        }
        assert document['manifest_version'] == wire.MANIFEST_VERSION
        assert document['profile'] == {'kind': 'NAMED', 'name': 'batch'}
        assert document['metrics_endpoint'] == {'kind': 'NO_METRICS_ENDPOINT'}
        assert document['tasks'] == {
            'app.tasks.add': {'execution': 'ISOLATED', 'error_codes': []},
            'app.tasks.fetch': {
                'execution': 'SHARED_ASYNC',
                'error_codes': ['RATE_LIMITED', 'NOT_FOUND'],
            },
        }
        assert document['send'] == {
            'retain_rerun_input_default': False,
            'idempotency_reservation_window': WINDOW // timedelta(microseconds=1),
        }

    def test_a_metrics_endpoint_is_written_with_its_host_and_port(self) -> None:
        endpoint = wire.MetricsEndpointListenAddress(host='::1', port=9100)
        document = self._document(metrics_endpoint=endpoint)
        assert document['metrics_endpoint'] == {
            'kind': 'LISTEN_ADDRESS',
            'host': '::1',
            'port': 9100,
        }

    def test_the_tcp_settings_are_written_in_the_form_the_core_parses(self) -> None:
        database = self._document()['database']
        assert database['tcp_keepalives'] == {
            'kind': 'ON',
            'idle_seconds': 30,
            'interval_seconds': 10,
            'probes': 3,
        }
        assert database['tcp_user_timeout'] == {'kind': 'AFTER', 'milliseconds': 30_000}

    def test_a_profile_is_written_with_its_kinds_and_milliseconds(self) -> None:
        profile = self._document()['profiles']['batch']
        assert profile['drain_timeout_ms'] == 90_000
        assert profile['isolated']['kind'] == 'CHILDREN'
        assert profile['isolated']['recycle']['above'] == {
            'kind': 'MEGABYTES',
            'mb': 512,
        }
        assert profile['shared_async']['executor'] == {'kind': 'THREADS', 'threads': 6}
        assert profile['pool'] == {
            'max_size': {'kind': 'CONNECTIONS', 'connections': 9}
        }
        assert profile['parent_cpu'] == 'OWN_CORE'

    def test_the_child_launch_is_written_as_lists(self) -> None:
        launch = self._document()['child_launch']
        assert launch['command'] == {
            'program': '/usr/bin/python3',
            'args': [*FLAGS, '-m', CHILD_MAIN_MODULE],
        }
        assert launch['preload'] == {
            'kind': 'COMMAND',
            'command': {
                'program': '/usr/bin/python3',
                'args': [*FLAGS, '-m', PRELOAD_MAIN_MODULE],
            },
        }
        assert launch['program']['kind'] == 'PYTHON'
        assert launch['program']['imports'] == ['app.tasks']
        assert launch['log_level'] == 'INFO'

    def test_the_sections_are_written_in_the_form_the_core_parses(self) -> None:
        document = self._document()
        assert document['recovery']['running_stale_threshold_ms'] == 300_000
        assert document['retention']['paused_workflow_auto_cancel_after'] == {
            'kind': 'NO_EXPIRY',
        }
        assert document['retention']['worker_state_retention'] == {
            'kind': 'HOURS',
            'hours': 24 * 7,
        }
        assert document['resilience']['db_retry_max_attempts'] == {'kind': 'NO_LIMIT'}
        assert document['payload'] == {
            'warn_bytes': {'kind': 'BYTES', 'bytes': 1_048_576},
            'reject_bytes': {'kind': 'NO_LIMIT'},
        }
