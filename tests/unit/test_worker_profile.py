"""Worker profiles: the bounds of each type, each named variant, and the
profile checks that ``AppConfig`` runs with the profile name and field path."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from pydantic import BaseModel, SecretStr, ValidationError

import horsies
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    MultipleValidationErrors,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.worker_profile import (
    DEFAULT_DRAIN_TIMEOUT,
    DEFAULT_RECYCLE_AFTER_TASKS,
    MAX_DRAIN_TIMEOUT,
    MAX_ISOLATED_CHILDREN,
    MAX_MEMORY_LIMIT_MB,
    MAX_TASK_LIMIT,
    PROFILE_NAME_MAX_LENGTH,
    ClaimLimits,
    IsolatedChildren,
    IsolatedRecycle,
    MemoryLimit,
    NoIsolatedChildren,
    NoMemoryLimit,
    NoTaskLimit,
    TaskLimit,
    WorkerProfile,
    describe_worker_profile,
)
from horsies.core.worker.config import ChildPreload

pytestmark = [pytest.mark.unit]


def _app_config(
    profiles: dict[str, WorkerProfile],
    **overrides: Any,
) -> AppConfig:
    return AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql+psycopg://user:pass@localhost/db'),
        ),
        worker_profiles=profiles,
        **overrides,
    )


def _errors_of(raised: HorsiesError) -> list[HorsiesError]:
    match raised:
        case MultipleValidationErrors(report=report):
            return list(report.errors)
        case _:
            return [raised]


def _isolated(count: int, **claim: TaskLimit | NoTaskLimit) -> WorkerProfile:
    return WorkerProfile(
        isolated=IsolatedChildren(count=count),
        claim=ClaimLimits(**claim),
    )


class TestTaskLimitBounds:
    @pytest.mark.parametrize('tasks', [0, -3])
    def test_below_one_is_refused(self, tasks: int) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 1'):
            TaskLimit(tasks=tasks)

    @pytest.mark.parametrize('tasks', [1, 2, 50, MAX_TASK_LIMIT])
    def test_one_to_max_is_accepted(self, tasks: int) -> None:
        assert TaskLimit(tasks=tasks).tasks == tasks

    def test_above_max_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            TaskLimit(tasks=MAX_TASK_LIMIT + 1)


class TestMemoryLimitBounds:
    @pytest.mark.parametrize('mb', [0, -1])
    def test_below_one_is_refused(self, mb: int) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 1'):
            MemoryLimit(mb=mb)

    @pytest.mark.parametrize('mb', [1, 1024, MAX_MEMORY_LIMIT_MB])
    def test_one_to_max_is_accepted(self, mb: int) -> None:
        assert MemoryLimit(mb=mb).mb == mb

    def test_above_max_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            MemoryLimit(mb=MAX_MEMORY_LIMIT_MB + 1)


class TestIsolatedChildrenBounds:
    @pytest.mark.parametrize('count', [0, -2])
    def test_below_one_is_refused(self, count: int) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 1'):
            IsolatedChildren(count=count)

    @pytest.mark.parametrize('count', [1, 4, MAX_ISOLATED_CHILDREN])
    def test_one_to_max_is_accepted(self, count: int) -> None:
        assert IsolatedChildren(count=count).count == count

    def test_above_max_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            IsolatedChildren(count=MAX_ISOLATED_CHILDREN + 1)

    def test_count_is_required(self) -> None:
        """There is no default number of children."""
        with pytest.raises(ValidationError, match='count'):
            IsolatedChildren()  # pyright: ignore[reportCallIssue]


class TestDrainTimeoutBounds:
    def test_seconds_are_accepted(self) -> None:
        profile = WorkerProfile(drain_timeout=timedelta(seconds=2.5))
        assert profile.drain_timeout == timedelta(seconds=2.5)

    @pytest.mark.parametrize(
        'value',
        [timedelta(0), timedelta(seconds=-1), float('nan'), float('inf')],
    )
    def test_a_value_with_no_deadline_is_refused(self, value: Any) -> None:
        with pytest.raises(ValidationError, match='drain_timeout'):
            WorkerProfile(drain_timeout=value)

    def test_max_is_accepted(self) -> None:
        assert WorkerProfile(drain_timeout=MAX_DRAIN_TIMEOUT).drain_timeout == (
            MAX_DRAIN_TIMEOUT
        )

    def test_above_max_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            WorkerProfile(drain_timeout=MAX_DRAIN_TIMEOUT + timedelta(seconds=1))


class TestDefaults:
    def test_recycle_defaults_to_after_100_tasks_and_no_memory_limit(self) -> None:
        recycle = IsolatedRecycle()
        assert recycle.after == TaskLimit(tasks=DEFAULT_RECYCLE_AFTER_TASKS)
        assert DEFAULT_RECYCLE_AFTER_TASKS == 100
        assert recycle.above == NoMemoryLimit()

    def test_claim_limits_default_to_no_limit(self) -> None:
        claim = ClaimLimits()
        assert claim.per_queue_per_pass == NoTaskLimit()
        assert claim.claimed_per_worker == NoTaskLimit()

    def test_profile_defaults(self) -> None:
        profile = WorkerProfile()
        assert profile.isolated == NoIsolatedChildren()
        assert profile.drain_timeout == DEFAULT_DRAIN_TIMEOUT
        assert DEFAULT_DRAIN_TIMEOUT == timedelta(seconds=300)
        assert profile.preload is ChildPreload.LIBRARY


class TestNamedVariants:
    """Each variant parses from its own ``kind``, so a dict config (for
    example from a file) reaches the same type as a Python constructor."""

    @pytest.mark.parametrize(
        ('data', 'expected'),
        [
            ({'kind': 'task_limit', 'tasks': 5}, TaskLimit(tasks=5)),
            ({'kind': 'no_task_limit'}, NoTaskLimit()),
        ],
    )
    def test_task_limit_variants(self, data: dict[str, Any], expected: Any) -> None:
        claim = ClaimLimits.model_validate({'claimed_per_worker': data})
        assert claim.claimed_per_worker == expected

    @pytest.mark.parametrize(
        ('data', 'expected'),
        [
            ({'kind': 'memory_limit', 'mb': 512}, MemoryLimit(mb=512)),
            ({'kind': 'no_memory_limit'}, NoMemoryLimit()),
        ],
    )
    def test_memory_limit_variants(
        self,
        data: dict[str, Any],
        expected: Any,
    ) -> None:
        recycle = IsolatedRecycle.model_validate({'above': data})
        assert recycle.above == expected

    @pytest.mark.parametrize(
        ('data', 'expected'),
        [
            (
                {'kind': 'isolated_children', 'count': 3},
                IsolatedChildren(count=3),
            ),
            ({'kind': 'no_isolated_children'}, NoIsolatedChildren()),
        ],
    )
    def test_isolated_variants(self, data: dict[str, Any], expected: Any) -> None:
        profile = WorkerProfile.model_validate({'isolated': data})
        assert profile.isolated == expected

    def test_a_dict_error_names_the_field_path(self) -> None:
        with pytest.raises(ValidationError) as raised:
            WorkerProfile.model_validate(
                {'isolated': {'kind': 'isolated_children', 'count': 0}},
            )
        assert raised.value.errors()[0]['loc'] == (
            'isolated',
            'isolated_children',
            'count',
        )

    @pytest.mark.parametrize(
        'model',
        [
            TaskLimit(tasks=1),
            NoTaskLimit(),
            MemoryLimit(mb=1),
            NoMemoryLimit(),
            IsolatedRecycle(),
            IsolatedChildren(count=1),
            NoIsolatedChildren(),
            ClaimLimits(),
            WorkerProfile(),
        ],
    )
    def test_every_profile_type_is_frozen(self, model: BaseModel) -> None:
        name = next(iter(type(model).model_fields))
        with pytest.raises(ValidationError, match='frozen'):
            setattr(model, name, getattr(model, name))

    def test_child_count_is_the_isolated_count(self) -> None:
        assert WorkerProfile(isolated=IsolatedChildren(count=3)).child_count() == 3

    def test_child_count_of_no_isolated_children_is_zero(self) -> None:
        assert WorkerProfile().child_count() == 0

    def test_public_names_are_exported(self) -> None:
        for name in (
            'WorkerProfile',
            'IsolatedChildren',
            'NoIsolatedChildren',
            'IsolatedRecycle',
            'TaskLimit',
            'NoTaskLimit',
            'MemoryLimit',
            'NoMemoryLimit',
            'ClaimLimits',
            'ChildPreload',
        ):
            assert name in horsies.__all__
            assert getattr(horsies, name) is not None


class TestAppConfigProfileChecks:
    def test_no_profiles_is_valid(self) -> None:
        """An app that runs no worker (a producer, the web UI) needs none."""
        assert _app_config({}).worker_profiles == {}

    def test_valid_profiles_are_kept(self) -> None:
        profiles = {'default': _isolated(4), 'batch': _isolated(1)}
        assert _app_config(profiles).worker_profiles == profiles

    def test_a_profile_with_no_children_names_the_profile(self) -> None:
        with pytest.raises(ConfigurationError) as raised:
            _app_config({'empty': WorkerProfile()})
        assert raised.value.code == ErrorCode.CONFIG_INVALID_WORKER_PROFILE
        assert raised.value.message == (
            "worker_profiles['empty']: the profile starts no children"
        )

    @pytest.mark.parametrize(
        'name',
        ['', 'has space', '-flag', '.hidden', 'a/b', 'x' * (PROFILE_NAME_MAX_LENGTH + 1)],
    )
    def test_an_invalid_name_is_refused(self, name: str) -> None:
        with pytest.raises(ConfigurationError) as raised:
            _app_config({name: _isolated(1)})
        assert raised.value.code == ErrorCode.CONFIG_INVALID_WORKER_PROFILE
        assert raised.value.message == (
            f'worker_profiles[{name!r}]: the profile name is not valid'
        )

    @pytest.mark.parametrize(
        'name',
        ['default', 'batch-high-memory', 'v1.2', '_internal', 'x' * PROFILE_NAME_MAX_LENGTH],
    )
    def test_a_valid_name_is_accepted(self, name: str) -> None:
        assert name in _app_config({name: _isolated(1)}).worker_profiles

    @pytest.mark.parametrize('field', ['per_queue_per_pass', 'claimed_per_worker'])
    def test_a_claim_limit_above_capacity_names_the_field_path(
        self,
        field: str,
    ) -> None:
        with pytest.raises(ConfigurationError) as raised:
            _app_config({'small': _isolated(2, **{field: TaskLimit(tasks=3)})})
        assert raised.value.code == ErrorCode.CONFIG_INVALID_WORKER_PROFILE
        assert raised.value.message == (
            f"worker_profiles['small'].claim.{field}: 3 tasks is above the "
            'capacity of the worker (2)'
        )

    @pytest.mark.parametrize('field', ['per_queue_per_pass', 'claimed_per_worker'])
    def test_a_claim_limit_equal_to_capacity_is_accepted(self, field: str) -> None:
        config = _app_config({'small': _isolated(2, **{field: TaskLimit(tasks=2)})})
        assert 'small' in config.worker_profiles

    def test_capacity_includes_the_prefetch_buffer(self) -> None:
        config = _app_config(
            {'soft': _isolated(2, claimed_per_worker=TaskLimit(tasks=5))},
            prefetch_buffer=3,
            claim_lease_ms=60_000,
        )
        assert 'soft' in config.worker_profiles

    def test_capacity_with_prefetch_buffer_is_still_a_bound(self) -> None:
        with pytest.raises(ConfigurationError) as raised:
            _app_config(
                {'soft': _isolated(2, claimed_per_worker=TaskLimit(tasks=6))},
                prefetch_buffer=3,
                claim_lease_ms=60_000,
            )
        assert 'capacity of the worker (5)' in raised.value.message

    def test_every_profile_is_reported_in_name_order(self) -> None:
        """`horsies check` reports all profiles at once, each with its path."""
        with pytest.raises(MultipleValidationErrors) as raised:
            _app_config(
                {
                    'zeta': _isolated(1, per_queue_per_pass=TaskLimit(tasks=9)),
                    'alpha': WorkerProfile(),
                    'ok': _isolated(4),
                }
            )
        messages = [error.message for error in _errors_of(raised.value)]
        assert messages == [
            "worker_profiles['alpha']: the profile starts no children",
            "worker_profiles['zeta'].claim.per_queue_per_pass: 9 tasks is above "
            'the capacity of the worker (1)',
        ]

    def test_claim_limits_are_not_compared_when_prefetch_buffer_is_invalid(
        self,
    ) -> None:
        """The prefetch error is reported; no capacity is computed from it."""
        with pytest.raises(ConfigurationError) as raised:
            _app_config(
                {'p': _isolated(1, claimed_per_worker=TaskLimit(tasks=1))},
                prefetch_buffer=-1,
            )
        assert raised.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_log_config_lists_each_profile(self) -> None:
        config = _app_config({'default': _isolated(2)})
        formatted = config._format_for_logging()  # pyright: ignore[reportPrivateUsage]
        assert '  worker_profiles:' in formatted
        assert f'    default: {describe_worker_profile(_isolated(2))}' in formatted


class TestDescribeWorkerProfile:
    def test_every_setting_is_named(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(
                count=8,
                recycle=IsolatedRecycle(
                    after=NoTaskLimit(),
                    above=MemoryLimit(mb=1024),
                ),
            ),
            claim=ClaimLimits(per_queue_per_pass=TaskLimit(tasks=4)),
            drain_timeout=timedelta(seconds=90),
            preload=ChildPreload.APP,
        )
        assert describe_worker_profile(profile) == (
            'isolated=8 (recycle after no limit, above 1024 MB), '
            'claim per queue per pass: 4 tasks, claimed per worker: no limit, '
            'drain_timeout=90s, preload=app'
        )

    def test_no_isolated_children(self) -> None:
        assert describe_worker_profile(WorkerProfile()).startswith('isolated=none, ')
